use crate::plan::{builder::PlanBuilder, pair::PairSpec};
use model::{
    core::value::Value,
    execution::{errors::ConvertError, pipeline::Pipeline},
    hook::{
        delivery::{Delivery, Setting},
        event::HookEvent,
    },
    integrity::import::validate_import_name,
};
use ppl_syntax::ast::{
    attribute::Attribute, block::HookBlock, expr::ExpressionKind, pipeline::NestedBlock,
};
use std::fmt;

/// A `hook` block, resolved.
#[derive(Debug, Clone)]
pub struct HookSpec {
    pub name: String,
    pub on: Vec<HookEvent>,
    pub target: Option<HookTarget>,
    pub delivery: Delivery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookTarget {
    Pipeline(String),
    Pair(String),
}

impl HookTarget {
    fn can_emit(&self, event: HookEvent) -> bool {
        matches!(
            event,
            HookEvent::DriftCompleted | HookEvent::DriftDiverged | HookEvent::DriftFailed
        )
    }
}

impl fmt::Display for HookTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HookTarget::Pipeline(name) => write!(f, "pipeline.{name}"),
            HookTarget::Pair(name) => write!(f, "pair.{name}"),
        }
    }
}

impl PlanBuilder {
    pub fn build_hook(
        &self,
        block: &HookBlock,
        pipelines: &[Pipeline],
        pairs: &[PairSpec],
    ) -> Result<HookSpec, ConvertError> {
        let name = &block.name;
        let err = |msg: String| ConvertError::Plan(format!("hook \"{name}\": {msg}"));

        validate_import_name(name)
            .map_err(|_| err("use 1-64 letters, digits, '_' or '-' in the name".into()))?;

        let mut on = None;
        let mut target = None;

        for attr in &block.attributes {
            match attr.key.name.as_str() {
                "on" => on = Some(self.hook_events(attr).map_err(err)?),
                "target" => target = Some(hook_target(attr, pipelines, pairs).map_err(err)?),
                other => return Err(err(format!("unknown attribute `{other}`"))),
            }
        }

        let on = on.ok_or_else(|| err("needs `on`, a list of events".into()))?;

        if let Some(target) = &target
            && let Some(event) = on.iter().find(|e| !target.can_emit(**e))
        {
            return Err(err(format!("`{event}` can't fire for {target}")));
        }

        let delivery = match block.nested_blocks.as_slice() {
            [one] => self.delivery(one).map_err(err)?,
            [] => return Err(err("needs a `slack` or `webhook` block".into())),
            _ => return Err(err("takes one `slack` or `webhook` block".into())),
        };

        Ok(HookSpec {
            name: name.clone(),
            on,
            target,
            delivery,
        })
    }

    fn hook_events(&self, attr: &Attribute) -> Result<Vec<HookEvent>, String> {
        let ExpressionKind::Array(items) = &attr.value.kind else {
            return Err("`on` must be a list of events, e.g. [\"drift.diverged\"]".into());
        };

        if items.is_empty() {
            return Err("`on` is empty".into());
        }

        let mut events: Vec<HookEvent> = Vec::new();
        for item in items {
            let Ok(Value::String(name)) = self.eval_with_definitions(item) else {
                return Err("`on` must contain event names as strings".into());
            };

            let event = HookEvent::parse(&name)?;
            if !events.contains(&event) {
                events.push(event);
            }
        }

        Ok(events)
    }

    fn delivery(&self, block: &NestedBlock) -> Result<Delivery, String> {
        let kind = block.kind.as_str();

        if kind != "slack" && kind != "webhook" {
            return Err(format!("unknown block `{kind}`; use `slack` or `webhook`"));
        }

        let mut url = None;
        let mut secret = None;

        for attr in &block.attributes {
            let key = attr.key.name.as_str();

            match (kind, key) {
                (_, "url") => url = Some(self.setting(attr)?),
                ("webhook", "secret") => secret = Some(self.setting(attr)?),
                _ => return Err(format!("unknown attribute `{kind}.{key}`")),
            }
        }

        let url = url.ok_or_else(|| format!("`{kind}` needs a `url`"))?;

        match kind {
            "slack" => Ok(Delivery::Slack { url }),
            "webhook" => {
                let secret = secret.ok_or("`webhook` needs a `secret`")?;
                Ok(Delivery::Webhook { url, secret })
            }
            _ => unreachable!(),
        }
    }

    fn setting(&self, attr: &Attribute) -> Result<Setting, String> {
        match self.eval_with_definitions(&attr.value) {
            Ok(Value::String(s)) => Ok(Setting::Value(s)),
            Ok(_) => Err(format!("`{}` must be a string", attr.key.name)),
            Err(e) => Ok(Setting::Unset(e.to_string())),
        }
    }
}

/// `pipeline.<name>` or `pair.<name>`, naming a block of this config.
fn hook_target(
    attr: &Attribute,
    pipelines: &[Pipeline],
    pairs: &[PairSpec],
) -> Result<HookTarget, String> {
    const TARGET_SHAPE_ERR: &str = "`target` must be pipeline.<name> or pair.<name>";

    let ExpressionKind::DotNotation(path) = &attr.value.kind else {
        return Err(TARGET_SHAPE_ERR.into());
    };

    let [kind, name] = path.segments.as_slice() else {
        return Err(TARGET_SHAPE_ERR.into());
    };

    match kind.as_str() {
        "pipeline" if pipelines.iter().any(|p| &p.name == name) => {
            Ok(HookTarget::Pipeline(name.clone()))
        }
        "pair" if pairs.iter().any(|p| &p.name == name) => Ok(HookTarget::Pair(name.clone())),
        "pipeline" | "pair" => Err(format!("unknown {kind} `{name}`")),
        _ => Err(TARGET_SHAPE_ERR.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::HookTarget;
    use crate::{context::env::EnvContext, plan::execution::ExecutionPlan};
    use model::hook::{
        delivery::{Delivery, Setting},
        event::HookEvent,
    };
    use ppl_syntax::builder::parse;
    use std::sync::Arc;

    const BLOCKS: &str = r#"
        connection "app" { driver = "postgres" url = "postgres://localhost/app" }
        connection "dw"  { driver = "mysql"    url = "mysql://localhost/dw" }
        pipeline "orders" {
            from { connection = connection.app table = "orders" }
            to   { connection = connection.dw  table = "orders" }
        }
        pair "orders_copy" {
            source { connection = connection.app table = "orders" }
            target { connection = connection.dw  table = "orders" }
        }
    "#;

    fn build_with(hooks: &str, env: EnvContext) -> Result<ExecutionPlan, String> {
        let doc = parse(&format!("{BLOCKS}\n{hooks}")).map_err(|e| e.message)?;
        ExecutionPlan::build(&doc, Arc::new(env)).map_err(|e| e.to_string())
    }

    fn build(hooks: &str) -> Result<ExecutionPlan, String> {
        build_with(hooks, EnvContext::empty())
    }

    #[test]
    fn both_delivery_kinds_resolve() {
        let mut env = EnvContext::empty();
        env.set("AUDIT_SECRET".into(), "s3cret".into());
        let plan = build_with(
            r#"
            hook "orders_alert" {
                on     = ["drift.diverged", "drift.failed", "drift.diverged"]
                target = pair.orders_copy
                slack { url = "https://hooks.slack.com/services/T/B/x" }
            }
            hook "audit" {
                on = ["drift.completed"]
                webhook {
                    url    = "https://audit.example.com/paganel"
                    secret = env("AUDIT_SECRET")
                }
            }
            "#,
            env,
        )
        .unwrap();

        let [alert, audit] = plan.hooks.as_slice() else {
            panic!("two hooks expected");
        };
        assert_eq!(alert.on, [HookEvent::DriftDiverged, HookEvent::DriftFailed]);
        assert_eq!(alert.target, Some(HookTarget::Pair("orders_copy".into())));
        assert_eq!(format!("{:?}", alert.delivery), "slack(hooks.slack.com)");

        assert_eq!(audit.target, None);
        let Delivery::Webhook { secret, .. } = &audit.delivery else {
            panic!("webhook expected");
        };
        assert_eq!(secret, &Setting::Value("s3cret".into()));
    }

    #[test]
    fn an_unset_env_only_leaves_the_setting_unset() {
        let plan = build(
            r#"hook "a" { on = ["drift.completed"] slack { url = env("NOT_SET_ANYWHERE") } }"#,
        )
        .unwrap();

        let err = plan.hooks[0].delivery.url().get().unwrap_err();
        assert!(err.contains("NOT_SET_ANYWHERE"), "{err}");
    }

    #[test]
    fn hooks_stay_out_of_the_run_id() {
        let with =
            build(r#"hook "a" { on = ["drift.completed"] slack { url = "https://x.io/h" } }"#)
                .unwrap();
        let without = build("").unwrap();

        assert_eq!(with.hooks.len(), 1);
        assert_eq!(with.run_id(), without.run_id());
    }

    #[test]
    fn mistakes_are_named() {
        let slack = r#"slack { url = "https://x.io/h" }"#;
        let cases = [
            (format!(r#"hook "a" {{ {slack} }}"#), "needs `on`"),
            (format!(r#"hook "a" {{ on = [] {slack} }}"#), "`on` is empty"),
            (
                format!(r#"hook "a" {{ on = "drift.completed" {slack} }}"#),
                "`on` must be a list of events",
            ),
            (
                format!(r#"hook "a" {{ on = ["drift.done"] {slack} }}"#),
                "unknown event `drift.done`; use one of drift.completed",
            ),
            (
                format!(r#"hook "a" {{ on = ["apply.failed"] {slack} }}"#),
                "event `apply.failed` is not supported yet",
            ),
            (
                format!(
                    r#"hook "a" {{ on = ["drift.completed"] target = pair.orders_cpy {slack} }}"#
                ),
                "unknown pair `orders_cpy`",
            ),
            (
                format!(
                    r#"hook "a" {{ on = ["drift.completed"] target = connection.app {slack} }}"#
                ),
                "`target` must be pipeline.<name> or pair.<name>",
            ),
            (
                format!(r#"hook "a" {{ on = ["drift.completed"] when = "always" {slack} }}"#),
                "unknown attribute `when`",
            ),
            (
                r#"hook "a" { on = ["drift.completed"] }"#.to_string(),
                "needs a `slack` or `webhook` block",
            ),
            (
                format!(r#"hook "a" {{ on = ["drift.completed"] {slack} {slack} }}"#),
                "takes one `slack` or `webhook` block",
            ),
            (
                r#"hook "a" { on = ["drift.completed"] email { to = "x@y.z" } }"#.to_string(),
                "unknown block `email`; use `slack` or `webhook`",
            ),
            (
                r#"hook "a" { on = ["drift.completed"] slack { url = "https://x.io" secret = "s" } }"#
                    .to_string(),
                "unknown attribute `slack.secret`",
            ),
            (
                r#"hook "a" { on = ["drift.completed"] webhook { url = "https://x.io" } }"#
                    .to_string(),
                "`webhook` needs a `secret`",
            ),
            (
                r#"hook "a" { on = ["drift.completed"] slack { url = 42 } }"#.to_string(),
                "`url` must be a string",
            ),
            (
                format!(r#"hook "a b" {{ on = ["drift.completed"] {slack} }}"#),
                "letters, digits",
            ),
            (
                format!(
                    r#"hook "a" {{ on = ["drift.completed"] {slack} }}
                       hook "a" {{ on = ["drift.failed"] {slack} }}"#
                ),
                "more than one hook named `a`",
            ),
        ];

        for (config, expected) in cases {
            let err = build(&config).unwrap_err();
            assert!(err.contains(expected), "{expected:?} not in {err:?}");
        }
    }
}
