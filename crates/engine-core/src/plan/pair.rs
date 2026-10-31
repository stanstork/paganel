use crate::plan::builder::{PlanBuilder, parse_duration};
use connectors::sql::filter::{SqlFilter, compile::compile_filter};
use model::{
    core::value::Value,
    execution::{
        connection::Connection,
        errors::ConvertError,
        expr::{BinaryOp, CompiledExpression},
    },
    integrity::import::validate_import_name,
};
use ppl_syntax::ast::{
    attribute::Attribute,
    block::{ColumnMatch, PairBlock, PairSideBlock},
    dotpath::DotPath,
    expr::ExpressionKind,
    pipeline::WhereClause,
};
use std::{collections::BTreeMap, time::Duration};

/// How long a pair waits before re-reading the keys that differ.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// A `pair` block, resolved.
#[derive(Debug, Clone)]
pub struct PairSpec {
    pub name: String,
    pub source: PairSide,
    pub target: PairSide,
    /// Source column name -> target column name.
    pub columns: BTreeMap<String, String>,
    /// Key columns by source name.
    pub key: Option<Vec<String>>,
    /// Zero turns the re-check off.
    pub grace: Duration,
}

/// One table of a pair.
#[derive(Debug, Clone)]
pub struct PairSide {
    pub connection: Connection,
    pub table: String,
    pub filter: Option<SqlFilter>,
    /// Columns this side leaves out of the hash.
    pub ignore: Vec<String>,
}

impl PairSpec {
    pub fn target_column(&self, source: &str) -> String {
        target_column(&self.columns, source)
    }
}

/// The target's name for a source column.
pub fn target_column(columns: &BTreeMap<String, String>, source: &str) -> String {
    columns
        .iter()
        .find(|(s, _)| s.eq_ignore_ascii_case(source))
        .map_or_else(|| source.to_string(), |(_, t)| t.clone())
}

/// The source's name for a target column.
pub fn source_column(columns: &BTreeMap<String, String>, target: &str) -> String {
    columns
        .iter()
        .find(|(_, t)| t.eq_ignore_ascii_case(target))
        .map_or_else(|| target.to_string(), |(s, _)| s.clone())
}

impl PlanBuilder {
    pub fn build_pair(&self, block: &PairBlock) -> Result<PairSpec, ConvertError> {
        let name = &block.name;
        let err = |msg: String| ConvertError::Plan(format!("pair \"{name}\": {msg}"));

        validate_import_name(name)
            .map_err(|_| err("use 1-64 letters, digits, '_' or '-' in the name".into()))?;

        let (source, target) = self.parse_sides(&block.sides).map_err(err)?;
        let columns = pair_columns(&block.map, &source.ignore, &target.ignore).map_err(err)?;

        let mut key = None;
        let mut grace = DEFAULT_GRACE;

        for attr in &block.attributes {
            match attr.key.name.as_str() {
                "key" => key = Some(self.pair_key(attr, &columns).map_err(err)?),
                "grace" => {
                    let Value::String(s) = self.eval_with_definitions(&attr.value)? else {
                        return Err(err("`grace` must be a duration such as \"30s\"".into()));
                    };
                    grace = Duration::from_secs(parse_duration(&s)?);
                }
                other => return Err(err(format!("unknown attribute `{other}`"))),
            }
        }

        Ok(PairSpec {
            name: name.clone(),
            source,
            target,
            columns,
            key,
            grace,
        })
    }

    fn parse_sides(&self, side_blocks: &[PairSideBlock]) -> Result<(PairSide, PairSide), String> {
        let mut sides: [Option<PairSide>; 2] = [None, None];

        for side in side_blocks {
            let slot = match side.kind.as_str() {
                "source" => 0,
                "target" => 1,
                other => return Err(format!("unknown block `{other}`")),
            };
            if sides[slot].is_some() {
                return Err(format!("`{}` appears twice", side.kind));
            }
            sides[slot] = Some(self.pair_side(side)?);
        }

        let [Some(source), Some(target)] = sides else {
            return Err("needs both a `source` and a `target` block".into());
        };

        Ok((source, target))
    }

    fn pair_side(&self, block: &PairSideBlock) -> Result<PairSide, String> {
        let side = &block.kind;
        let mut connection = None;
        let mut table = None;
        let mut ignore = Vec::new();

        for attr in &block.attributes {
            match attr.key.name.as_str() {
                "connection" => {
                    connection = Some(self.parse_connection(attr, side)?);
                }
                "table" => {
                    let Value::String(s) = self
                        .eval_with_definitions(&attr.value)
                        .map_err(|_| format!("`{side}.table` must be a string"))?
                    else {
                        return Err(format!("`{side}.table` must be a string"));
                    };
                    table = Some(s);
                }
                "ignore" => ignore = self.string_list(attr)?,
                other => return Err(format!("unknown attribute `{side}.{other}`")),
            }
        }

        let connection =
            connection.ok_or_else(|| format!("`{side}` needs a connection and a table"))?;
        let table = table.ok_or_else(|| format!("`{side}` needs a connection and a table"))?;
        let filter = self.side_filter(&block.where_clauses, side, &table)?;

        Ok(PairSide {
            connection,
            table,
            filter,
            ignore,
        })
    }

    fn parse_connection(&self, attr: &Attribute, side: &str) -> Result<Connection, String> {
        let ExpressionKind::DotNotation(path) = &attr.value.kind else {
            return Err(format!("`{side}.connection` must be connection.<name>"));
        };
        let [kind, name] = path.segments.as_slice() else {
            return Err(format!("`{side}.connection` must be connection.<name>"));
        };
        if kind != "connection" {
            return Err(format!("`{side}.connection` must be connection.<name>"));
        }
        self.connections
            .get(name)
            .cloned()
            .ok_or_else(|| format!("connection `{name}` not found"))
    }

    fn pair_key(
        &self,
        attr: &Attribute,
        columns: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let names = self.string_list(attr)?;
        if names.is_empty() {
            return Err("`key` is empty".into());
        }

        names
            .iter()
            .map(|k| match k.split_once('.') {
                Some(("source", column)) => Ok(column.to_string()),
                Some(("target", column)) => Ok(source_column(columns, column)),
                Some(_) => Err(format!(
                    "key `{k}`: write `column`, `source.column` or `target.column`"
                )),
                None => {
                    let renamed = columns
                        .iter()
                        .any(|(s, t)| s.eq_ignore_ascii_case(k) || t.eq_ignore_ascii_case(k));
                    if renamed {
                        Err(format!(
                            "key `{k}` is renamed by `map`; write `source.{k}` or `target.{k}`"
                        ))
                    } else {
                        Ok(k.clone())
                    }
                }
            })
            .collect()
    }

    fn side_filter(
        &self,
        clauses: &[WhereClause],
        side: &str,
        table: &str,
    ) -> Result<Option<SqlFilter>, String> {
        let combined = clauses
            .iter()
            .flat_map(|clause| &clause.conditions)
            .try_fold(None, |acc, expr| {
                let condition = self.compile_expression(expr).map_err(|e| e.to_string())?;
                Ok::<_, String>(Some(match acc {
                    None => condition,
                    Some(left) => CompiledExpression::Binary {
                        left: Box::new(left),
                        op: BinaryOp::And,
                        right: Box::new(condition),
                    },
                }))
            })?;

        let Some(expr) = combined else {
            return Ok(None);
        };

        let filter = compile_filter(&expr).map_err(|e| format!("`{side}` where: {e}"))?;

        if let Some(other) = filter
            .tables()
            .into_iter()
            .find(|t| !t.is_empty() && !t.eq_ignore_ascii_case(table))
        {
            return Err(format!(
                "`{side}` where: `{other}` is not `{table}`; write `column` or `{table}.column`"
            ));
        }

        Ok(Some(filter.on_table(table)))
    }

    fn string_list(&self, attr: &Attribute) -> Result<Vec<String>, String> {
        let name = &attr.key.name;
        let ExpressionKind::Array(items) = &attr.value.kind else {
            return Err(format!("`{name}` must be a list of column names"));
        };

        items
            .iter()
            .map(|item| match self.eval_with_definitions(item) {
                Ok(Value::String(s)) => Ok(s),
                _ => Err(format!("`{name}` must contain strings")),
            })
            .collect()
    }
}

/// `map { source.a = target.b }` lines, as source name -> target name.
fn pair_columns(
    map: &[ColumnMatch],
    source_ignore: &[String],
    target_ignore: &[String],
) -> Result<BTreeMap<String, String>, String> {
    let ignored = |list: &[String], c: &str| list.iter().any(|i| i.eq_ignore_ascii_case(c));
    let mut columns: BTreeMap<String, String> = BTreeMap::new();

    for line in map {
        let (source, target) = match (side_of(&line.left), side_of(&line.right)) {
            (Some(("source", s)), Some(("target", t)))
            | (Some(("target", t)), Some(("source", s))) => (s, t),
            _ => {
                return Err(format!(
                    "map `{} = {}` must pair a source.column with a target.column",
                    line.left, line.right
                ));
            }
        };

        if ignored(source_ignore, source) {
            return Err(format!("`source.{source}` is both mapped and ignored"));
        }
        if ignored(target_ignore, target) {
            return Err(format!("`target.{target}` is both mapped and ignored"));
        }
        if columns.keys().any(|s| s.eq_ignore_ascii_case(source)) {
            return Err(format!("`source.{source}` is mapped twice"));
        }
        if columns.values().any(|t| t.eq_ignore_ascii_case(target)) {
            return Err(format!("`target.{target}` is mapped twice"));
        }

        columns.insert(source.to_string(), target.to_string());
    }

    Ok(columns)
}

fn side_of(path: &DotPath) -> Option<(&str, &str)> {
    match path.segments.as_slice() {
        [side, column] => Some((side.as_str(), column.as_str())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{context::env::EnvContext, plan::execution::ExecutionPlan};
    use ppl_syntax::builder::parse;
    use std::{sync::Arc, time::Duration};

    const CONNECTIONS: &str = r#"
        connection "app" { driver = "postgres" url = "postgres://localhost/app" }
        connection "dw"  { driver = "mysql"    url = "mysql://localhost/dw" }
    "#;

    fn build(pair: &str) -> Result<ExecutionPlan, String> {
        let doc = parse(&format!("{CONNECTIONS}\n{pair}")).map_err(|e| e.message)?;
        ExecutionPlan::build(&doc, Arc::new(EnvContext::empty())).map_err(|e| e.to_string())
    }

    /// A pair over `source { ... }` / `target { ... }` with `extra` at the top.
    fn pair_with(extra: &str) -> String {
        format!(
            r#"pair "a" {{
                source {{ connection = connection.app table = "t" }}
                target {{ connection = connection.dw  table = "t" }}
                {extra}
            }}"#
        )
    }

    #[test]
    fn a_full_pair_resolves() {
        let plan = build(
            r#"
            pair "orders" {
                source {
                    connection = connection.app
                    table      = "orders"
                    where      { status == "paid" }
                    where      { orders.customer_id > 0 }
                }
                target {
                    connection = connection.dw
                    table      = "orders_raw"
                    where      { state == "PAID" }
                    ignore     = ["_fivetran_synced"]
                }
                map {
                    source.customer_id = target.cust_id
                    target.order_ref   = source.order_no
                }
                key   = ["target.order_ref", "region"]
                grace = "0s"
            }
            "#,
        )
        .unwrap();

        let spec = &plan.pairs[0];
        assert_eq!(spec.source.connection.name, "app");
        assert_eq!(spec.target.table, "orders_raw");
        assert_eq!(spec.target.ignore, ["_fivetran_synced"]);
        assert_eq!(spec.target_column("customer_id"), "cust_id");
        assert_eq!(spec.target_column("order_no"), "order_ref");
        assert_eq!(spec.target_column("total"), "total");
        assert_eq!(
            spec.key.as_deref(),
            Some(&["order_no".to_string(), "region".to_string()][..])
        );
        assert_eq!(spec.grace, Duration::ZERO);

        let source = spec.source.filter.as_ref().unwrap();
        assert_eq!(source.columns(), ["orders.customer_id", "orders.status"]);
        let target = spec.target.filter.as_ref().unwrap();
        assert_eq!(target.columns(), ["orders_raw.state"]);
    }

    #[test]
    fn pairs_stay_out_of_the_run_id() {
        let pipeline = r#"
            pipeline "copy" {
                from { connection = connection.app table = "orders" }
                to   { connection = connection.dw  table = "orders" }
            }
        "#;
        let with = build(&format!("{pipeline}\n{}", pair_with(""))).unwrap();
        let without = build(pipeline).unwrap();

        assert_eq!(with.pairs.len(), 1);
        assert_eq!(with.run_id(), without.run_id());
    }

    #[test]
    fn mistakes_are_named() {
        let cases = [
            (
                r#"pair "a" { source { connection = connection.app table = "t" } }"#.to_string(),
                "needs both a `source` and a `target`",
            ),
            (
                r#"pair "a" {
                    source { connection = connection.app table = "t" }
                    target { connection = connection.nope table = "t" }
                }"#
                .to_string(),
                "connection `nope` not found",
            ),
            (
                r#"pair "a" {
                    source { connection = connection.app table = "t"  color = "red" }
                    target { connection = connection.dw table = "t" }
                }"#
                .to_string(),
                "unknown attribute `source.color`",
            ),
            (pair_with(r#"color = "red""#), "unknown attribute `color`"),
            (
                pair_with("map { source.x = source.y }"),
                "map `source.x = source.y` must pair a source.column with a target.column",
            ),
            (
                pair_with("map { source.x = target.y  source.z = target.y }"),
                "`target.y` is mapped twice",
            ),
            (
                pair_with("map { source.x = target.y  source.x = target.z }"),
                "`source.x` is mapped twice",
            ),
            (
                r#"pair "a" {
                    source { connection = connection.app table = "t"  ignore = ["x"] }
                    target { connection = connection.dw table = "t" }
                    map { source.x = target.y }
                }"#
                .to_string(),
                "`source.x` is both mapped and ignored",
            ),
            (
                r#"pair "a" {
                    source { connection = connection.app table = "t" }
                    target { connection = connection.dw table = "t"  where { other.x == 1 } }
                }"#
                .to_string(),
                "`target` where: `other` is not `t`",
            ),
            (
                pair_with(r#"map { source.x = target.y }  key = ["x"]"#),
                "key `x` is renamed by `map`; write `source.x` or `target.x`",
            ),
            (
                pair_with(r#"key = ["left.x"]"#),
                "key `left.x`: write `column`, `source.column` or `target.column`",
            ),
            (
                r#"pair "a b" {
                    source { connection = connection.app table = "t" }
                    target { connection = connection.dw table = "t" }
                }"#
                .to_string(),
                "letters, digits",
            ),
            (
                format!("{}\n{}", pair_with(""), pair_with("")),
                "more than one pair named `a`",
            ),
            (
                format!(
                    r#"pipeline "a" {{
                        from {{ connection = connection.app table = "t" }}
                        to   {{ connection = connection.dw  table = "t" }}
                    }}
                    {}"#,
                    pair_with("")
                ),
                "a pipeline is already named `a`",
            ),
        ];

        for (config, expected) in cases {
            let err = build(&config).unwrap_err();
            assert!(err.contains(expected), "{expected:?} not in {err:?}");
        }
    }
}
