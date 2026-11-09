use chrono::Utc;
use engine_core::plan::hook::{HookSpec, HookTarget};
use engine_infra::hooks::{Envelope, Notification};
use model::{
    hook::event::HookEvent,
    integrity::{
        drift::{DriftRun, Skipped},
        pair::pair_pipeline,
        report::{DivergenceReport, DivergenceReportKind, ReportResult, TableReport},
    },
};
use serde::Serialize;
use std::fmt::Write as _;

/// Slack truncates long messages.
const SLACK_MAX_LINES: usize = 30;
/// Keys quoted per mismatched table in Slack text.
const SLACK_KEYS: usize = 3;
/// Matching tables named before "and N more".
const SLACK_MATCHES: usize = 5;

/// Drift's `report` in the [`Envelope`].
#[derive(Serialize)]
struct DriftHookReport<'a> {
    tables: &'a [&'a TableReport],
    /// Only for a hook without `target`: it watches the whole run.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    skipped: &'a [Skipped],
}

/// One notification per hook whose target was checked and whose events fired.
pub fn drift_notifications(run: &DriftRun, hooks: &[HookSpec]) -> Vec<Notification> {
    hooks
        .iter()
        .filter_map(|hook| {
            let tables = watched(run, hook.target.as_ref());
            let events = fired(&hook.on, &tables);

            (!tables.is_empty() && !events.is_empty())
                .then(|| render(run, hook, &events, &tables, false))
        })
        .collect()
}

/// What `--notify-test` sends through `hook`: every event it listens to,
/// over a sample run with one matching table and one mismatch.
pub fn test_notification(hook: &HookSpec) -> Notification {
    let run = sample_run(hook.target.as_ref());
    let tables: Vec<&TableReport> = run.tables.iter().collect();
    render(&run, hook, &hook.on, &tables, true)
}

fn watched<'a>(run: &'a DriftRun, target: Option<&HookTarget>) -> Vec<&'a TableReport> {
    run.tables
        .iter()
        .filter(|t| match target {
            None => true,
            Some(HookTarget::Pipeline(name)) => t.pipeline == *name,
            Some(HookTarget::Pair(name)) => t.pipeline == pair_pipeline(name),
        })
        .collect()
}

fn fired(on: &[HookEvent], tables: &[&TableReport]) -> Vec<HookEvent> {
    let any_mismatch = tables.iter().any(|t| t.result == ReportResult::Mismatch);
    let any_failed = tables.iter().any(|t| failed(t.result));

    on.iter()
        .copied()
        .filter(|event| match event {
            HookEvent::DriftCompleted => true,
            HookEvent::DriftDiverged => any_mismatch,
            HookEvent::DriftFailed => any_failed,
        })
        .collect()
}

fn failed(result: ReportResult) -> bool {
    matches!(result, ReportResult::Error | ReportResult::LogUnavailable)
}

fn render(
    run: &DriftRun,
    hook: &HookSpec,
    events: &[HookEvent],
    tables: &[&TableReport],
    test: bool,
) -> Notification {
    let skipped: &[Skipped] = match hook.target {
        None => &run.skipped,
        Some(_) => &[],
    };

    let envelope = Envelope {
        hook: &hook.name,
        events,
        command: "drift",
        run_id: &run.run_id,
        started_at: run.started_at,
        finished_at: run.finished_at,
        target: hook.target.as_ref().map(ToString::to_string),
        test,
        report: DriftHookReport { tables, skipped },
    };

    Notification {
        hook: hook.name.clone(),
        events: events.to_vec(),
        json: serde_json::to_vec(&envelope).unwrap_or_default(),
        text: slack_text(run, &hook.name, tables, test),
    }
}

fn slack_text(run: &DriftRun, hook: &str, tables: &[&TableReport], test: bool) -> String {
    let mut differ = 0;
    let mut unchecked = 0;
    let mut problems = Vec::new();
    let mut matched = Vec::new();

    for &t in tables {
        match t.result {
            ReportResult::Mismatch => {
                differ += 1;
                problems.push(t);
            }
            r if failed(r) => {
                unchecked += 1;
                problems.push(t);
            }
            ReportResult::Match => matched.push(t),
            _ => {}
        }
    }

    let mut out = String::with_capacity(512);

    // Header
    let test_prefix = if test { "[test] " } else { "" };
    let time = run.started_at.format("%Y-%m-%d %H:%M");
    let _ = write!(
        out,
        "{test_prefix}Paganel drift, {time} UTC ({hook}): {differ} of {} tables differ",
        tables.len()
    );

    if unchecked > 0 {
        let _ = write!(out, ", {unchecked} could not be checked");
    }

    // Problems
    let room = SLACK_MAX_LINES - 2;
    let show_problems = std::cmp::min(problems.len(), room - 1);

    for t in problems.iter().take(show_problems) {
        out.push('\n');
        match t.result {
            ReportResult::Mismatch => {
                let _ = write!(
                    out,
                    ":red_circle: {} {}",
                    t.display_name(),
                    mismatch_summary(t)
                );
            }
            _ => {
                let msg = t.message.as_deref().unwrap_or("not compared");
                let _ = write!(out, ":warning: {} {msg}", t.display_name());
            }
        }
    }

    if problems.len() > room {
        let more = problems.len() - show_problems;
        let _ = write!(out, "\n... and {more} more");
    }

    // Matches
    if !matched.is_empty() {
        let _ = write!(out, "\n:large_green_circle: {} match", short_list(&matched));
    }

    out
}

fn mismatch_summary(t: &TableReport) -> String {
    let mut out = String::new();
    let mut has_previous = false;

    let mut add = |count: u64, label: &str| {
        if count > 0 {
            if has_previous {
                out.push_str(", ");
            }
            let _ = write!(out, "{count} {label}");
            has_previous = true;
        }
    };

    add(t.changed, "changed");
    add(t.missing, "missing");
    add(t.extra, "extra");

    if t.new_since_last {
        out.push_str(". New since last run");
    }

    if !t.divergences.is_empty() {
        let keys: Vec<&str> = t
            .divergences
            .iter()
            .take(SLACK_KEYS)
            .map(|d| d.key.as_str())
            .collect();

        let _ = write!(out, ". e.g. {}", keys.join(", "));
    }

    out
}

fn short_list(tables: &[&TableReport]) -> String {
    let names: Vec<String> = tables
        .iter()
        .take(SLACK_MATCHES)
        .map(|t| t.display_name())
        .collect();

    match tables.len().checked_sub(SLACK_MATCHES) {
        None | Some(0) => names.join(", "),
        Some(more) => format!("{} and {more} more", names.join(", ")),
    }
}

fn sample_run(target: Option<&HookTarget>) -> DriftRun {
    let pipeline = match target {
        Some(HookTarget::Pipeline(name)) => name.clone(),
        Some(HookTarget::Pair(name)) => pair_pipeline(name),
        None => "sample".to_string(),
    };

    let base = TableReport::error(&pipeline, "customers", "");
    let changed = |key: &str| DivergenceReport {
        key: key.to_string(),
        kind: DivergenceReportKind::Changed,
        expected_hash: None,
        actual_hash: None,
    };

    let matched = TableReport {
        result: ReportResult::Match,
        expected_rows: 1_000,
        actual_rows: Some(1_000),
        message: None,
        ..base.clone()
    };

    let mismatch = TableReport {
        table: "orders".to_string(),
        result: ReportResult::Mismatch,
        expected_rows: 1_000,
        actual_rows: Some(1_000),
        changed: 2,
        divergences: vec![changed("id=4411"), changed("id=9002")],
        new_since_last: true,
        message: None,
        ..base
    };

    let now = Utc::now();
    DriftRun {
        run_id: DriftRun::run_id_at(now),
        config: String::new(),
        started_at: now,
        finished_at: now,
        tables: vec![mismatch, matched],
        skipped: Vec::new(),
        notifications: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{
        hook::delivery::{Delivery, Setting},
        integrity::import::import_pipeline,
    };

    fn hook(on: &[HookEvent], target: Option<HookTarget>) -> HookSpec {
        HookSpec {
            name: "h".into(),
            on: on.to_vec(),
            target,
            delivery: Delivery::Slack {
                url: Setting::Value("https://hooks.slack.com/x".into()),
            },
        }
    }

    fn table(pipeline: &str, name: &str, result: ReportResult) -> TableReport {
        TableReport {
            result,
            message: None,
            ..TableReport::error(pipeline, name, "")
        }
    }

    /// A pipeline that matches, a pair that differs, an import that failed.
    fn run() -> DriftRun {
        let mut run = sample_run(None);
        run.tables = vec![
            table("orders", "orders", ReportResult::Match),
            TableReport {
                changed: 1,
                ..table(
                    &pair_pipeline("orders_copy"),
                    "orders",
                    ReportResult::Mismatch,
                )
            },
            TableReport {
                message: Some("connection refused".into()),
                ..table(&import_pipeline("legacy"), "events", ReportResult::Error)
            },
        ];
        run.skipped = vec![Skipped {
            name: "customers".into(),
            reason: "no receipt".into(),
        }];
        run
    }

    fn envelope(n: &Notification) -> serde_json::Value {
        serde_json::from_slice(&n.json).unwrap()
    }

    #[test]
    fn a_target_watches_only_its_own_table() {
        let pair = hook(
            &[HookEvent::DriftCompleted],
            Some(HookTarget::Pair("orders_copy".into())),
        );
        let [n] = drift_notifications(&run(), &[pair]).try_into().unwrap();

        let body = envelope(&n);
        assert_eq!(body["target"], "pair.orders_copy");
        assert_eq!(body["report"]["tables"].as_array().unwrap().len(), 1);
        assert_eq!(body["report"]["tables"][0]["pipeline"], "@pair/orders_copy");
        assert!(body["report"].get("skipped").is_none());
    }

    #[test]
    fn no_target_watches_the_whole_run() {
        let all = hook(&[HookEvent::DriftCompleted], None);
        let [n] = drift_notifications(&run(), &[all]).try_into().unwrap();

        let body = envelope(&n);
        assert_eq!(body["target"], serde_json::Value::Null);
        assert_eq!(body["report"]["tables"].as_array().unwrap().len(), 3);
        assert_eq!(body["report"]["skipped"][0]["name"], "customers");
    }

    #[test]
    fn only_the_events_that_happened_fire() {
        let on = [HookEvent::DriftDiverged, HookEvent::DriftFailed];
        let cases = [
            (Some(HookTarget::Pipeline("orders".into())), vec![]),
            (
                Some(HookTarget::Pair("orders_copy".into())),
                vec![HookEvent::DriftDiverged],
            ),
            (None, vec![HookEvent::DriftDiverged, HookEvent::DriftFailed]),
        ];

        for (target, expected) in cases {
            let sent = drift_notifications(&run(), &[hook(&on, target.clone())]);
            let events: Vec<HookEvent> = sent.iter().flat_map(|n| n.events.clone()).collect();
            assert_eq!(events, expected, "{target:?}");
            assert!(sent.len() <= 1, "one POST per hook per run");
        }
    }

    #[test]
    fn a_target_skipped_this_run_sends_nothing() {
        let gone = hook(
            &[HookEvent::DriftCompleted],
            Some(HookTarget::Pipeline("customers".into())),
        );
        assert!(drift_notifications(&run(), &[gone]).is_empty());
    }

    #[test]
    fn slack_lists_problems_first() {
        let all = hook(&[HookEvent::DriftCompleted], None);
        let [n] = drift_notifications(&run(), &[all]).try_into().unwrap();
        let lines: Vec<&str> = n.text.lines().collect();

        assert!(
            lines[0].ends_with("(h): 1 of 3 tables differ, 1 could not be checked"),
            "{}",
            lines[0]
        );
        assert_eq!(
            &lines[1..],
            [
                ":red_circle: pair orders_copy (orders) 1 changed",
                ":warning: import legacy (events) connection refused",
                ":large_green_circle: orders/orders match",
            ]
        );
    }

    #[test]
    fn slack_stays_under_its_line_cap() {
        let mut big = run();
        big.tables = (0..40)
            .map(|i| TableReport {
                missing: 1,
                ..table(&format!("p{i}"), "t", ReportResult::Mismatch)
            })
            .chain((0..8).map(|i| table(&format!("m{i}"), "t", ReportResult::Match)))
            .collect();

        let all = hook(&[HookEvent::DriftDiverged], None);
        let [n] = drift_notifications(&big, &[all]).try_into().unwrap();
        let lines: Vec<&str> = n.text.lines().collect();

        assert_eq!(lines.len(), SLACK_MAX_LINES);
        assert_eq!(lines[SLACK_MAX_LINES - 2], "... and 13 more");
        assert!(lines[SLACK_MAX_LINES - 1].ends_with("m4/t and 3 more match"));
    }

    #[test]
    fn the_test_notification_is_marked_and_carries_every_event() {
        let on = [HookEvent::DriftDiverged, HookEvent::DriftFailed];
        let n = test_notification(&hook(&on, Some(HookTarget::Pair("orders_copy".into()))));

        let body = envelope(&n);
        assert_eq!(body["test"], true);
        assert_eq!(
            body["events"],
            serde_json::json!(["drift.diverged", "drift.failed"])
        );
        assert_eq!(body["report"]["tables"][0]["pipeline"], "@pair/orders_copy");
        assert!(n.text.starts_with("[test] Paganel drift"));
        assert!(n.text.contains("e.g. id=4411, id=9002"));
    }
}
