use crate::{
    commands::{CliError, DriftArgs},
    config,
    format::{DATE_FORMAT, commas},
};
use engine_core::plan::{execution::ExecutionPlan, hook::HookSpec};
use engine_infra::hooks::deliver;
use engine_processing::EnvContext;
use engine_verify::{
    NoopProgress, VerifyOptions, VerifyState, discover_targets,
    error::VerifyError,
    hooks::{drift_notifications, test_notification},
    run_drift,
};
use futures_util::future::join_all;
use model::{
    hook::result::HookDeliveryResult,
    integrity::{
        drift::DriftRun,
        report::{DivergenceReportKind, Recheck, ReportResult, TableReport},
    },
};
use std::{sync::Arc, time::Duration};
use tracing::info;
use tracing::warn;

/// Check every target once, or every `--interval` with `--watch`.
pub async fn execute(args: &DriftArgs, env: Arc<EnvContext>) -> Result<(), CliError> {
    let config_path = std::fs::canonicalize(config::resolve_path(args.config.clone())?)?
        .to_string_lossy()
        .into_owned();
    info!(config = %config_path, "checking for drift");

    let plan = config::load_plan(&config_path, false, env.clone()).await?;

    if let Some(only) = &args.notify_test {
        return notify_test(&plan, only).await;
    }

    let options = VerifyOptions {
        allow_extra: args.allow_extra,
    };
    let state = VerifyState::open_default()?;

    loop {
        match drift_once(&plan, env.clone(), &state, &args.filters, options).await {
            Ok(mut run) => {
                run.notifications = notify(&plan.hooks, &run).await;

                if !args.json {
                    print_run(&run);
                } else if args.watch {
                    println!("{}", serde_json::to_string(&run)?);
                } else {
                    println!("{}", serde_json::to_string_pretty(&run)?);
                }

                if !args.watch {
                    return exit_status(run.exit_code());
                }
            }
            // In watch mode one failed run is logged; the next interval retries.
            Err(e) if args.watch => warn!(error = %e, "drift run failed"),
            Err(e) => return Err(e),
        }

        info!(interval = ?args.interval, "waiting for the next drift run");
        tokio::time::sleep(args.interval).await;
    }
}

/// `15m`, `6h`, `1d`: a whole number and one of s, m, h, d.
pub fn parse_interval(s: &str) -> Result<Duration, String> {
    let invalid = || format!("'{s}' is not an interval; use a number and s, m, h or d, e.g. 15m");

    let (at, unit) = s.char_indices().last().ok_or_else(invalid)?;
    let n: u64 = s[..at].parse().map_err(|_| invalid())?;
    let unit_secs = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => return Err(invalid()),
    };

    match n.checked_mul(unit_secs) {
        Some(secs) if secs > 0 => Ok(Duration::from_secs(secs)),
        _ => Err(invalid()),
    }
}

/// Run once, flag what changed since the last run, and save it.
async fn drift_once(
    plan: &ExecutionPlan,
    env: Arc<EnvContext>,
    state: &VerifyState,
    filters: &[String],
    options: VerifyOptions,
) -> Result<DriftRun, CliError> {
    let (targets, skipped) = discover_targets(plan, state, filters).await?;
    let last = state
        .store
        .last_drift_run(&plan.config_path)
        .await
        .map_err(VerifyError::from)?;

    let mut run = run_drift(plan, env, state, &targets, &mut NoopProgress, options).await?;
    run.skipped.extend(skipped);
    run.mark_new_since(last.as_ref());

    state
        .store
        .save_drift_run(&run)
        .await
        .map_err(VerifyError::from)?;

    Ok(run)
}

/// Post every hook `run` fires, all at once.
async fn notify(hooks: &[HookSpec], run: &DriftRun) -> Vec<HookDeliveryResult> {
    let sends = drift_notifications(run, hooks).into_iter().filter_map(|n| {
        let hook = hooks.iter().find(|h| h.name == n.hook)?;
        Some(async move { deliver(&hook.delivery, &n).await })
    });

    join_all(sends).await
}

/// `--notify-test [HOOK]`: a sample through every hook, or one, then exit.
async fn notify_test(plan: &ExecutionPlan, only: &str) -> Result<(), CliError> {
    let hooks: Vec<&HookSpec> = plan
        .hooks
        .iter()
        .filter(|h| only.is_empty() || h.name == only)
        .collect();
    if hooks.is_empty() {
        return Err(CliError::Config(match only {
            "" => "the config has no hooks".to_string(),
            name => format!("no hook named `{name}`"),
        }));
    }

    let results = join_all(
        hooks
            .iter()
            .map(|h| async move { deliver(&h.delivery, &test_notification(h)).await }),
    )
    .await;

    for result in &results {
        println!("{}", format_delivery(result));
    }

    match results.iter().filter(|r| !r.ok).count() {
        0 => Ok(()),
        failed => Err(CliError::UserMessage(format!(
            "{failed} of {} hooks could not be delivered",
            results.len()
        ))),
    }
}

fn format_delivery(r: &HookDeliveryResult) -> String {
    let tries = |n: u32| format!("{n} attempt{}", if n == 1 { "" } else { "s" });

    match (&r.error, r.status) {
        (None, status) => format!(
            "→ hook {} delivered ({}, {})",
            r.hook,
            status.unwrap_or_default(),
            tries(r.attempts)
        ),
        (Some(e), _) if r.attempts == 0 => format!("✗ hook {} not sent: {e}", r.hook),
        (Some(e), _) => format!("✗ hook {} failed: {e} after {}", r.hook, tries(r.attempts)),
    }
}

/// Map a run's exit code onto the errors `main` turns into 3 and 4.
fn exit_status(code: i32) -> Result<(), CliError> {
    match code {
        3 => Err(VerifyError::Mismatch.into()),
        4 => Err(VerifyError::Inconclusive.into()),
        _ => Ok(()),
    }
}

fn print_run(run: &DriftRun) {
    println!("{} ({})", run.run_id, run.started_at.format(DATE_FORMAT));

    for table in &run.tables {
        println!("{}", format_table(table));
    }
    for s in &run.skipped {
        println!("- {}: skipped, {}", s.name, s.reason);
    }
    for result in &run.notifications {
        println!("{}", format_delivery(result));
    }

    println!("{}", tally(run));
}

fn format_table(t: &TableReport) -> String {
    let name = t.display_name();
    let recheck = match &t.recheck {
        Some(Recheck::Done { settled }) if *settled > 0 => {
            format!(", {} settled after grace", commas(*settled))
        }
        Some(Recheck::Skipped { reason }) => format!(", not re-checked: {reason}"),
        _ => String::new(),
    };

    match t.result {
        ReportResult::Match => format!(
            "✓ {name} - match ({} rows, {}ms{recheck})",
            commas(t.expected_rows),
            commas(t.duration_ms)
        ),
        ReportResult::Mismatch => {
            let new = if t.new_since_last {
                ", new since the last run"
            } else {
                ""
            };
            let mut out = format!(
                "✗ {name} - MISMATCH{new} ({} missing, {} changed, {} extra{recheck})",
                commas(t.missing),
                commas(t.changed),
                commas(t.extra)
            );

            for d in &t.divergences {
                let kind = match d.kind {
                    DivergenceReportKind::Missing => "missing",
                    DivergenceReportKind::Changed => "changed",
                    DivergenceReportKind::Extra => "extra",
                };
                out.push_str(&format!("\n  {} - {kind}", d.key));
            }

            let total = t.missing + t.changed + t.extra;
            let shown = t.divergences.len() as u64;
            if total > shown {
                out.push_str(&format!("\n  ... and {} more", commas(total - shown)));
            }

            out
        }
        ReportResult::LogUnavailable | ReportResult::Error | ReportResult::NoPriorRun => {
            format!(
                "? {name} - {}",
                t.message.as_deref().unwrap_or("not compared")
            )
        }
    }
}

fn tally(run: &DriftRun) -> String {
    let count = |result: ReportResult| run.tables.iter().filter(|t| t.result == result).count();

    let mut parts = vec![format!("{} matched", count(ReportResult::Match))];
    for (result, label) in [
        (ReportResult::Mismatch, "mismatched"),
        (ReportResult::LogUnavailable, "inconclusive"),
        (ReportResult::Error, "failed"),
    ] {
        match count(result) {
            0 => {}
            n => parts.push(format!("{n} {label}")),
        }
    }
    if !run.skipped.is_empty() {
        parts.push(format!("{} skipped", run.skipped.len()));
    }

    parts.join(", ")
}
