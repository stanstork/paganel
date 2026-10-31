use crate::{
    harness::{
        Direction,
        runner::{DbType, get_row_count, run_ppl},
    },
    reset_postgres_schema,
};
use engine_core::plan::execution::ExecutionPlan;
use engine_processing::EnvContext;
use engine_verify::{
    NoopProgress, VerifyOptions, VerifyState, discover_targets, run_drift, verify,
    verify_with_progress,
};
use model::integrity::{drift::DriftRun, result::VerificationResult};
use ppl_syntax::builder::parse;
use std::sync::Arc;

/// One drift run over `filters` of `ppl`, without saving it.
pub(crate) async fn drift_run(ppl: &str, filters: &[&str]) -> DriftRun {
    let doc = parse(ppl).expect("parse");
    let env = Arc::new(EnvContext::empty());
    let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
    let state = VerifyState::open_default().expect("state");
    let filters: Vec<String> = filters.iter().map(|f| f.to_string()).collect();

    let (targets, _) = discover_targets(&plan, &state, &filters)
        .await
        .expect("targets");
    run_drift(
        &plan,
        env,
        &state,
        &targets,
        &mut NoopProgress,
        VerifyOptions::default(),
    )
    .await
    .expect("drift")
}

/// Load `actor` in two halves: the first without integrity (so no receipt
/// covers it), the second with. Returns the config for the second migration,
/// which is the one under verification.
pub(crate) async fn seed_then_append() -> String {
    reset_postgres_schema().await;

    let seed = Direction::MYSQL_TO_POSTGRES.ppl(
        r#"
        pipeline "seed_first_half" {
            from { connection = connection.src  table = "actor" }
            to   { connection = connection.dst  table = "actor" }
            where "first_half" { actor.actor_id <= 100 }
            settings { create_missing_tables = true  batch_size = 50 }
        }
    "#,
    );
    run_ppl(&seed, false).await.expect("seed apply failed");

    let append = Direction::MYSQL_TO_POSTGRES.ppl(
        r#"
        pipeline "append_second_half" {
            from { connection = connection.src  table = "actor" }
            to   { connection = connection.dst  table = "actor" }
            where "second_half" { actor.actor_id > 100 }
            settings { create_missing_tables = true  batch_size = 50 }
        }
    "#,
    );
    run_ppl(&append, true).await.expect("append apply failed");

    assert_eq!(
        get_row_count("actor", "sakila", DbType::Postgres).await,
        200,
        "both halves should be in the destination"
    );

    append
}

/// Run verify and hand back the raw results for assertions on detail.
pub(crate) async fn verify_results(ppl: &str) -> Vec<VerificationResult> {
    let doc = parse(ppl).expect("parse");
    let env = Arc::new(EnvContext::empty());
    let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");

    verify(plan, env).await.expect("verify call failed")
}

/// Run verify with explicit options and hand back the raw results.
pub(crate) async fn verify_results_with(
    ppl: &str,
    options: VerifyOptions,
) -> Vec<VerificationResult> {
    let doc = parse(ppl).expect("parse");
    let env = Arc::new(EnvContext::empty());
    let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");

    verify_with_progress(plan, env, &mut NoopProgress, options)
        .await
        .expect("verify call failed")
}
