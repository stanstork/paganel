use crate::{
    diff::{DiffInput, DiffOutcome, diff_keyed_sets},
    error::VerifyError,
    progress::{NoopProgress, VerifyProgress},
    state::VerifyState,
    table::{HashSink, HashSpec, fetch_table_metadata, hash_table},
};
use connectors::{sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector};
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    dispatch_driver,
    drivers::DriverRef,
    plan::{cascade::resolve_cascade_tables, execution::ExecutionPlan},
};
use engine_schema::{
    graph_expander::GraphExpander,
    plan::SchemaObjectFlags,
    type_registry::{Dialect, TypeRegistry},
};
use engine_state::{RowHashLog, RowHashScope, RunLock, StateStore};
use model::{
    execution::{
        pipeline::Pipeline,
        references::{DataMode, GraphReferences},
    },
    integrity::{receipt::VerificationReceipt, result::VerificationResult},
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tracing::warn;

/// How strictly the destination must match the receipt.
#[derive(Debug, Clone, Copy, Default)]
pub struct VerifyOptions {
    /// Accept destination rows the receipt does not cover.
    pub allow_extra: bool,
}

pub async fn verify(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
) -> Result<Vec<VerificationResult>, VerifyError> {
    verify_with_progress(plan, env, &mut NoopProgress, VerifyOptions::default()).await
}

pub async fn verify_with_progress(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<Vec<VerificationResult>, VerifyError> {
    let state = VerifyState::open_default()?;
    verify_plan(&plan, env, &state, progress, options).await
}

pub async fn verify_plan(
    plan: &ExecutionPlan,
    env: Arc<EnvContext>,
    state: &VerifyState,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<Vec<VerificationResult>, VerifyError> {
    let _run_lock = RunLock::acquire(state.store.dir(), &plan.run_id())?;

    let exec_ctx = ExecutionContext::new(plan, state.store.clone(), state.hash_log.clone(), env);
    let mut results: Vec<VerificationResult> = Vec::new();

    for pipeline in &plan.pipelines {
        verify_pipeline(
            pipeline,
            &exec_ctx,
            &state.store,
            &state.hash_log,
            progress,
            &mut results,
            options,
        )
        .await?;
    }

    Ok(results)
}

/// The tables a pipeline's receipts cover: its destination, then any cascade
/// tables, each once.
pub(crate) async fn pipeline_tables(
    pipeline: &Pipeline,
    driver: &DriverRef,
) -> Result<Vec<String>, VerifyError> {
    let mapping = TransformationMetadata::new(pipeline);
    let cascade_meta = get_graph_expansion(pipeline, driver, &mapping).await?;
    let cascade_tables = resolve_cascade_tables(pipeline, &mapping, &cascade_meta);

    let mut seen = HashSet::new();
    let tables = std::iter::once(pipeline.destination.table.as_str())
        .chain(cascade_tables.iter().map(String::as_str))
        .filter(|t| !t.is_empty() && seen.insert(*t))
        .map(String::from)
        .collect();

    Ok(tables)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_pipeline(
    pipeline: &Pipeline,
    exec_ctx: &ExecutionContext,
    state: &Arc<StateStore>,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
    results: &mut Vec<VerificationResult>,
    options: VerifyOptions,
) -> Result<(), VerifyError> {
    let driver = exec_ctx
        .resolve_driver(&pipeline.destination.connection)
        .await?;

    for table in pipeline_tables(pipeline, &driver).await? {
        // Load the receipt written by the most recent `apply --integrity`.
        let Some(receipt) = state.load_receipt(&pipeline.name, &table).await? else {
            let result = VerificationResult::NoPriorRun {
                pipeline: pipeline.name.clone(),
                table: table.clone(),
            };
            progress.table_finished(&result);
            results.push(result);
            continue;
        };

        progress.table_started(&pipeline.name, &table);
        let result = verify_table(
            &driver,
            &pipeline.name,
            &table,
            &receipt,
            hash_log,
            progress,
            options,
        )
        .await?;
        progress.table_finished(&result);
        results.push(result);
    }

    Ok(())
}

/// Re-read every row of `table` from the destination, key and hash each one,
/// then diff that keyed set against the set the migration committed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_table(
    driver: &DriverRef,
    pipeline_name: &str,
    table: &str,
    receipt: &VerificationReceipt,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<VerificationResult, VerifyError> {
    let start = Instant::now();

    let meta = fetch_table_metadata(driver, table).await?;
    let sink = HashSink {
        log: hash_log,
        scope: RowHashScope::Verify,
        pipeline: pipeline_name,
        table,
    };

    // Stage the destination's keyed hashes so both sides can be walked in the
    // same sorted key order without holding either set in memory.
    hash_log.clear(RowHashScope::Verify, pipeline_name, table)?;

    let outcome = stage_and_diff(driver, meta, receipt, &sink, progress, options).await;

    if let Err(e) = hash_log.clear(RowHashScope::Verify, pipeline_name, table) {
        warn!(table, error = %e, "failed to clear verify scratch row hashes");
    }

    let DiffOutcome {
        actual_root,
        summary,
        divergences,
        expected_seen,
    } = outcome?;
    let duration_ms = start.elapsed().as_millis() as u64;

    if expected_seen != receipt.total_rows {
        return Ok(VerificationResult::LogUnavailable {
            pipeline: pipeline_name.to_string(),
            table: table.to_string(),
            expected_rows: receipt.total_rows,
            found_rows: expected_seen,
        });
    }

    // With `allow_extra`, uncovered rows were neither folded into the root nor
    // recorded, so the root still has to reproduce for the receipt's own rows.
    let clean = if options.allow_extra {
        summary.missing == 0 && summary.changed == 0
    } else {
        summary.is_clean()
    };
    let is_match = clean && actual_root == receipt.table_root;

    Ok(if is_match {
        VerificationResult::Match {
            receipt: receipt.clone(),
            duration_ms,
            extra_ignored: summary.extra,
        }
    } else {
        VerificationResult::Mismatch {
            receipt: receipt.clone(),
            actual_root,
            summary,
            divergences,
            duration_ms,
        }
    })
}

/// Stage the destination's rows, then diff them against the receipt's set.
async fn stage_and_diff(
    driver: &DriverRef,
    meta: TableMetadata,
    receipt: &VerificationReceipt,
    sink: &HashSink<'_>,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<DiffOutcome, VerifyError> {
    progress.table_phase("reading destination");

    let spec = HashSpec::from_receipt(receipt);
    let dest_rows = hash_table(driver, meta, &spec, sink, Some(receipt.total_rows)).await?;

    // Both sides have to be walked in the same order; sealing is what puts the
    // staged destination into the receipt set's key order.
    progress.table_phase("sorting row hashes");
    let log = sink.log;
    log.seal(RowHashScope::Verify, sink.pipeline, sink.table)?;

    let expected = log.stream(RowHashScope::Baseline, sink.pipeline, sink.table)?;
    let actual = log.stream(RowHashScope::Verify, sink.pipeline, sink.table)?;
    let input = DiffInput::from_receipt(receipt);

    progress.table_phase("comparing");

    tokio::task::spawn_blocking(move || {
        diff_keyed_sets(expected, actual, &input, dest_rows, options)
    })
    .await
    .map_err(|e| VerifyError::InitializationError(format!("verification task failed: {e}")))?
}

async fn get_graph_expansion(
    pipeline: &Pipeline,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    let Some(refs) = &pipeline.source.graph_references else {
        return Ok(None);
    };

    let dest_driver = &pipeline.destination.connection.driver;
    let dest_dialect = Dialect::parse(dest_driver).ok_or_else(|| {
        VerifyError::InitializationError(format!(
            "graph verification requires a SQL destination dialect, but destination driver '{dest_driver}' is not a SQL dialect"
        ))
    })?;

    expand_graph_references(
        &pipeline.source.table,
        src_driver,
        mapping,
        refs,
        dest_dialect,
    )
    .await
}

async fn expand_graph_references(
    root_table: &str,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
    refs: &GraphReferences,
    dest_dialect: Dialect,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    let source_dialect = src_driver.dialect();

    let result = dispatch_driver!(src_driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        let type_registry = Arc::new(TypeRegistry::new(source_dialect, dest_dialect));
        let expander = GraphExpander::new(introspector, type_registry, source_dialect);

        expander
            .expand(
                root_table,
                refs,
                mapping,
                SchemaObjectFlags::default(),
                false,
            )
            .await
            .map_err(|e| VerifyError::InitializationError(e.to_string()))?
    });

    Ok(matches!(refs.data_mode, DataMode::Cascade).then_some(result.discovered_tables))
}
