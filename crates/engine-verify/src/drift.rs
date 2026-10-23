use crate::{
    VerifyOptions, VerifyProgress, VerifyState,
    error::VerifyError,
    verifier::{pipeline_tables, verify_table},
};
use chrono::Utc;
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    drivers::DriverRef,
    error::StateStoreError,
    plan::execution::ExecutionPlan,
};
use engine_state::RunLock;
use model::{
    execution::{connection::Connection, pipeline::Pipeline},
    integrity::{
        drift::{DriftRun, Skipped},
        import::{ImportRecord, import_pipeline},
        receipt::VerificationReceipt,
        report::TableReport,
    },
};
use std::{collections::HashSet, sync::Arc};

/// Something `pag drift` re-reads and compares against its stored receipt.
pub enum DriftTarget<'a> {
    Pipeline(&'a Pipeline),
    Import {
        record: ImportRecord,
        connection: &'a Connection,
    },
}

impl DriftTarget<'_> {
    pub fn name(&self) -> &str {
        match self {
            Self::Pipeline(p) => &p.name,
            Self::Import { record, .. } => &record.name,
        }
    }
}

/// Every pipeline and import the config reaches,
/// narrowed to `filters` when it is not empty.
pub async fn discover_targets<'a>(
    plan: &'a ExecutionPlan,
    state: &VerifyState,
    filters: &[String],
) -> Result<(Vec<DriftTarget<'a>>, Vec<Skipped>), VerifyError> {
    let is_selected = |name: &str| filters.is_empty() || filters.iter().any(|o| o == name);

    let mut targets: Vec<DriftTarget<'a>> = plan
        .pipelines
        .iter()
        .filter(|p| is_selected(&p.name))
        .map(DriftTarget::Pipeline)
        .collect();
    let mut skipped = Vec::new();

    for record in state.store.list_imports().await? {
        if !is_selected(&record.name) {
            continue;
        }

        match plan.get_connection(&record.connection) {
            Some(connection) => targets.push(DriftTarget::Import { record, connection }),
            None => skipped.push(Skipped {
                reason: format!("connection '{}' is not in this config", record.connection),
                name: record.name,
            }),
        }
    }

    let found: HashSet<&str> = targets
        .iter()
        .map(DriftTarget::name)
        .chain(skipped.iter().map(|s| s.name.as_str()))
        .collect();

    if let Some(unknown) = filters.iter().find(|o| !found.contains(o.as_str())) {
        return Err(VerifyError::InitializationError(format!(
            "no pipeline or import named '{unknown}'"
        )));
    }

    Ok((targets, skipped))
}

/// Check every target once. A target that fails to connect or read
/// becomes an `error` report.
pub async fn run_drift(
    plan: &ExecutionPlan,
    env: Arc<EnvContext>,
    state: &VerifyState,
    targets: &[DriftTarget<'_>],
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<DriftRun, VerifyError> {
    let started_at = Utc::now();
    let mut run = DriftRun {
        run_id: DriftRun::run_id_at(started_at),
        config: plan.config_path.clone(),
        started_at,
        finished_at: started_at,
        tables: Vec::new(),
        skipped: Vec::new(),
    };

    let has_pipelines = targets
        .iter()
        .any(|t| matches!(t, DriftTarget::Pipeline(_)));

    let lock = if has_pipelines {
        match RunLock::acquire(state.store.dir(), &plan.run_id()) {
            Ok(lock) => Some(lock),
            Err(StateStoreError::RunLocked { .. }) => None,
            Err(e) => return Err(e.into()),
        }
    } else {
        None
    };

    let pipelines_locked = has_pipelines && lock.is_none();
    let exec_ctx = ExecutionContext::new(plan, state.store.clone(), state.hash_log.clone(), env);

    for target in targets {
        match target {
            DriftTarget::Pipeline(p) if pipelines_locked => {
                run.skipped.push(Skipped {
                    name: p.name.clone(),
                    reason: "an apply, resume or verify of this config is running".into(),
                });
            }
            DriftTarget::Pipeline(p) => {
                check_pipeline(p, &exec_ctx, state, progress, options, &mut run).await?;
            }
            DriftTarget::Import { record, connection } => {
                let report =
                    check_import(record, connection, &exec_ctx, state, progress, options).await?;
                run.tables.push(report);
            }
        }
    }

    run.finished_at = Utc::now();
    Ok(run)
}

async fn check_pipeline(
    pipeline: &Pipeline,
    exec_ctx: &ExecutionContext,
    state: &VerifyState,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
    run: &mut DriftRun,
) -> Result<(), VerifyError> {
    let dest = &pipeline.destination;

    let driver = match exec_ctx.resolve_driver(&dest.connection).await {
        Ok(driver) => driver,
        Err(e) => {
            run.tables.push(TableReport::error(
                &pipeline.name,
                &dest.table,
                e.to_string(),
            ));
            return Ok(());
        }
    };

    let tables = match pipeline_tables(pipeline, &driver).await {
        Ok(tables) => tables,
        Err(e) => {
            run.tables.push(TableReport::error(
                &pipeline.name,
                &dest.table,
                e.to_string(),
            ));
            return Ok(());
        }
    };

    for table in tables {
        let Some(receipt) = state.store.load_receipt(&pipeline.name, &table).await? else {
            run.skipped.push(Skipped {
                name: format!("{}/{table}", pipeline.name),
                reason: "no receipt; run `apply --integrity` first".into(),
            });
            continue;
        };

        let report = check_table(&driver, &pipeline.name, &receipt, state, progress, options).await;
        run.tables.push(report);
    }

    Ok(())
}

async fn check_import(
    record: &ImportRecord,
    connection: &Connection,
    exec_ctx: &ExecutionContext,
    state: &VerifyState,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<TableReport, VerifyError> {
    let pipeline = import_pipeline(&record.name);

    let Some(receipt) = state.store.load_receipt(&pipeline, &record.table).await? else {
        return Ok(TableReport::error(
            &pipeline,
            &record.table,
            "the import has no receipt; run `pag state import` again",
        ));
    };

    Ok(match exec_ctx.resolve_driver(connection).await {
        Ok(driver) => check_table(&driver, &pipeline, &receipt, state, progress, options).await,
        Err(e) => TableReport::error(&pipeline, &record.table, e.to_string()),
    })
}

async fn check_table(
    driver: &DriverRef,
    pipeline: &str,
    receipt: &VerificationReceipt,
    state: &VerifyState,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> TableReport {
    let table = &receipt.table_name;
    progress.table_started(pipeline, table);

    let result = verify_table(
        driver,
        pipeline,
        table,
        receipt,
        &state.hash_log,
        progress,
        options,
    )
    .await;

    match result {
        Ok(result) => {
            progress.table_finished(&result);
            TableReport::from(&result)
        }
        Err(e) => TableReport::error(pipeline, table, e.to_string()),
    }
}
