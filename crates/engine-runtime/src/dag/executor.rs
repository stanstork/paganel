use crate::{
    calibration::CalibrationRecorder,
    dag::{
        Dag,
        endpoint::{SourceEndpoint, resolve_destination, resolve_source},
    },
    error::MigrationError,
    execution::orchestrator::PipelineOrchestrator,
};
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    plan::execution::ExecutionPlan,
    utils::make_item_id,
};
use engine_infra::event_bus::bus::EventBus;
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::{
    context::PipelineContext,
    io::{destination::Destination, source::Source},
};
use engine_state::models::{PauseReason, PipelineRunState, PipelineStatus, RunState, RunStatus};
use engine_state::{RowHashLog, RunLock, StateStore, models::WalEntry};
use engine_wasm::registry::{PluginRegistry, load_registry, plugin_columns};
use futures::stream::{self, StreamExt};
use model::{
    events::migration::MigrationEvent,
    execution::{
        config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
        flags::ExecutionFlags,
        pipeline::Pipeline,
    },
};
use model::{pagination::cursor::Cursor, transform::mapping::TransformationMetadata};
use query_builder::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

pub struct DagExecutor {
    plan: ExecutionPlan,
    flags: ExecutionFlags,
    shutdown: ShutdownSignal,
    exec_ctx: ExecutionContext,
    exec_config: ExecutionConfig,
    event_bus: EventBus,
    done_ops: Arc<Mutex<HashSet<String>>>,
    plugin_registry: Arc<PluginRegistry>,
    calibration: CalibrationRecorder,
    _run_lock: RunLock,
}

impl DagExecutor {
    /// Create executor without event bus (headless mode)
    pub async fn new(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        let event_bus = EventBus::new();
        Self::subscribe_to_events(&event_bus).await;
        Self::init(plan, flags, shutdown, event_bus, env).await
    }

    /// Create executor with event bus for external monitoring (TUI/Pretty mode)
    pub async fn with_event_bus(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        Self::init(plan, flags, shutdown, event_bus, env).await
    }

    async fn init(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;

        let state_dir = home_dir.join(".paganel/state");
        let state = Arc::new(StateStore::open(&state_dir).map_err(|e| {
            MigrationError::InitializationError(format!("Failed to open state store: {e}"))
        })?);

        // Held for the life of the executor
        let run_lock = RunLock::acquire(&state_dir, &plan.run_id())?;
        let hash_log = Arc::new(RowHashLog::in_state_dir(&state_dir));
        let exec_ctx = ExecutionContext::new(&plan, state, hash_log, env.clone());
        let exec_config = plan.execution_config.clone();
        let plugin_registry = load_registry(&plan.plugins, &|name| env.get(name))?;

        Ok(Self {
            plan,
            flags,
            shutdown,
            exec_ctx,
            exec_config,
            event_bus,
            done_ops: Arc::new(Mutex::new(HashSet::new())),
            plugin_registry,
            calibration: CalibrationRecorder::new(),
            _run_lock: run_lock,
        })
    }

    async fn subscribe_to_events(event_bus: &EventBus) {
        let (event_tx, mut event_rx) = mpsc::channel::<Arc<MigrationEvent>>(100);
        event_bus.subscribe::<MigrationEvent>(event_tx).await;

        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                debug!(event = %event, "migration event");
            }
        });

        debug!("event subscriber configured");
    }

    pub async fn execute(self, dag: Dag) -> Result<(), MigrationError> {
        let session_start = std::time::Instant::now();

        let mut failed_pipelines = HashSet::new();

        // Initialize state or resume from a paused run
        let (mut run_state, mut completed_pipelines) = self.init_or_resume_run().await?;

        // Execute levels, updating run_state as pipelines complete
        let run_result = self
            .execute_levels(
                &dag,
                &mut run_state,
                &mut completed_pipelines,
                &mut failed_pipelines,
            )
            .await;

        // Complete run and finalize state
        self.finalize_run(run_result, run_state, failed_pipelines, session_start)
            .await
    }

    async fn init_or_resume_run(&self) -> Result<(RunState, HashSet<String>), MigrationError> {
        let run_id = self.exec_ctx.run_id().to_string();
        let existing_run = self.exec_ctx.state().load_run_state(&run_id).await?;

        let resuming = matches!(
            existing_run.as_ref().map(|r| &r.status),
            Some(RunStatus::Paused { .. })
        );

        let completed_pipelines: HashSet<String> = existing_run
            .as_ref()
            .map(|r| {
                r.pipelines
                    .iter()
                    .filter(|p| p.status == PipelineStatus::Completed)
                    .map(|p| p.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        if resuming {
            info!(
                run_id = %run_id,
                completed = completed_pipelines.len(),
                "resuming paused migration"
            );
        }

        let run_state = self.build_initial_run_state(&run_id, existing_run, &completed_pipelines);

        self.exec_ctx.state().save_run_state(&run_state).await?;

        let wal_entry = if resuming {
            WalEntry::RunResumed { run_id }
        } else {
            WalEntry::RunStart {
                run_id,
                plan_hash: self.plan.hash().to_string(),
            }
        };
        self.exec_ctx.state().append_wal(&wal_entry).await?;

        Ok((run_state, completed_pipelines))
    }

    fn build_initial_run_state(
        &self,
        run_id: &str,
        existing_run: Option<RunState>,
        completed_pipelines: &HashSet<String>,
    ) -> RunState {
        let pipelines = self
            .plan
            .pipelines
            .iter()
            .enumerate()
            .map(|(idx, p)| {
                let item_id = make_item_id(self.plan.hash(), &p.destination.table, idx);

                if completed_pipelines.contains(&p.name) {
                    let rows_done = existing_run
                        .as_ref()
                        .and_then(|r| r.pipelines.iter().find(|ps| ps.name == p.name))
                        .map(|ps| ps.rows_done)
                        .unwrap_or(0);

                    PipelineRunState {
                        name: p.name.clone(),
                        item_id,
                        status: PipelineStatus::Completed,
                        rows_done,
                        total_rows: None,
                    }
                } else {
                    let rows_done = existing_run
                        .as_ref()
                        .and_then(|r| r.pipelines.iter().find(|ps| ps.name == p.name))
                        .map(|ps| ps.rows_done)
                        .unwrap_or(0);

                    PipelineRunState {
                        name: p.name.clone(),
                        item_id,
                        status: PipelineStatus::Pending,
                        rows_done,
                        total_rows: None,
                    }
                }
            })
            .collect();

        RunState {
            run_id: run_id.to_string(),
            config_path: self.plan.config_path.clone(),
            config_hash: self.plan.hash().to_string(),
            status: RunStatus::Running,
            started_at: existing_run
                .map(|r| r.started_at)
                .unwrap_or_else(chrono::Utc::now),
            total_pipelines: self.plan.pipelines.len(),
            pipelines,
        }
    }

    async fn execute_levels(
        &self,
        dag: &Dag,
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
        failed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        let execution_order = dag.execution_order();
        info!(
            pipelines = dag.total_pipelines(),
            levels = execution_order.len(),
            "starting migration"
        );

        for (level_idx, level) in execution_order.iter().enumerate() {
            let level_remaining: Vec<String> = level
                .iter()
                .filter(|name| !completed_pipelines.contains(*name))
                .cloned()
                .collect();

            let executable = self.filter_executable(&level_remaining, failed_pipelines);

            if executable.is_empty() {
                if level_remaining.is_empty() {
                    info!(level = level_idx + 1, "level already completed, skipping");
                } else {
                    warn!(level = level_idx + 1, "all pipelines in level skipped");
                }
                continue;
            }

            info!(
                level = level_idx + 1,
                levels = execution_order.len(),
                executing = executable.len(),
                skipped = level.len() - executable.len(),
                pipelines = ?executable,
                "executing level"
            );

            // Check for process cancellation
            if self.shutdown.cancel.is_cancelled() {
                warn!("shutdown requested, aborting migration");
                return Err(MigrationError::ShutdownRequested);
            }

            // Check for requested pauses
            if self.shutdown.pause.is_cancelled() {
                info!("pause requested, saving state");
                self.save_paused_state(
                    run_state,
                    failed_pipelines,
                    completed_pipelines,
                    PauseReason::Manual,
                )
                .await?;
                return Err(MigrationError::Paused);
            }

            // Execute the current level's pipelines
            match self
                .execute_level(
                    &executable,
                    run_state,
                    completed_pipelines,
                    failed_pipelines,
                )
                .await
            {
                Err(MigrationError::Paused) => {
                    info!(
                        level = level_idx + 1,
                        "pause detected during level, saving state"
                    );

                    self.save_paused_state(
                        run_state,
                        failed_pipelines,
                        completed_pipelines,
                        PauseReason::Manual,
                    )
                    .await?;

                    return Err(MigrationError::Paused);
                }
                Err(e) => return Err(e),
                Ok(()) => {}
            }
        }

        Ok(())
    }

    async fn finalize_run(
        self,
        run_result: Result<(), MigrationError>,
        mut run_state: RunState,
        failed_pipelines: HashSet<String>,
        session_start: Instant,
    ) -> Result<(), MigrationError> {
        self.calibration.flush();

        match run_result {
            // Process finalize state when the migration actually completed cleanly or handled its pipeline failures
            Ok(()) | Err(MigrationError::PipelinesFailed(_)) => {
                let run_id = self.exec_ctx.run_id().to_string();
                for ps in &mut run_state.pipelines {
                    if ps.status == PipelineStatus::Pending {
                        if failed_pipelines.contains(&ps.name) {
                            ps.status = PipelineStatus::Failed {
                                error: "Failed during execution".to_string(),
                            };
                        } else {
                            ps.status = PipelineStatus::Completed;
                        }
                    }

                    // Refresh rows_done from checkpoints (authoritative cumulative
                    // count, summed across every lane/part of the pipeline).
                    if let Ok(total) = self
                        .exec_ctx
                        .state()
                        .total_rows_done(&run_id, &ps.item_id)
                        .await
                        && total > 0
                    {
                        ps.rows_done = total;
                    }
                }

                let failed = run_state
                    .pipelines
                    .iter()
                    .filter(|p| matches!(p.status, PipelineStatus::Failed { .. }))
                    .map(|p| p.name.clone())
                    .collect::<Vec<_>>();

                run_state.status = if failed.is_empty() {
                    RunStatus::Completed {
                        completed_at: chrono::Utc::now(),
                    }
                } else {
                    RunStatus::Failed {
                        error: format!(
                            "{} of {} pipelines failed: {}",
                            failed.len(),
                            run_state.pipelines.len(),
                            failed.join(", ")
                        ),
                        failed_at: chrono::Utc::now(),
                    }
                };

                self.exec_ctx.state().save_run_state(&run_state).await?;
                self.exec_ctx
                    .state()
                    .append_wal(&WalEntry::RunDone {
                        run_id: self.exec_ctx.run_id().to_string(),
                    })
                    .await?;

                let total_rows: u64 = run_state.pipelines.iter().map(|p| p.rows_done).sum();
                let elapsed = session_start.elapsed().as_secs_f64();
                let rows_per_sec = if elapsed > 0.0 {
                    total_rows as f64 / elapsed
                } else {
                    0.0
                };
                info!(
                    pipelines = run_state.total_pipelines,
                    failed = failed_pipelines.len(),
                    rows = total_rows,
                    elapsed_secs = %format!("{:.2}", elapsed),
                    rows_per_sec = %format!("{:.0}", rows_per_sec),
                    "migration run summary"
                );

                self.finalize(failed_pipelines)
            }
            // Retain unhandled structural errors (Pauses, Manual Cancelations) directly
            Err(e) => Err(e),
        }
    }

    fn finalize(self, failed_pipelines: HashSet<String>) -> Result<(), MigrationError> {
        if failed_pipelines.is_empty() {
            return Ok(());
        }

        let failed_list: Vec<String> = failed_pipelines.into_iter().collect();
        error!(
            failed = failed_list.len(),
            pipelines = ?failed_list,
            "migration completed with failed/skipped pipelines"
        );

        match self.exec_config.on_failure {
            FailureStrategy::Continue => {
                warn!(
                    failed = failed_list.len(),
                    "continue strategy: reporting success despite failed/skipped pipelines"
                );
                Ok(())
            }
            FailureStrategy::FailFast => Err(MigrationError::PipelinesFailed(failed_list)),
        }
    }

    /// Filter pipelines in a level, marking skipped ones as failed so dependents propagate.
    fn filter_executable(
        &self,
        level: &[String],
        failed_pipelines: &mut HashSet<String>,
    ) -> Vec<String> {
        let mut executable = Vec::new();

        for name in level {
            let pipeline = self
                .plan
                .pipelines
                .iter()
                .find(|p| &p.name == name)
                .expect("DAG level name exists in plan pipelines");

            let has_failed_dep = pipeline
                .dependencies
                .iter()
                .any(|dep| failed_pipelines.contains(dep));

            if has_failed_dep {
                warn!(pipeline = %name, "skipping pipeline: dependency failed");
                failed_pipelines.insert(name.clone());
            } else {
                executable.push(name.clone());
            }
        }

        executable
    }

    /// Execute a level of pipelines according to the configured strategy.
    async fn execute_level(
        &self,
        executable: &[String],
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
        failed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        let results: Vec<(String, Result<u64, MigrationError>)> = match self.exec_config.strategy {
            ExecutionStrategy::Sequential => {
                let mut results = Vec::new();
                for name in executable {
                    let result = self.execute_pipeline(name).await;
                    let should_fail_fast = result.is_err()
                        && matches!(self.exec_config.on_failure, FailureStrategy::FailFast);
                    results.push((name.clone(), result));
                    if should_fail_fast {
                        break;
                    }
                }
                results
            }
            ExecutionStrategy::Parallel => {
                let max_concurrency = self.exec_config.max_concurrency.unwrap_or(4) as usize;
                stream::iter(executable.to_vec())
                    .map(|name| async move {
                        let result = self.execute_pipeline(&name).await;
                        (name, result)
                    })
                    .buffer_unordered(max_concurrency)
                    .collect()
                    .await
            }
        };

        for (name, result) in results {
            match result {
                Ok(rows) => {
                    debug!(pipeline = %name, rows, "pipeline completed");
                    self.mark_pipeline_completed(&name, rows, run_state, completed_pipelines)
                        .await?;
                }
                Err(MigrationError::Paused) => {
                    info!(pipeline = %name, "pipeline paused at batch boundary");
                    return Err(MigrationError::Paused);
                }
                Err(MigrationError::ShutdownRequested) => {
                    info!(pipeline = %name, "pipeline stopped due to shutdown");
                    return Err(MigrationError::ShutdownRequested);
                }
                Err(e) => {
                    error!(pipeline = %name, error = %e, "pipeline failed");
                    failed_pipelines.insert(name.clone());

                    if matches!(self.exec_config.on_failure, FailureStrategy::FailFast) {
                        return Err(MigrationError::PipelinesFailed(vec![name]));
                    }
                }
            }
        }

        Ok(())
    }

    /// Mark a pipeline as completed in the RunState and persist to sled.
    async fn mark_pipeline_completed(
        &self,
        name: &str,
        rows_done: u64,
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        completed_pipelines.insert(name.to_string());

        if let Some(ps) = run_state.pipelines.iter_mut().find(|p| p.name == name) {
            ps.status = PipelineStatus::Completed;
            // Cumulative rows from checkpoints (authoritative, summed across all
            // lanes), falling back to prior count + this session's metrics.
            match self
                .exec_ctx
                .state()
                .total_rows_done(self.exec_ctx.run_id(), &ps.item_id)
                .await
            {
                Ok(total) if total > 0 => ps.rows_done = total,
                _ => ps.rows_done += rows_done,
            }
        }

        self.exec_ctx.state().save_run_state(run_state).await?;
        Ok(())
    }

    /// Save RunState as paused with current pipeline statuses.
    async fn save_paused_state(
        &self,
        base_state: &RunState,
        failed_pipelines: &HashSet<String>,
        completed_pipelines: &HashSet<String>,
        reason: PauseReason,
    ) -> Result<(), MigrationError> {
        let mut state = base_state.clone();
        state.status = RunStatus::Paused {
            reason: reason.clone(),
            paused_at: chrono::Utc::now(),
        };

        let run_id = self.exec_ctx.run_id().to_string();
        for ps in &mut state.pipelines {
            if completed_pipelines.contains(&ps.name) {
                ps.status = PipelineStatus::Completed;
            } else if failed_pipelines.contains(&ps.name) {
                ps.status = PipelineStatus::Failed {
                    error: "Failed during execution".to_string(),
                };
            }

            // Update rows_done from checkpoints (cumulative across all lanes).
            if let Ok(total) = self
                .exec_ctx
                .state()
                .total_rows_done(&run_id, &ps.item_id)
                .await
                && total > 0
            {
                ps.rows_done = total;
            }
        }

        self.exec_ctx.state().save_run_state(&state).await?;
        self.exec_ctx
            .state()
            .append_wal(&WalEntry::RunPaused {
                run_id: self.exec_ctx.run_id().to_string(),
                reason,
            })
            .await?;

        Ok(())
    }

    /// Returns (rows_processed) on success.
    async fn execute_pipeline(&self, pipeline_name: &str) -> Result<u64, MigrationError> {
        let (idx, pipeline) = self
            .plan
            .pipelines
            .iter()
            .enumerate()
            .find(|(_, p)| p.name == pipeline_name)
            .ok_or_else(|| {
                MigrationError::PipelineFailed(format!("Pipeline '{}' not found", pipeline_name))
            })?;

        self.run_pipeline(idx, pipeline).await
    }

    #[instrument(
        skip_all,
        fields(pipeline = %pipeline.name, table = %pipeline.destination.table)
    )]
    async fn run_pipeline(&self, idx: usize, pipeline: &Pipeline) -> Result<u64, MigrationError> {
        let start_time = std::time::Instant::now();
        info!("starting pipeline");

        let isolated = matches!(self.exec_config.strategy, ExecutionStrategy::Parallel);

        let source_ep: Arc<dyn SourceEndpoint> = Arc::from(
            resolve_source(
                &pipeline.source.connection,
                &self.exec_ctx,
                &self.plugin_registry,
                isolated,
            )
            .await?,
        );
        let dest_ep = resolve_destination(
            &pipeline.destination.connection,
            &self.exec_ctx,
            &self.plugin_registry,
            isolated,
        )
        .await?;

        let mut mapping = TransformationMetadata::new(pipeline);
        mapping.set_plugin_columns(plugin_columns(pipeline, &self.plugin_registry));

        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination)
            .map_err(|e| MigrationError::Unexpected(e.to_string()))?;

        let source = source_ep
            .build(pipeline, &mapping, offset_strategy.clone())
            .await?;
        let destination = dest_ep.build(pipeline, source_ep.dialect()).await?;

        // Build context and log start
        let mut pipeline_ctx = self
            .create_pipeline_context(
                idx,
                pipeline,
                source.source,
                destination,
                mapping,
                offset_strategy,
            )
            .await?;

        // Validate settings and collect schema ops (no DDL execution)
        let (settings, mut schema_ops) = dest_ep
            .plan_settings(
                &mut pipeline_ctx,
                source_ep.as_ref(),
                pipeline,
                self.flags.dry_run,
                self.flags.integrity,
            )
            .await?;

        // Graph-expansion ops (already topo-sorted) replace settings-based ops.
        if let Some(expanded) = source.schema_ops {
            schema_ops = expanded;
        }

        let lanes = settings.lanes();

        let orchestrator = PipelineOrchestrator::new(
            pipeline.clone(),
            pipeline_ctx,
            source_ep.clone(),
            dest_ep,
            settings,
            schema_ops,
            self.shutdown.clone(),
            self.event_bus.clone(),
            self.done_ops.clone(),
            source.cascade_tables,
        );

        let exec_start = std::time::Instant::now();
        let rows = orchestrator.execute().await?;
        let exec_secs = exec_start.elapsed().as_secs_f64();

        info!(
            rows,
            elapsed_secs = start_time.elapsed().as_secs_f64(),
            "pipeline finished"
        );

        self.calibration.observe(
            &pipeline.destination.connection.driver,
            rows,
            exec_secs,
            lanes,
        );

        Ok(rows)
    }

    /// Initializes the `PipelineContext` and commits the initialization event to the WAL.
    async fn create_pipeline_context(
        &self,
        idx: usize,
        pipeline: &Pipeline,
        source: Source,
        destination: Destination,
        mapping: TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<PipelineContext, MigrationError> {
        let exec_ctx = Arc::new(self.exec_ctx.clone());
        let run_id = self.exec_ctx.run_id().to_string();
        let item_id = make_item_id(self.plan.hash(), &pipeline.destination.table, idx);

        let pipeline_ctx = PipelineContext::builder(exec_ctx.clone())
            .run_id(run_id.clone())
            .item_id(item_id.clone())
            .source(source)
            .destination(destination)
            .pipeline(pipeline.clone())
            .mapping(mapping)
            .state(self.exec_ctx.state().clone())
            .offset_strategy(offset_strategy)
            .cursor(Cursor::None)
            .plugin_registry(self.plugin_registry.clone())
            .build();

        pipeline_ctx
            .state
            .append_wal(&WalEntry::ItemStart { run_id, item_id })
            .await?;

        Ok(pipeline_ctx)
    }
}
