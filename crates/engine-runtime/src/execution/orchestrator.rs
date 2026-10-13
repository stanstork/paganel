use crate::{
    actor::coordinator::PipelineCoordinator,
    dag::endpoint::{DestinationEndpoint, HookPhase, SourceEndpoint},
    error::MigrationError,
};
use chrono;
use connectors::sql::metadata::table::TableMetadata;
use engine_config::settings::validated::ValidatedSettings;
use engine_infra::shutdown::ShutdownSignal;
use engine_infra::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{
    channel::{BatchEnvelope, ByteBudget},
    consumer::Consumer,
    context::PipelineContext,
    io::{destination::Destination, source::Source},
    producer::{
        Producer,
        components::integrity::{finalize_receipts, reset_row_hashes},
        config::ProducerConfig,
    },
};
use engine_schema::schema_ops::{SchemaOp, SchemaOps};
use model::integrity::{algorithm::HashAlgorithm, config::IntegrityConfig};
use model::{
    events::migration::MigrationEvent,
    execution::{
        pipeline::{Pipeline, WriteMode},
        references::DataMode,
    },
    pagination::cursor::QualCol,
};
use query_builder::offsets::{KeysetOffset, OffsetStrategyFactory};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Depth of the producer -> consumer batch channel, counted in batches. This is
/// the row-count bound on the in-flight window: for normal-width rows, memory
/// tracks row count, so a small read-ahead keeps both sides busy and absorbs
/// jitter while capping memory. Extra depth buys nothing but memory.
const BATCH_CHANNEL_CAPACITY: usize = 4;

/// Byte bound on the in-flight window, complementing the batch-count bound above
/// (whichever binds first throttles the producer). This is the wide-row guard: a batch of very
/// wide rows draws proportionally more budget, so a channel's worth of them
/// can't hold far more memory than a channel's worth of narrow rows. Sized well
/// above a full narrow-row channel so it only binds when rows are unusually wide.
const MAX_INFLIGHT_BYTES: usize = 128 * 1024 * 1024;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// A discovered graph table whose integer-key span is at least this large is
/// range-split into sub-lanes, so one big table doesn't bottleneck the
/// per-table parallelism. Smaller tables migrate as a single lane each.
const GRAPH_TABLE_RANGE_SPLIT_MIN_ROWS: u64 = 1_000_000;

/// Split the inclusive key range `[lo, hi]` into `n` contiguous half-open
/// `[start, end)` slices whose union is `[lo, hi+1)`. Remainder rows are spread
/// across the first lanes so sizes differ by at most one.
fn split_range(lo: u64, hi: u64, n: usize) -> Vec<(u64, u64)> {
    let n = n as u64;
    let span = hi - lo + 1;
    let (chunk, rem) = (span / n, span % n);

    let mut ranges = Vec::with_capacity(n as usize);
    let mut start = lo;

    for i in 0..n {
        let end = start + chunk + u64::from(i < rem);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

/// Orchestrates the complete pipeline execution lifecycle including hooks.
/// The orchestrator ensures proper sequencing and error handling across all phases.
pub struct PipelineOrchestrator {
    pipeline: Pipeline,
    ctx: PipelineContext,
    source_ep: Arc<dyn SourceEndpoint>,
    dest_ep: Box<dyn DestinationEndpoint>,
    settings: ValidatedSettings,
    schema_ops: SchemaOps,
    shutdown: ShutdownSignal,
    event_bus: EventBus,
    done_ops: Arc<Mutex<HashSet<String>>>,
    cascade_tables: Vec<String>,
}

impl PipelineOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline: Pipeline,
        ctx: PipelineContext,
        source_ep: Arc<dyn SourceEndpoint>,
        dest_ep: Box<dyn DestinationEndpoint>,
        settings: ValidatedSettings,
        schema_ops: SchemaOps,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        done_ops: Arc<Mutex<HashSet<String>>>,
        cascade_tables: Vec<String>,
    ) -> Self {
        Self {
            pipeline,
            ctx,
            source_ep,
            dest_ep,
            settings,
            schema_ops,
            shutdown,
            event_bus,
            done_ops,
            cascade_tables,
        }
    }

    /// Executes the complete pipeline lifecycle:
    /// pre-DDL -> before hooks -> data migration -> post-DDL -> after hooks
    /// Returns the number of rows processed.
    pub async fn execute(&self) -> Result<u64, MigrationError> {
        self.execute_schema_ops("pre-migration", &self.schema_ops.pre)
            .await?;
        self.execute_hooks(HookPhase::Before).await?;

        let rows = if self.is_schema_only() {
            info!("schema-only mode, skipping data migration");
            0
        } else {
            // `replace` mode: clear the destination table before loading.
            if matches!(self.pipeline.destination.mode, WriteMode::Replace) {
                self.truncate_destination().await?;
            }
            self.execute_pipeline().await?
        };

        self.execute_schema_ops("post-migration", &self.schema_ops.post)
            .await?;
        self.execute_hooks(HookPhase::After).await?;
        Ok(rows)
    }

    /// Execute a batch of schema operations against the destination endpoint,
    /// skipping any ops whose SQL has already been executed in a prior pipeline.
    async fn execute_schema_ops(
        &self,
        phase: &str,
        ops: &[SchemaOp],
    ) -> Result<(), MigrationError> {
        if ops.is_empty() {
            return Ok(());
        }

        info!(count = ops.len(), phase, "executing schema operations");

        for op in ops {
            {
                let set = self.done_ops.lock().expect("done_ops lock poisoned");
                if set.contains(&op.sql) {
                    debug!(op = %op.description, "skipping already-executed schema op");
                    continue;
                }
            }

            self.dest_ep
                .apply_schema_ops(std::slice::from_ref(op), phase)
                .await?;

            self.done_ops
                .lock()
                .expect("done_ops lock poisoned")
                .insert(op.sql.clone());
        }

        Ok(())
    }

    async fn truncate_destination(&self) -> Result<(), MigrationError> {
        let table = &self.pipeline.destination.table;
        info!(table = %table, "truncating destination table (replace mode)");
        self.ctx
            .destination
            .truncate(table)
            .await
            .map_err(|e| MigrationError::PipelineFailed(e.to_string()))
    }

    fn is_schema_only(&self) -> bool {
        self.pipeline
            .source
            .graph_references
            .as_ref()
            .is_some_and(|r| matches!(r.data_mode, DataMode::SchemaOnly))
    }

    async fn execute_hooks(&self, phase: HookPhase) -> Result<(), MigrationError> {
        let hooks = match &self.pipeline.lifecycle {
            Some(h) => h,
            None => return Ok(()),
        };

        // Check if there are actual hooks to run for this phase
        let should_run = match phase {
            HookPhase::Before => !hooks.before.is_empty(),
            HookPhase::After => !hooks.after.is_empty(),
        };

        if should_run {
            self.dest_ep.run_hooks(phase, hooks).await?;
        }

        Ok(())
    }

    /// Returns the number of rows processed.
    async fn execute_pipeline(&self) -> Result<u64, MigrationError> {
        info!("starting data migration");

        self.publish_started().await;

        let start_time = std::time::Instant::now();
        let metrics = Metrics::new();

        let dest_metas = self.fetch_destination_metadata().await?;
        let lanes = self.plan_lanes().await?;

        let single_lane = lanes.len() == 1;
        let intended_pk = if self.settings.integrity().is_enabled() && !self.settings.skip_pk() {
            self.source_ep
                .int_key_range(&self.pipeline.source.table)
                .await
                .map(|(pk, _, _)| pk)
        } else {
            None
        };
        let integrity = self.build_integrity_config(&dest_metas, intended_pk.as_deref());

        if let Some(config) = &integrity {
            let resuming = self
                .ctx
                .state
                .total_rows_done(&self.ctx.run_id, &self.ctx.item_id)
                .await
                .unwrap_or_default()
                > 0;

            if resuming {
                info!("resuming: keeping row hashes recorded before the interruption");
            } else {
                reset_row_hashes(config, self.ctx.exec_ctx.hash_log(), &self.pipeline.name)
                    .map_err(|e| {
                        MigrationError::PipelineFailed(format!("integrity reset failed: {e}"))
                    })?;
            }
        }

        let mut coordinators = Vec::with_capacity(lanes.len());

        for (source, part_id) in lanes {
            // Each lane writes on its own connection so their COPYs actually run
            // in parallel. A single lane reuses the shared destination handle.
            let destination = if single_lane {
                self.ctx.destination.clone()
            } else {
                self.dest_ep
                    .build_lane(&self.pipeline, self.source_ep.dialect())
                    .await?
            };

            let config = self.build_producer_config(integrity.clone());
            let coord = self
                .build_lane_coordinator(
                    source,
                    destination,
                    &part_id,
                    config,
                    &dest_metas,
                    &metrics,
                )
                .await?;

            coordinators.push((coord, part_id));
        }

        self.run_coordinators(coordinators, &metrics, start_time)
            .await?;

        // One receipt per table, written once every lane has finished: the root
        // commits to the complete row set, so no lane can finalize on its own.
        if let Some(config) = &integrity {
            let receipts = self.ctx.state.clone();

            finalize_receipts(
                config,
                self.ctx.exec_ctx.hash_log(),
                &receipts,
                &self.event_bus,
                &self.pipeline.name,
                &self.ctx.item_id,
                &self.pipeline.destination.table,
                &self.ctx.run_id,
                metrics.snapshot().rows_skipped,
            )
            .await
            .map_err(|e| {
                MigrationError::PipelineFailed(format!("integrity finalize failed: {e}"))
            })?;
        }

        Ok(metrics.snapshot().records_processed)
    }

    async fn plan_lanes(&self) -> Result<Vec<(Source, String)>, MigrationError> {
        let single = || vec![(self.ctx.source.clone(), "part-0".to_string())];
        let requested = self.settings.lanes();

        if requested <= 1 {
            return Ok(single());
        }

        // Lanes are for pure data movement only.
        if !self.pipeline.source.filters.is_empty() || !self.pipeline.source.joins.is_empty() {
            debug!("filters or joins present; using a single lane");
            return Ok(single());
        }

        // Graph pipelines parallelize across discovered tables rather than
        // range-splitting a single table.
        if self.pipeline.source.graph_references.is_some() {
            return self.plan_graph_table_lanes().await;
        }

        let table = &self.pipeline.source.table;
        let Some((pk, lo, hi)) = self.source_ep.int_key_range(table).await else {
            return Ok(single());
        };

        // Don't split finer than there are keys.
        let span = hi - lo + 1;
        let lane_nums = (requested as u64).min(span).max(1) as usize;
        if lane_nums <= 1 {
            return Ok(single());
        }

        let mut lanes = Vec::with_capacity(lane_nums);
        for (i, (rlo, rhi)) in split_range(lo, hi, lane_nums).into_iter().enumerate() {
            let strategy = Arc::new(
                KeysetOffset::new(vec![QualCol {
                    table: table.clone(),
                    column: pk.clone(),
                }])
                .with_lane(rlo, rhi),
            );

            let artifacts = self
                .source_ep
                .build(&self.pipeline, &self.ctx.mapping, strategy)
                .await?;

            lanes.push((artifacts.source, format!("part-{i}")));
        }

        info!(lanes = lane_nums, pk = %pk, key_min = lo, key_max = hi, "parallel range lanes enabled");
        Ok(lanes)
    }

    /// Parallelize a graph migration by giving each discovered table its own
    /// full-table lane (run through a bounded pool sized by `lanes`).
    async fn plan_graph_table_lanes(&self) -> Result<Vec<(Source, String)>, MigrationError> {
        let single = || vec![(self.ctx.source.clone(), "part-0".to_string())];

        // Source table names: the root plus every discovered table. `cascade_tables`
        // are destination names, so map them back to source names for reading.
        let mut src_tables = vec![self.pipeline.source.table.clone()];

        for dest in &self.cascade_tables {
            src_tables.push(self.ctx.mapping.entities.reverse_resolve(dest));
        }

        let mut seen = HashSet::new();
        src_tables.retain(|t| !t.is_empty() && seen.insert(t.clone()));

        if src_tables.len() <= 1 {
            return Ok(single());
        }

        let requested = self.settings.lanes();
        let mut lanes = Vec::new();

        // A single part index across all sub-lanes keeps checkpoint namespaces
        // unique whether a table contributes one lane or several range sub-lanes.
        let mut part = 0usize;
        for table in &src_tables {
            for source in self.plan_table_sublanes(table, requested).await? {
                lanes.push((source, format!("part-{part}")));
                part += 1;
            }
        }

        info!(
            tables = src_tables.len(),
            lanes = lanes.len(),
            concurrency = requested,
            "graph table parallelism enabled"
        );
        Ok(lanes)
    }

    /// Sources for one discovered table: range sub-lanes when it is large and has
    /// a single integer primary key, otherwise a single full-table source.
    async fn plan_table_sublanes(
        &self,
        table: &str,
        requested: usize,
    ) -> Result<Vec<Source>, MigrationError> {
        if let Some((pk, lo, hi)) = self.source_ep.int_key_range(table).await {
            let span = hi - lo + 1;
            let lane_nums = (requested as u64).min(span).max(1) as usize;

            if span >= GRAPH_TABLE_RANGE_SPLIT_MIN_ROWS && lane_nums > 1 {
                let mut sources = Vec::with_capacity(lane_nums);

                for (rlo, rhi) in split_range(lo, hi, lane_nums) {
                    let strategy = Arc::new(
                        KeysetOffset::new(vec![QualCol {
                            table: table.to_string(),
                            column: pk.clone(),
                        }])
                        .with_lane(rlo, rhi),
                    );

                    sources.push(self.source_ep.build_table_source(table, strategy).await?);
                }

                info!(
                    table,
                    ranges = lane_nums,
                    key_min = lo,
                    key_max = hi,
                    "range-splitting large graph table"
                );

                return Ok(sources);
            }
        }

        let offset = OffsetStrategyFactory::from_pagination(&self.pipeline.source.pagination)
            .map_err(|e| MigrationError::Unexpected(e.to_string()))?;
        let source = self.source_ep.build_table_source(table, offset).await?;

        Ok(vec![source])
    }

    /// Build one producer/consumer lane over `source`, identified by `part_id`
    /// (its own checkpoint namespace) and reporting into the shared `metrics`.
    async fn build_lane_coordinator(
        &self,
        source: Source,
        destination: Destination,
        part_id: &str,
        config: ProducerConfig,
        dest_metas: &[TableMetadata],
        metrics: &Metrics,
    ) -> Result<PipelineCoordinator, MigrationError> {
        let (batch_tx, batch_rx) = mpsc::channel::<BatchEnvelope>(BATCH_CHANNEL_CAPACITY);
        let byte_budget = ByteBudget::new(MAX_INFLIGHT_BYTES);

        let producer = Producer::new(
            &self.ctx,
            source,
            part_id,
            batch_tx,
            byte_budget,
            config,
            self.pipeline.has_projection(),
        )
        .await
        .map_err(|e| MigrationError::InitializationError(e.to_string()))?;

        let consumer = Consumer::new(
            &self.ctx,
            destination,
            batch_rx,
            dest_metas.to_vec(),
            part_id,
            metrics.clone(),
        )
        .await;

        Ok(PipelineCoordinator::new(
            producer,
            consumer,
            metrics.clone(),
            self.shutdown.cancel.clone(),
            self.event_bus.clone(),
        ))
    }

    /// Fetches destination table metadata.
    /// In cascade mode, fetches metadata for all discovered tables.
    /// Otherwise, just the single destination table.
    async fn fetch_destination_metadata(&self) -> Result<Vec<TableMetadata>, MigrationError> {
        self.dest_ep
            .destination_metadata(&self.ctx, &self.cascade_tables)
            .await
    }

    fn build_producer_config(&self, integrity: Option<IntegrityConfig>) -> ProducerConfig {
        let mut config = ProducerConfig::default().with_batch_size(self.settings.batch_size);
        if let Some(integrity) = integrity {
            config = config.with_integrity(integrity);
        }
        config
    }

    /// Build the integrity config for this run, or `None` when integrity is off.
    fn build_integrity_config(
        &self,
        dest_metas: &[TableMetadata],
        intended_pk: Option<&str>,
    ) -> Option<IntegrityConfig> {
        if !self.settings.integrity().is_enabled() {
            return None;
        }

        let capacity = dest_metas.len();
        let mut tables = HashMap::with_capacity(capacity);
        let mut column_types = HashMap::with_capacity(capacity);
        let mut key_columns = HashMap::with_capacity(capacity);

        for m in dest_metas {
            let table_name = &m.name;

            // Map columns
            tables.insert(table_name.clone(), m.columns.keys().cloned().collect());

            // Map column types
            let col_types = m
                .columns
                .values()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            column_types.insert(table_name.clone(), col_types);

            let keys = if !m.primary_keys.is_empty() {
                m.primary_keys.clone()
            } else if let Some(pk) = intended_pk.filter(|pk| {
                *table_name == self.pipeline.destination.table && m.columns.contains_key(*pk)
            }) {
                vec![pk.to_string()]
            } else {
                if !self.settings.skip_pk() {
                    warn!(
                        table = %table_name,
                        "integrity: no usable primary key; rows are keyed by their own hash, \
                         so duplicate identical rows cannot be distinguished"
                    );
                }
                Vec::new()
            };
            key_columns.insert(table_name.clone(), keys);
        }

        Some(
            IntegrityConfig::new(HashAlgorithm::Sha256, tables)
                .with_column_types(column_types)
                .with_key_columns(key_columns),
        )
    }

    /// Start every lane, then wait for all of them to finish - honoring pause and
    /// shutdown across the whole set. A single lane is the common case; N lanes
    /// run their producer/consumer pairs concurrently over disjoint key ranges.
    async fn run_coordinators(
        &self,
        coordinators: Vec<(PipelineCoordinator, String)>,
        metrics: &Metrics,
        start_time: std::time::Instant,
    ) -> Result<(), MigrationError> {
        let concurrency = self.settings.lanes().max(1);
        let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let run_id = self.ctx.run_id.clone();
        let item_id = self.ctx.item_id.clone();

        let tasks: Vec<_> = coordinators
            .into_iter()
            .map(|(coordinator, part_id)| {
                let sem = sem.clone();
                let run_id = run_id.clone();
                let item_id = item_id.clone();

                async move {
                    let _permit = sem
                        .acquire_owned()
                        .await
                        .expect("lane semaphore is never closed");
                    coordinator
                        .start_snapshot_pipeline(run_id, item_id, part_id)
                        .await?;
                    coordinator.wait().await
                }
            })
            .collect();

        // All lanes must complete; the first error aborts the join.
        let all_fut = async move { futures::future::try_join_all(tasks).await.map(|_| ()) };

        let cancel_fut = self.shutdown.cancel.cancelled();
        let pause_fut = self.shutdown.pause.cancelled();

        tokio::pin!(cancel_fut);
        tokio::pin!(pause_fut);
        tokio::pin!(all_fut);

        tokio::select! {
            result = &mut all_fut => {
                self.handle_pipeline_result(result, metrics, start_time).await
            }
            _ = &mut pause_fut => {
                self.handle_pause(all_fut).await
            }
            _ = &mut cancel_fut => {
                self.handle_shutdown(all_fut).await
            }
        }
    }

    async fn handle_pipeline_result(
        &self,
        result: Result<(), impl std::fmt::Display>,
        metrics: &Metrics,
        start_time: std::time::Instant,
    ) -> Result<(), MigrationError> {
        match result {
            Ok(()) => {
                let snap = metrics.snapshot();
                if snap.rows_skipped > 0 || snap.rows_failed > 0 {
                    warn!(
                        skipped = snap.rows_skipped,
                        failed = snap.rows_failed,
                        "pipeline completed with skipped/failed rows"
                    );
                }
                debug!("data migration completed");
                self.publish_completed(metrics, start_time).await;
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "pipeline error");
                self.publish_failed(&e.to_string(), metrics).await;
                Err(MigrationError::PipelineFailed(format!(
                    "Pipeline error: {}",
                    e
                )))
            }
        }
    }

    async fn handle_pause(
        &self,
        wait_fut: impl Future<Output = Result<(), impl std::fmt::Display>>,
    ) -> Result<(), MigrationError> {
        info!("pause signal received, draining current batch");

        // Trigger cancel so producer/consumer finish current work and stop
        self.shutdown.cancel.cancel();

        match tokio::time::timeout(SHUTDOWN_TIMEOUT, wait_fut).await {
            Ok(Ok(())) => {
                info!("pipeline paused gracefully after draining batch");
                Err(MigrationError::Paused)
            }
            Ok(Err(e)) => {
                error!(error = %e, "pipeline error during pause drain");
                Err(MigrationError::PipelineFailed(format!(
                    "Pipeline error during pause: {}",
                    e
                )))
            }
            Err(_) => {
                warn!(
                    timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                    "pipeline did not drain within timeout; progress has been checkpointed"
                );
                Err(MigrationError::Paused)
            }
        }
    }

    async fn handle_shutdown(
        &self,
        wait_fut: impl Future<Output = Result<(), impl std::fmt::Display>>,
    ) -> Result<(), MigrationError> {
        warn!(
            timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
            "shutdown signal received, waiting for in-flight operations to complete"
        );

        match tokio::time::timeout(SHUTDOWN_TIMEOUT, wait_fut).await {
            Ok(Ok(())) => {
                info!("pipeline shutdown completed gracefully");
                Err(MigrationError::ShutdownRequested)
            }
            Ok(Err(e)) => {
                error!(error = %e, "pipeline error during shutdown");
                Err(MigrationError::PipelineFailed(format!(
                    "Pipeline error during shutdown: {}",
                    e
                )))
            }
            Err(_) => {
                warn!(
                    timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                    "pipeline did not complete within timeout; progress has been checkpointed"
                );
                Err(MigrationError::ShutdownRequested)
            }
        }
    }

    async fn publish_started(&self) {
        self.event_bus
            .publish(MigrationEvent::Started {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                source: self.pipeline.source.connection.name.clone(),
                destination: self.pipeline.destination.connection.name.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;
    }

    async fn publish_completed(&self, metrics: &Metrics, start_time: std::time::Instant) {
        let snapshot = metrics.snapshot();
        self.event_bus
            .publish(MigrationEvent::Completed {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                rows_processed: snapshot.records_processed,
                rows_skipped: snapshot.rows_skipped,
                rows_failed: snapshot.rows_failed,
                duration_ms: start_time.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now(),
            })
            .await;
    }

    async fn publish_failed(&self, error: &str, metrics: &Metrics) {
        let snapshot = metrics.snapshot();
        self.event_bus
            .publish(MigrationEvent::Failed {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                error: error.to_string(),
                error_code: None,
                rows_processed: snapshot.records_processed,
                timestamp: chrono::Utc::now(),
            })
            .await;
    }
}
