use crate::builder::plugin_validation::blocking_input_reason;
use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::SampleCollectorError,
        utils::MaskingPolicy,
    },
    plan::{
        sample::{
            issue::{SampleIssue, SampleIssueLevel},
            method::SamplingMethod,
            preview::{SampleDataPreview, SampleQuery},
            row::{SampleRow, SampleRowStatus, SampleValidationResult, SampleValue},
            stats::{SampleStats, ValidationStats},
        },
        validation::plan::ValidationPlan,
    },
};
use async_trait::async_trait;
use connectors::sql::{
    filter::compile::compile_filter,
    query::generator::QueryGenerator,
    request::{FetchRowsRequest, FetchRowsRequestBuilder},
};
use engine_processing::io::{
    driver::SchemaDriver, filter::combine_filters, format::DataFormat, linked::LinkedSource,
};
use engine_processing::{
    EnvContext,
    producer::build_transform_pipeline,
    transform::{
        error::TransformError,
        pipeline::{ApplyOutcome, TransformPipeline, ValidationWarning},
    },
};
use engine_wasm::registry::{PluginRegistry, unexecutable_plugin_reason};
use model::{
    core::{types::Type, value::Value},
    execution::pipeline::Pipeline,
    records::Record,
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tracing::info;

/// Configuration for sample collection behavior and privacy
#[derive(Clone, Debug)]
pub struct SampleConfig {
    /// Whether sampling is enabled
    pub enabled: bool,
    pub size: usize,
    pub method: SamplingMethod,
    pub mask_columns: Vec<String>,
    pub auto_mask_sensitive: bool,
    pub sample_ids: Option<Vec<Value>>,
    pub id_column: String,
}

impl Default for SampleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            size: 5,
            method: SamplingMethod::First,
            mask_columns: Vec::new(),
            auto_mask_sensitive: true,
            sample_ids: None,
            id_column: "id".to_string(),
        }
    }
}

struct ValidationContext<'a> {
    validations: &'a [ValidationPlan],
    mapping: &'a TransformationMetadata,
    val_stats: &'a mut HashMap<String, (usize, usize)>,
    results: &'a mut Vec<SampleValidationResult>,
}

pub struct SampleProcessor {
    config: SampleConfig,
}

impl SampleProcessor {
    pub fn new(config: SampleConfig) -> Self {
        Self { config }
    }

    /// Transform and validate pre-fetched `source_rows` into a preview. `query`
    /// is the SQL description shown in DB output (`None` for plugin sources).
    #[allow(clippy::too_many_arguments)]
    pub fn process(
        &self,
        mut source_rows: Vec<Record>,
        pipeline: &Pipeline,
        plugin_registry: &PluginRegistry,
        mapping: &TransformationMetadata,
        validations: &[ValidationPlan],
        mapped_columns_only: bool,
        query: Option<SampleQuery>,
        start: Instant,
    ) -> Result<SampleDataPreview, SampleCollectorError> {
        if let Some(reason) = unexecutable_plugin_reason(pipeline, plugin_registry) {
            return Ok(self.unavailable_preview(start, query, reason));
        }

        if source_rows.is_empty() {
            return Ok(self.empty_preview(start, query));
        }

        // Don't run a plugin the plan has already flagged
        let available: HashMap<String, Type> = source_rows
            .first()
            .map(|r| {
                r.schema()
                    .columns()
                    .iter()
                    .map(|c| (c.name.to_string(), c.data_type.clone()))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(reason) = blocking_input_reason(pipeline, plugin_registry, &available) {
            return Ok(self.unavailable_preview(start, query, reason));
        }

        source_rows
            .iter_mut()
            .for_each(|r| r.set_table(&pipeline.source.table));

        let transform_pipeline = build_transform_pipeline(
            pipeline,
            plugin_registry,
            mapping,
            mapped_columns_only,
            Arc::new(EnvContext::empty()),
        )
        .map_err(|e| SampleCollectorError::PipelineBuildFailed(e.to_string()))?;

        let mut sample_rows = Vec::with_capacity(source_rows.len());
        let mut val_stats: HashMap<String, (usize, usize)> = HashMap::new();

        for (idx, mut row) in source_rows.into_iter().enumerate() {
            sample_rows.push(self.process_sample_row(
                idx,
                &mut row,
                &transform_pipeline,
                validations,
                mapping,
                &mut val_stats,
            ));
        }

        info!(table = %pipeline.source.table, count = sample_rows.len(), "collected sample rows");

        let unavailable_reason = self.sample_unavailable_reason(&sample_rows);
        let stats = self.aggregate_stats(&sample_rows, &val_stats);
        let issues = sample_rows
            .iter()
            .flat_map(|r| r.issues.iter().cloned())
            .collect();

        Ok(SampleDataPreview {
            enabled: true,
            sampled_at: Some(chrono::Utc::now()),
            sample_size: sample_rows.len(),
            sampling_method: self.config.method.clone(),
            duration_ms: Some(start.elapsed().as_millis() as u64),
            query,
            stats,
            issues,
            rows: sample_rows,
            unavailable_reason,
        })
    }

    fn sample_unavailable_reason(&self, rows: &[SampleRow]) -> Option<String> {
        (!rows.is_empty() && rows.iter().all(|r| r.status == SampleRowStatus::Failed)).then(|| {
            format!(
                "all {} sampled rows failed to transform - see diagnostics",
                rows.len()
            )
        })
    }

    fn unavailable_preview(
        &self,
        start: Instant,
        query: Option<SampleQuery>,
        reason: String,
    ) -> SampleDataPreview {
        SampleDataPreview {
            enabled: true,
            sampled_at: Some(chrono::Utc::now()),
            sample_size: 0,
            sampling_method: self.config.method.clone(),
            duration_ms: Some(start.elapsed().as_millis() as u64),
            query,
            rows: Vec::new(),
            stats: SampleStats::default(),
            issues: Vec::new(),
            unavailable_reason: Some(reason),
        }
    }

    /// Handles the transformation and validation lifecycle for a single row.
    fn process_sample_row(
        &self,
        idx: usize,
        row: &mut Record,
        pipeline: &TransformPipeline,
        validations: &[ValidationPlan],
        mapping: &TransformationMetadata,
        val_stats: &mut HashMap<String, (usize, usize)>,
    ) -> SampleRow {
        let input_values = self.map_to_sample_values(row);
        let source_id = self.extract_identifier(row);

        let mut status = SampleRowStatus::Ok;
        let mut issues = Vec::new();
        let mut output = None;
        let mut validation_results = Vec::new();

        match pipeline.apply(row) {
            Ok(ApplyOutcome::Success) => {
                output = Some(self.map_to_sample_values(row));
                self.record_passed(validations, val_stats, &mut validation_results);
            }
            Ok(ApplyOutcome::Warning { warnings }) => {
                output = Some(self.map_to_sample_values(row));
                status = SampleRowStatus::Warning;
                self.handle_validation_warnings(
                    idx,
                    row,
                    &warnings,
                    &mut ValidationContext {
                        validations,
                        mapping,
                        val_stats,
                        results: &mut validation_results,
                    },
                    &mut issues,
                );
            }
            Ok(ApplyOutcome::Skipped { reason }) => {
                status = SampleRowStatus::Skipped;
                issues.push(self.info_issue(
                    idx,
                    "SKIPPED",
                    &reason.unwrap_or_else(|| "Filtered by logic".into()),
                ));
            }
            Err(e) => {
                let (err_status, issue) = self.handle_transform_error(
                    idx,
                    e,
                    row,
                    &mut ValidationContext {
                        validations,
                        mapping,
                        val_stats,
                        results: &mut validation_results,
                    },
                );
                status = err_status;
                issues.push(issue);
            }
        }

        SampleRow {
            index: idx,
            source_id,
            input: input_values,
            output,
            status,
            validations: validation_results,
            issues,
        }
    }

    fn record_passed(
        &self,
        validations: &[ValidationPlan],
        val_stats: &mut HashMap<String, (usize, usize)>,
        results: &mut Vec<SampleValidationResult>,
    ) {
        for v in validations {
            val_stats.entry(v.name.clone()).or_insert((0, 0)).0 += 1;
            results.push(SampleValidationResult {
                name: v.name.clone(),
                passed: true,
                check: v.check.expression.clone(),
                actual_values: String::new(),
                message: None,
            });
        }
    }

    fn handle_validation_warnings(
        &self,
        idx: usize,
        row: &Record,
        warnings: &[ValidationWarning],
        val_ctx: &mut ValidationContext,
        issues: &mut Vec<SampleIssue>,
    ) {
        let failed_names: HashSet<_> = warnings.iter().map(|w| w.rule.clone()).collect();

        for warning in warnings {
            val_ctx
                .val_stats
                .entry(warning.rule.clone())
                .or_insert((0, 0))
                .1 += 1;

            if let Some(v) = val_ctx
                .validations
                .iter()
                .find(|val| val.name == warning.rule)
            {
                val_ctx.results.push(SampleValidationResult {
                    name: v.name.clone(),
                    passed: false,
                    check: v.check.expression.clone(),
                    actual_values: self.format_val_context(
                        row,
                        &v.check.columns_referenced,
                        val_ctx.mapping,
                    ),
                    message: Some(warning.message.clone()),
                });
            }

            issues.push(SampleIssue {
                level: SampleIssueLevel::Warning,
                code: "VALIDATION_WARNING".into(),
                message: format!("Warning for '{}': {}", warning.rule, warning.message),
                row_index: Some(idx),
                column: None,
                suggestion: Some("Verify input data constraints".into()),
            });
        }

        for v in val_ctx
            .validations
            .iter()
            .filter(|v| !failed_names.contains(&v.name))
        {
            val_ctx.val_stats.entry(v.name.clone()).or_insert((0, 0)).0 += 1;
            val_ctx.results.push(SampleValidationResult {
                name: v.name.clone(),
                passed: true,
                check: v.check.expression.clone(),
                actual_values: String::new(),
                message: None,
            });
        }
    }

    fn handle_transform_error(
        &self,
        idx: usize,
        err: TransformError,
        row: &Record,
        val_ctx: &mut ValidationContext,
    ) -> (SampleRowStatus, SampleIssue) {
        match err {
            TransformError::ValidationFailed { rule, message } => {
                val_ctx.val_stats.entry(rule.clone()).or_insert((0, 0)).1 += 1;
                if let Some(v) = val_ctx.validations.iter().find(|val| val.name == rule) {
                    val_ctx.results.push(SampleValidationResult {
                        name: v.name.clone(),
                        passed: false,
                        check: v.check.expression.clone(),
                        actual_values: self.format_val_context(
                            row,
                            &v.check.columns_referenced,
                            val_ctx.mapping,
                        ),
                        message: Some(message.clone()),
                    });
                }
                (
                    SampleRowStatus::Failed,
                    SampleIssue {
                        level: SampleIssueLevel::Failed,
                        code: "VALIDATION_FAILED".into(),
                        message: format!("Validation '{}' failed: {}", rule, message),
                        row_index: Some(idx),
                        column: None,
                        suggestion: Some("Review constraints".into()),
                    },
                )
            }
            TransformError::FilteredOut => (
                SampleRowStatus::Skipped,
                self.info_issue(idx, "FILTERED", "Row filtered"),
            ),
            _ => (
                SampleRowStatus::Failed,
                SampleIssue {
                    level: SampleIssueLevel::Failed,
                    code: "TRANSFORM_ERROR".into(),
                    message: format!("Error: {}", err),
                    row_index: Some(idx),
                    column: None,
                    suggestion: Some("Check mapping logic and expressions".into()),
                },
            ),
        }
    }

    /// Resolves column values from the current row, accounting for table aliases in joins.
    fn resolve_val(
        &self,
        row: &Record,
        col_ref: &str,
        mapping: &TransformationMetadata,
    ) -> Option<Value> {
        if let Some(v) = row.value(col_ref) {
            return Some(v.clone());
        }

        if let Some((alias, field)) = col_ref.split_once('.') {
            // Find target field through foreign_fields mapping
            let target_val = mapping
                .foreign_fields
                .values()
                .flatten()
                .find(|cr| {
                    cr.entity.eq_ignore_ascii_case(alias) && cr.field.eq_ignore_ascii_case(field)
                })
                .and_then(|cr| cr.target.as_ref())
                .and_then(|target| row.value(target));

            if let Some(v) = target_val {
                return Some(v.clone());
            }

            if let Some(v) = row.value(field) {
                return Some(v.clone());
            }
        }

        row.value(col_ref.split('.').next_back()?).cloned()
    }

    fn format_val_context(
        &self,
        row: &Record,
        cols: &[String],
        mapping: &TransformationMetadata,
    ) -> String {
        if cols.is_empty() {
            return "<no columns referenced>".into();
        }

        cols.iter()
            .map(|col| {
                let val_str = self
                    .resolve_val(row, col, mapping)
                    .and_then(|v| v.as_string())
                    .unwrap_or_else(|| "NULL".into());
                format!("{col}={val_str}")
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn map_to_sample_values(&self, row: &Record) -> HashMap<String, SampleValue> {
        const MAX_DISPLAY: usize = 120;

        row.iter()
            .map(|f| {
                let (display, is_null, truncated, len) = match &f.value {
                    None | Some(Value::Null) => ("NULL".into(), true, false, None),
                    Some(v) => {
                        let mut s = v.as_string().unwrap_or_else(|| format!("{:?}", v));
                        if s.len() > MAX_DISPLAY {
                            // Safely truncate string up to MAX_DISPLAY ensuring we land on a char boundary
                            let mut end = MAX_DISPLAY;
                            while end > 0 && !s.is_char_boundary(end) {
                                end -= 1;
                            }
                            s.truncate(end);
                            s.push_str("...");
                            (s.clone(), false, true, Some(s.len()))
                        } else {
                            (s, false, false, None)
                        }
                    }
                };

                (
                    f.name.to_string(),
                    SampleValue {
                        display,
                        value_type: format!("{:?}", f.data_type),
                        is_null,
                        truncated,
                        original_length: len,
                    },
                )
            })
            .collect()
    }

    fn aggregate_stats(
        &self,
        rows: &[SampleRow],
        val_results: &HashMap<String, (usize, usize)>,
    ) -> SampleStats {
        let (mut ok, mut warnings, mut skipped, mut errors) = (0, 0, 0, 0);

        for r in rows {
            match r.status {
                SampleRowStatus::Ok => ok += 1,
                SampleRowStatus::Warning => warnings += 1,
                SampleRowStatus::Skipped => skipped += 1,
                SampleRowStatus::Failed => errors += 1,
            }
        }

        let validation_stats = val_results
            .iter()
            .map(|(name, &(passed, failed))| {
                let total = passed + failed;
                ValidationStats {
                    name: name.clone(),
                    passed,
                    failed,
                    pass_rate: if total > 0 {
                        passed as f32 / total as f32
                    } else {
                        0.0
                    },
                }
            })
            .collect();

        SampleStats {
            ok,
            warnings,
            skipped,
            errors,
            validation_stats,
        }
    }

    fn extract_identifier(&self, row: &Record) -> Option<String> {
        let candidates = ["id", "_id", "uuid", "pk", self.config.id_column.as_str()];

        row.iter()
            .find(|f| candidates.iter().any(|&c| f.name.eq_ignore_ascii_case(c)))
            .and_then(|f| f.value.as_ref().map(|v| format!("{:?}", v)))
    }

    fn empty_preview(&self, start: Instant, query: Option<SampleQuery>) -> SampleDataPreview {
        SampleDataPreview {
            enabled: true,
            sampled_at: Some(chrono::Utc::now()),
            sample_size: 0,
            sampling_method: self.config.method.clone(),
            duration_ms: Some(start.elapsed().as_millis() as u64),
            query,
            rows: Vec::new(),
            stats: SampleStats::default(),
            issues: vec![self.info_issue(0, "EMPTY", "No source data found")],
            unavailable_reason: None,
        }
    }

    fn info_issue(&self, idx: usize, code: &str, msg: &str) -> SampleIssue {
        SampleIssue {
            level: SampleIssueLevel::Info,
            code: code.into(),
            message: msg.into(),
            row_index: Some(idx),
            column: None,
            suggestion: None,
        }
    }

    pub fn apply_masking(&self, preview: &mut SampleDataPreview, masking: &MaskingPolicy) {
        for row in &mut preview.rows {
            for val in row.input.values_mut() {
                if masking.should_mask(&val.display) && !val.is_null {
                    val.display = masking.mask_value(&val.display);
                }
            }
            if let Some(out) = &mut row.output {
                for val in out.values_mut() {
                    if masking.should_mask(&val.display) && !val.is_null {
                        val.display = masking.mask_value(&val.display);
                    }
                }
            }
        }
    }
}

/// Orchestrates the collection, transformation, and validation of data samples
/// to provide a "dry-run" preview of the pipeline's behavior.
pub struct SampleCollector<S: SchemaDriver> {
    src_driver: Arc<S>,
    config: SampleConfig,
    processor: SampleProcessor,
}

impl<S: SchemaDriver> SampleCollector<S> {
    pub fn new(src_driver: Arc<S>, config: SampleConfig) -> Self {
        Self {
            src_driver,
            config: config.clone(),
            processor: SampleProcessor::new(config),
        }
    }

    pub async fn collect<D: SchemaDriver>(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        validations: &[ValidationPlan],
        mapped_columns_only: bool,
        masking: &MaskingPolicy,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<SampleDataPreview, SampleCollectorError> {
        let start = Instant::now();

        let (source_rows, query) = self.fetch_sample(pipeline, mapping, masking, ctx).await?;

        // Running the transform pipeline can invoke WASM plugins whose WASI ops
        // (fs/env) block on Tokio internally - which panics on an async worker
        // thread. `block_in_place` moves this thread out of the async executor so
        // those blocking calls are legal (the same reason apply runs plugins on a
        // blocking thread).
        tokio::task::block_in_place(|| {
            self.processor.process(
                source_rows,
                pipeline,
                &ctx.plugin_registry,
                mapping,
                validations,
                mapped_columns_only,
                query,
                start,
            )
        })
    }

    /// Build the `SELECT` request the sample runs: the source table's columns
    /// (plus any `with { }` join columns so computed fields can resolve), the
    /// `where` filter, and the sampling method (first / random / by-id).
    async fn build_sample_request(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
    ) -> Result<FetchRowsRequest, SampleCollectorError> {
        let table = || pipeline.source.table.clone();
        let query_err = |e: String| SampleCollectorError::QueryExecutionFailed {
            table: table(),
            error: e,
        };

        let source_meta = self
            .src_driver
            .table_metadata(&pipeline.source.table)
            .await
            .map_err(|e| query_err(e.to_string()))?;

        let mut columns = source_meta.select_fields();
        let mut joins = Vec::new();

        if !pipeline.source.joins.is_empty() {
            let format =
                DataFormat::parse(&pipeline.source.connection.driver).ok_or_else(|| {
                    query_err(format!(
                        "unsupported source driver '{}'",
                        pipeline.source.connection.driver
                    ))
                })?;

            if let Some(LinkedSource::Table(join_source)) = LinkedSource::new(
                self.src_driver.clone(),
                &format,
                &pipeline.source.joins,
                mapping,
            )
            .await
            .map_err(|e| query_err(e.to_string()))?
            {
                columns.extend(join_source.fields());
                joins = join_source.clauses.clone();
            }
        }

        let filter_clause = combine_filters(&pipeline.source.filters)
            .map(|cond| compile_filter(&cond))
            .transpose()
            .map_err(|e| query_err(e.to_string()))?
            .map(|c| c.for_table(&pipeline.source.table, &joins));

        let mut request = FetchRowsRequestBuilder::new(table())
            .alias(table())
            .columns(columns)
            .joins(joins)
            .filter(filter_clause)
            .limit(self.config.size)
            .build();

        match self.config.method {
            SamplingMethod::Random => request.order_random = true,
            SamplingMethod::ById => {
                let ids = self.config.sample_ids.as_ref().ok_or(
                    SampleCollectorError::MissingRequiredConfig {
                        field: "sample_ids".into(),
                        method: "ById".into(),
                    },
                )?;
                request.in_clause = Some((self.config.id_column.clone(), ids.clone()));
            }
            SamplingMethod::Stratified => {
                return Err(SampleCollectorError::UnsupportedSamplingMethod {
                    method: "Stratified".into(),
                });
            }
            SamplingMethod::First => {}
        }

        Ok(request)
    }

    async fn fetch_sample<D: SchemaDriver>(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        masking: &MaskingPolicy,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<(Vec<Record>, Option<SampleQuery>), SampleCollectorError> {
        let request = self.build_sample_request(pipeline, mapping).await?;
        let dialect = ctx.source_dialect.as_query_dialect();
        let generator = QueryGenerator::new(dialect);
        let (sql, params) = generator.select(&request);

        let query = Some(SampleQuery {
            sql: sql.clone(),
            params: self.format_query_params(&params, masking),
        });

        let rows = self
            .src_driver
            .query_params(&sql, &params)
            .await
            .map_err(|e| SampleCollectorError::QueryExecutionFailed {
                table: pipeline.source.table.clone(),
                error: e.to_string(),
            })?;

        Ok((rows, query))
    }

    fn format_query_params(&self, params: &[Value], masking: &MaskingPolicy) -> Vec<String> {
        params
            .iter()
            .map(|value| self.format_query_param(value, masking))
            .collect()
    }

    fn format_query_param(&self, value: &Value, masking: &MaskingPolicy) -> String {
        let raw = value.as_string().unwrap_or_else(|| format!("{:?}", value));

        if MaskingPolicy::is_db_url(&raw) {
            return MaskingPolicy::mask_url(&raw);
        }

        if !masking.auto_mask_sensitive {
            return raw;
        }

        // Mask string-like values that might contain sensitive data
        match value {
            Value::String(_) | Value::Null => masking.mask_value(&raw),
            _ => raw,
        }
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for SampleCollector<S> {
    type Input = (Pipeline, TransformationMetadata, Vec<ValidationPlan>, bool);
    type Output = SampleDataPreview;

    fn name(&self) -> &'static str {
        "sample"
    }

    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        let (pipeline, mapping, validations, mapped_columns_only) = input;

        let mut preview = self
            .collect(
                pipeline,
                mapping,
                validations,
                *mapped_columns_only,
                &ctx.masking,
                ctx,
            )
            .await
            .map_err(|e| AnalyzerError::error("sample", e.to_string()))?;

        self.processor.apply_masking(&mut preview, &ctx.masking);
        Ok(preview)
    }
}
