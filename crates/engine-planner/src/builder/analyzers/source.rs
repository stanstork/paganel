use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::SourceAnalyzerError,
        explain::ExplainParser,
        infra::metadata_cache::MetadataCache,
    },
    plan::{
        connection::plan::DatabaseDriver,
        pipeline::source::{ColumnInfo, IndexInfo, SourcePlan},
    },
};
use async_trait::async_trait;
use chrono::Utc;
use connectors::sql::{
    filter::compile::compile_filter,
    metadata::{index::IndexMetadata, table::TableMetadata},
    query::generator::QueryGenerator,
    request::FetchRowsRequestBuilder,
};
use engine_processing::io::{driver::SchemaDriver, filter::combine_filters};
use model::execution::{pipeline::DataSource, row_count::RowCount};
use std::sync::Arc;
use tracing::{info, warn};

struct SourceTableMetrics {
    metadata: TableMetadata,
    indexes: Vec<IndexMetadata>,
    total_rows: RowCount,
    filtered_rows: Option<RowCount>,
    size_bytes: u64,
}

/// Analyzes source tables to gather schema metadata and statistics
pub struct SourceAnalyzer<S: SchemaDriver> {
    _cache: Arc<MetadataCache<S>>,
}

impl<S: SchemaDriver> SourceAnalyzer<S> {
    pub fn new(cache: Arc<MetadataCache<S>>) -> Self {
        Self { _cache: cache }
    }

    /// Orchestrates the full analysis of a source data entity.
    async fn analyze_source<D: SchemaDriver>(
        &self,
        source: &DataSource,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<SourcePlan> {
        info!(target: "analyzer", table = %source.table, "analyzing source metadata and statistics");

        self.ensure_table_exists(&source.table, ctx).await?;

        let metadata = self.fetch_metadata(&source.table, ctx).await?;
        let indexes = self.fetch_indexes(&source.table, ctx).await?;

        let (total_rows, filtered_rows) = self.calculate_row_metrics(source, ctx).await;
        let size_bytes = ctx
            .source_cache
            .table_size_bytes(&source.table)
            .await
            .unwrap_or(0);

        let driver = DatabaseDriver::from_name(&source.connection.driver);
        let plan = self.assemble_source_plan(
            source,
            SourceTableMetrics {
                metadata,
                indexes,
                total_rows,
                filtered_rows,
                size_bytes,
            },
            driver,
        );

        info!(
            target: "analyzer",
            table = %source.table,
            columns = plan.columns.len(),
            rows = %plan.effective_row_count().display(),
            "source analysis completed"
        );

        Ok(plan)
    }

    /// Validates that the table is reachable and exists in the source system.
    async fn ensure_table_exists<D: SchemaDriver>(
        &self,
        table: &str,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<()> {
        let exists = ctx.source_cache.table_exists(table).await.map_err(|e| {
            AnalyzerError::error(
                "source",
                format!("Failed to verify table existence for '{}': {}", table, e),
            )
        })?;

        if !exists {
            return Err(AnalyzerError::error(
                "source",
                format!("Table '{}' not found in source database", table),
            ));
        }
        Ok(())
    }

    /// Fetches structured table metadata (columns, constraints).
    async fn fetch_metadata<D: SchemaDriver>(
        &self,
        table: &str,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<TableMetadata> {
        ctx.source_cache.table_metadata(table).await.map_err(|e| {
            AnalyzerError::error(
                "source",
                format!("Failed to retrieve column metadata for '{}': {}", table, e),
            )
        })
    }

    /// Fetches physical index information for performance tuning and diagnostic checks.
    async fn fetch_indexes<D: SchemaDriver>(
        &self,
        table: &str,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Vec<IndexMetadata>> {
        ctx.source_cache.index_metadata(table).await.map_err(|e| {
            AnalyzerError::error(
                "source",
                format!("Failed to retrieve index metadata for '{}': {}", table, e),
            )
        })
    }

    /// Calculates row counts, applying any configured source filters.
    async fn calculate_row_metrics<D: SchemaDriver>(
        &self,
        source: &DataSource,
        ctx: &AnalysisContext<S, D>,
    ) -> (RowCount, Option<RowCount>) {
        let total_rows = ctx.source_cache.count_rows(&source.table, None).await;

        let filtered_rows = match combine_filters(&source.filters) {
            Some(filter) => match compile_filter(&filter) {
                Ok(sql_filter) => {
                    if ctx.use_exact_where {
                        Some(
                            ctx.source_cache
                                .count_rows(&source.table, Some(&sql_filter))
                                .await,
                        )
                    } else {
                        self.estimate_filtered_rows(source, ctx).await
                    }
                }
                Err(e) => {
                    warn!(
                        target: "analyzer",
                        error = %e,
                        "failed to compile source filter; skipping filtered row estimate"
                    );
                    None
                }
            },
            None => None,
        };

        (total_rows, filtered_rows)
    }

    /// Estimate filtered row count using EXPLAIN (faster than exact COUNT)
    async fn estimate_filtered_rows<D: SchemaDriver>(
        &self,
        source: &DataSource,
        ctx: &AnalysisContext<S, D>,
    ) -> Option<RowCount> {
        let driver = DatabaseDriver::from_name(&source.connection.driver);
        let prefix = match driver {
            DatabaseDriver::Postgres => "EXPLAIN (FORMAT JSON) ",
            DatabaseDriver::MySql => "EXPLAIN FORMAT=JSON ",
            _ => return None,
        };

        // Build a simple SELECT query from the table to EXPLAIN
        let request = FetchRowsRequestBuilder::new(source.table.clone())
            .limit(1)
            .build();
        let dialect = ctx.source_dialect.as_query_dialect();
        let generator = QueryGenerator::new(dialect);
        let (sql, params) = generator.select(&request);

        let explain_sql = format!("{}{}", prefix, sql);
        let rows = ctx
            .src_driver
            .query_params(&explain_sql, &params)
            .await
            .map_err(|e| SourceAnalyzerError::QueryFailed(format!("EXPLAIN command failed: {}", e)))
            .ok()?;

        if rows.is_empty() {
            return None;
        }

        let json_key = match driver {
            DatabaseDriver::Postgres => "QUERY PLAN",
            DatabaseDriver::MySql => "EXPLAIN",
            _ => return None,
        };

        let json_str = rows[0].get_value(json_key).as_string().unwrap_or_default();
        let explain_json: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| SourceAnalyzerError::QueryFailed(format!("JSON parse error: {}", e)))
            .ok()?;

        let rows_examined = match driver {
            DatabaseDriver::Postgres => ExplainParser::extract_pg(&explain_json),
            DatabaseDriver::MySql => ExplainParser::extract_mysql(&explain_json, &source.table),
            _ => return None,
        };

        rows_examined?;

        Some(RowCount {
            value: rows_examined.unwrap() as u64,
            is_estimated: true,
            confidence: Some(0.7),
        })
    }

    /// Maps gathered metadata into the SourcePlan structure.
    fn assemble_source_plan(
        &self,
        source: &DataSource,
        metrics: SourceTableMetrics,
        driver: DatabaseDriver,
    ) -> SourcePlan {
        let column_infos = metrics
            .metadata
            .columns()
            .iter()
            .map(ColumnInfo::from_metadata)
            .collect();

        let index_infos = metrics
            .indexes
            .iter()
            .map(IndexInfo::from_metadata)
            .collect();

        SourcePlan {
            connection: source.connection.name.clone(),
            table: source.table.clone(),
            schema: None,
            fqn: source.table.clone(),
            driver,
            total_rows: metrics.total_rows,
            filtered_rows: metrics.filtered_rows,
            columns: column_infos,
            primary_key: metrics.metadata.primary_keys,
            indexes: index_infos,
            size_bytes: metrics.size_bytes,
            last_analyzed: Utc::now(),
        }
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for SourceAnalyzer<S> {
    type Input = DataSource;
    type Output = SourcePlan;

    fn name(&self) -> &'static str {
        "source"
    }

    async fn analyze(
        &self,
        source: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        self.analyze_source(source, ctx).await
    }
}
