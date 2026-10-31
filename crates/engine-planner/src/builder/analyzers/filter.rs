use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::FilterAnalyzerError,
        infra::metadata_cache::MetadataCache,
        utils::ColumnRefParser,
    },
    plan::{
        pipeline::source::SourcePlan,
        transform::filter::{FilterPlan, FilterSelectivity},
    },
};
use async_trait::async_trait;
use connectors::sql::{filter::compile::compile_filter, metadata::table::TableMetadata};
use engine_processing::io::{driver::SchemaDriver, filter::combine_filters};
use expression_engine::ExpressionAnalyzer;
use model::execution::pipeline::DataSource;
use std::{collections::HashSet, sync::Arc};
use tracing::info;

/// Analyzes filters to determine their impact on query performance
pub struct FilterAnalyzer<S: SchemaDriver> {
    _cache: Arc<MetadataCache<S>>,
}

impl<S: SchemaDriver> FilterAnalyzer<S> {
    pub fn new(cache: Arc<MetadataCache<S>>) -> Self {
        Self { _cache: cache }
    }

    /// Validates that all filter columns exist within the metadata graph (primary + joins).
    async fn validate_columns<D: SchemaDriver>(
        &self,
        columns: &[String],
        metadata: &TableMetadata,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<(), FilterAnalyzerError> {
        let meta_graph = ctx.schema_plan.metadata_graph();

        for col in columns {
            let col_ref = ColumnRefParser::parse(col, &metadata.name).map_err(|e| {
                FilterAnalyzerError::InvalidColumn {
                    column: format!("Parse error for '{}': {}", col, e),
                }
            })?;

            let table_meta = meta_graph.get(&col_ref.table).ok_or_else(|| {
                FilterAnalyzerError::InvalidColumn {
                    column: format!("Table '{}' not found in graph", col_ref.table),
                }
            })?;

            if !table_meta
                .columns()
                .iter()
                .any(|c| c.name == col_ref.column)
            {
                return Err(FilterAnalyzerError::InvalidColumn {
                    column: col.clone(),
                });
            }
        }
        Ok(())
    }

    /// Determines if an index exists that covers the filtered columns.
    fn check_index_usage(&self, source_plan: &SourcePlan, columns: &[String]) -> bool {
        if columns.is_empty() {
            return false;
        }

        let target_cols: HashSet<_> = columns
            .iter()
            .map(|c| c.split('.').next_back().unwrap_or(c).to_lowercase())
            .collect();

        source_plan.indexes.iter().any(|idx| {
            let idx_cols: HashSet<_> = idx.columns.iter().map(|c| c.to_lowercase()).collect();
            target_cols.iter().all(|tc| idx_cols.contains(tc))
        })
    }

    /// Calculate selectivity using pre-calculated filtered rows from SourcePlan.
    fn estimate_selectivity(&self, source_plan: &SourcePlan) -> FilterSelectivity {
        if let Some(filtered_rows) = &source_plan.filtered_rows {
            let total = source_plan.total_rows.value as f64;
            if total > 0.0 {
                let selectivity = (filtered_rows.value as f64 / total).clamp(0.0, 1.0);
                return FilterSelectivity {
                    selectivity: selectivity as f32,
                    is_estimated: filtered_rows.is_estimated,
                    confidence: filtered_rows.confidence,
                };
            }
        }

        FilterSelectivity {
            selectivity: 1.0,
            is_estimated: true,
            confidence: Some(0.0),
        }
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for FilterAnalyzer<S> {
    type Input = (DataSource, SourcePlan);
    type Output = Option<FilterPlan>;

    fn name(&self) -> &'static str {
        "filter"
    }

    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        let (source, source_plan) = input;

        let metadata = ctx
            .source_cache
            .table_metadata(&source.table)
            .await
            .map_err(|e| AnalyzerError::error("filter", format!("Metadata cache miss: {}", e)))?;

        let expr = match combine_filters(&source.filters) {
            Some(e) => e,
            None => return Ok(None),
        };

        let sql_filter =
            compile_filter(&expr).map_err(|e| AnalyzerError::error("filter", e.to_string()))?;
        let columns = sql_filter.columns();

        self.validate_columns(&columns, &metadata, ctx)
            .await
            .map_err(|e| AnalyzerError::error("filter", e.to_string()))?;

        let uses_index = self.check_index_usage(source_plan, &columns);
        let selectivity = self.estimate_selectivity(source_plan);

        info!(
            target: "analyzer",
            pipeline = %source_plan.table,
            columns = columns.len(),
            uses_index,
            "filter analysis completed"
        );

        let name = input
            .0
            .filters
            .first()
            .and_then(|f| f.label.clone())
            .unwrap_or_else(|| "WHERE".to_string());

        Ok(Some(FilterPlan {
            name,
            expression: ExpressionAnalyzer::to_string(&expr),
            sql_preview: sql_filter.to_sql(),
            selectivity,
            columns_referenced: columns,
            uses_index,
        }))
    }
}
