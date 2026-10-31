use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::ValidationAnalyzerError,
        utils::ColumnRefParser,
    },
    plan::validation::{
        plan::ValidationPlan,
        types::{ValidationAction, ValidationCheck, ValidationLevel},
    },
};
use async_trait::async_trait;
use connectors::{
    sql::{
        filter::compile::compile_filter, query::generator::QueryGenerator,
        request::FetchRowsRequestBuilder,
    },
    sql_filter_expr,
};
use engine_processing::io::{driver::SchemaDriver, linked::build_join_clauses};
use expression_engine::ExpressionAnalyzer;
use model::execution::{
    expr::CompiledExpression,
    pipeline::{
        Join, Pipeline, ValidationAction as ModelValidationAction, ValidationKind, ValidationRule,
        ValidationSeverity,
    },
};
use tracing::{error, info, warn};

/// Analyzes validation rules to verify column availability and estimate failure probability.
pub struct ValidationAnalyzer;

impl ValidationAnalyzer {
    pub fn no_source(&self, pipeline: &Pipeline) -> Vec<ValidationPlan> {
        pipeline
            .validations
            .iter()
            .map(|v| self.plan_without_source(v))
            .collect()
    }

    fn plan_without_source(&self, validation: &ValidationRule) -> ValidationPlan {
        let level = match validation.severity {
            ValidationSeverity::Assert => ValidationLevel::Assert,
            ValidationSeverity::Warn => ValidationLevel::Warn,
        };
        let action = self.validation_action(level.clone(), &validation.action);

        let check = match &validation.kind {
            ValidationKind::Assert { check } => ValidationCheck {
                expression: ExpressionAnalyzer::to_string(check),
                columns_referenced: ExpressionAnalyzer::extract_columns(check),
            },
            ValidationKind::WasmFilter {
                plugin_name,
                input_mapping,
            } => ValidationCheck {
                expression: format!("wasm:{plugin_name}"),
                columns_referenced: input_mapping.values().cloned().collect(),
            },
        };

        ValidationPlan {
            name: validation.label.clone(),
            level,
            check,
            message: validation.message.clone(),
            action,
            estimated_failure_rate: None,
        }
    }

    /// Primary orchestration logic for analyzing a single validation rule.
    async fn analyze_rule<S: SchemaDriver, D: SchemaDriver>(
        &self,
        table: &str,
        validation: &ValidationRule,
        joins: &[Join],
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<ValidationPlan> {
        let level = match validation.severity {
            ValidationSeverity::Assert => ValidationLevel::Assert,
            ValidationSeverity::Warn => ValidationLevel::Warn,
        };
        let action = self.validation_action(level.clone(), &validation.action);

        let (check, estimated_failure_rate) = match &validation.kind {
            ValidationKind::Assert { check } => {
                let columns = ExpressionAnalyzer::extract_columns(check);
                self.verify_column_refs(&columns, ctx).map_err(|e| {
                    AnalyzerError::error(
                        "validation",
                        format!("Column check failed for '{}': {}", validation.label, e),
                    )
                })?;

                let rate = self
                    .estimate_probability(table, check, joins, ctx)
                    .await
                    .ok();
                let check_view = ValidationCheck {
                    expression: ExpressionAnalyzer::to_string(check),
                    columns_referenced: columns,
                };
                (check_view, rate)
            }
            ValidationKind::WasmFilter {
                plugin_name,
                input_mapping,
            } => {
                // Plugin logic isn't introspectable from PPL, so we can't run a
                // SQL-side probability estimate. Surface the plugin descriptor so
                // sample previews and diagnostics can show the rule.
                let check_view = ValidationCheck {
                    expression: format!("wasm:{plugin_name}"),
                    columns_referenced: input_mapping.values().cloned().collect(),
                };
                (check_view, None)
            }
        };

        info!(
            target: "analyzer",
            label = %validation.label,
            level = ?level,
            failure_rate = ?estimated_failure_rate,
            "validation analysis completed"
        );

        Ok(ValidationPlan {
            name: validation.label.clone(),
            level,
            check,
            message: validation.message.clone(),
            action,
            estimated_failure_rate,
        })
    }

    /// Verifies that all columns in the validation exist within the source or joined tables.
    fn verify_column_refs<S: SchemaDriver, D: SchemaDriver>(
        &self,
        columns: &[String],
        ctx: &AnalysisContext<S, D>,
    ) -> Result<(), ValidationAnalyzerError> {
        let metadata_graph = ctx.schema_plan.metadata_graph();

        for col_ref in columns {
            let parsed = ColumnRefParser::parse(col_ref, "").map_err(|e| {
                ValidationAnalyzerError::ParseError(format!(
                    "Invalid reference '{}': {}",
                    col_ref, e
                ))
            })?;

            let exists = if !parsed.table.is_empty() {
                let physical_table = ctx.mapping.entities.reverse_resolve(&parsed.table);
                metadata_graph
                    .get(&physical_table)
                    .map(|meta| meta.columns().iter().any(|c| c.name == parsed.column))
                    .unwrap_or(false)
            } else {
                metadata_graph
                    .values()
                    .any(|meta| meta.columns().iter().any(|c| c.name == parsed.column))
            };

            if !exists {
                return Err(ValidationAnalyzerError::ColumnNotFound {
                    column: col_ref.clone(),
                });
            }
        }
        Ok(())
    }

    /// Runs a sampling query to estimate how many rows might fail this validation.
    async fn estimate_probability<S: SchemaDriver, D: SchemaDriver>(
        &self,
        table: &str,
        check: &CompiledExpression,
        joins: &[Join],
        ctx: &AnalysisContext<S, D>,
    ) -> Result<f32, ValidationAnalyzerError> {
        let physical_tables: Vec<String> = ExpressionAnalyzer::extract_tables(check)
            .iter()
            .map(|t| ctx.mapping.entities.reverse_resolve(t))
            .collect();

        let related_joins: Vec<_> = joins
            .iter()
            .filter(|j| physical_tables.contains(&j.table))
            .cloned()
            .collect();

        let join_clauses = if !related_joins.is_empty() {
            build_join_clauses(&related_joins)
        } else {
            Vec::new()
        };

        let sql_filter = compile_filter(check)
            .map_err(|e| ValidationAnalyzerError::ParseError(e.to_string()))?;
        let filter_expr = sql_filter
            .expr
            .as_ref()
            .and_then(|e| sql_filter_expr!(e).ok())
            .ok_or_else(|| {
                ValidationAnalyzerError::ParseError("Failed to compile check expression".into())
            })?;

        let request = FetchRowsRequestBuilder::new(table.to_string())
            .joins(join_clauses)
            .build();
        let dialect = ctx.source_dialect.as_query_dialect();
        let generator = QueryGenerator::new(dialect);
        let (sql, params) = generator.validation_estimation(&request, filter_expr, 10_000);

        let rows = ctx
            .src_driver
            .query_params(&sql, &params)
            .await
            .map_err(|e| {
                error!(target: "analyzer", error = %e, sql = %sql, "probability estimation failed");
                ValidationAnalyzerError::QueryFailed(e.to_string())
            })?;

        if let Some(row) = rows.first() {
            let failures = row.get_value("failures").as_f64().unwrap_or(0.0) as f32;
            let total = row.get_value("total").as_i32().unwrap_or(0) as f32;

            if total > 0.0 {
                return Ok((failures / total).clamp(0.0, 1.0));
            }
        }

        Ok(0.0)
    }

    fn validation_action(
        &self,
        level: ValidationLevel,
        model_action: &ModelValidationAction,
    ) -> Option<ValidationAction> {
        if matches!(level, ValidationLevel::Assert) {
            Some(match model_action {
                ModelValidationAction::Fail => ValidationAction::Fail,
                _ => ValidationAction::Skip,
            })
        } else {
            Some(ValidationAction::Skip)
        }
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for ValidationAnalyzer {
    type Input = Pipeline;
    type Output = Vec<ValidationPlan>;

    fn name(&self) -> &'static str {
        "validation"
    }

    async fn analyze(
        &self,
        pipeline: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        let table = &pipeline.source.table;
        let joins = &pipeline.source.joins;

        let mut plans = Vec::with_capacity(pipeline.validations.len());
        for validation in &pipeline.validations {
            match self.analyze_rule(table, validation, joins, ctx).await {
                Ok(plan) => plans.push(plan),
                Err(e) => {
                    warn!(target: "analyzer", label = %validation.label, error = %e, "skipping rule due to error");
                    return Err(e);
                }
            }
        }

        info!(target: "analyzer", count = plans.len(), "validation analysis completed");
        Ok(plans)
    }
}
