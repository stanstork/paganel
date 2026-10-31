use crate::context::env::EnvContext;
use connectors::drivers::postgres::config::{CopyFormat, PgConflictAction, PkCreation};
use model::{
    core::value::Value,
    execution::{
        config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
        connection::Connection,
        define::DefinitionInfo,
        errors::ConvertError,
        expr::{BinaryOp, CompiledExpression, UnaryOp, WhenBranch},
        pipeline::{
            BackoffStrategy, DataDestination, DataSource, ErrorHandling, FailedRowsConfig,
            FailedRowsDestination, FileFormat, Filter, Join, LifecycleHooks, Pagination, Pipeline,
            PluginTransformCall, RetryConfig, Transformation, ValidationAction,
            ValidationKind as RuleKind, ValidationRule, ValidationSeverity, WriteMode,
        },
        plugin::PluginDecl,
        properties::Properties,
        references::{DataMode, GraphReferences, TraversalDepth},
        tuning::{
            COPY_FORMAT as TUNING_COPY_FORMAT, ON_CONFLICT as TUNING_ON_CONFLICT,
            PK_CREATION as TUNING_PK_CREATION,
        },
    },
};
use ppl_syntax::ast::{
    attribute::Attribute,
    block::{ConnectionBlock, DefineBlock, ExecutionBlock, PluginBlock},
    expr::{Expression, ExpressionKind},
    ident::Identifier,
    literal::Literal,
    operator::{BinaryOperator, UnaryOperator},
    pipeline::{FromBlock, NestedBlock, PipelineBlock, SelectBlock, ToBlock},
    span::Span,
    validation::ValidationKind,
};
use query_builder::ast::load_data::LoadDataConflict;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

// ============================================================
// Attribute Name Constants
// ============================================================

// Connection attributes
const ATTR_DRIVER: &str = "driver";

// Execution config attributes
const ATTR_STRATEGY: &str = "strategy";
const ATTR_MAX_CONCURRENCY: &str = "max_concurrency";
const ATTR_ON_FAILURE: &str = "on_failure";
const ATTR_PIPELINE_TIMEOUT: &str = "pipeline_timeout";
const ATTR_TOTAL_TIMEOUT: &str = "total_timeout";

// Pipeline attributes
const ATTR_CONNECTION: &str = "connection";
const ATTR_TABLE: &str = "table";
const ATTR_TABLES: &str = "tables";
const ATTR_MODE: &str = "mode";
const ATTR_STRATEGY_PAGINATION: &str = "strategy";
const ATTR_CURSOR: &str = "cursor";
const ATTR_TIEBREAKER: &str = "tiebreaker";
const ATTR_TIMEZONE: &str = "timezone";
const ATTR_MAX_ATTEMPTS: &str = "max_attempts";
const ATTR_DELAY_MS: &str = "delay_ms";
const ATTR_PATH: &str = "path";
const ATTR_FORMAT: &str = "format";
const ATTR_SCHEMA: &str = "schema";

// References block attributes
const ATTR_DATA: &str = "data";
const ATTR_DEPTH: &str = "depth";
const ATTR_EXCLUDE: &str = "exclude";
const ATTR_DROP_CONSTRAINTS: &str = "drop_constraints";

// References data modes
const DATA_MODE_CASCADE: &str = "cascade";
const DATA_MODE_SCHEMA_ONLY: &str = "schema_only";

// Depth values
const DEPTH_ALL: &str = "all";

// Nested block names
const BLOCK_TABLE: &str = "table";
const BLOCK_FILE: &str = "file";
const BLOCK_PIPELINE: &str = "pipeline";

// Keywords
const KEYWORD_CONNECTION: &str = "connection";
const KEYWORD_DEFINE: &str = "define";

// Default values
const DEFAULT_PAGINATION_STRATEGY: &str = "default";
const DEFAULT_CURSOR: &str = "id";

// Write modes
const MODE_INSERT: &str = "insert";
const MODE_APPEND: &str = "append";
const MODE_REPLACE: &str = "replace";

// Validation actions
const ACTION_SKIP: &str = "skip";
const ACTION_FAIL: &str = "fail";
const ACTION_WARN: &str = "warn";
const ACTION_CONTINUE: &str = "continue";

// File formats
const FORMAT_JSON: &str = "json";
const FORMAT_CSV: &str = "csv";
const FORMAT_PARQUET: &str = "parquet";

// Error messages
const ERR_MISSING_DRIVER: &str = "Connection missing driver";
const ERR_MISSING_FROM: &str = "Pipeline missing 'from' block";
const ERR_MISSING_TO: &str = "Pipeline missing 'to' block";
const ERR_MISSING_TABLE: &str = "Missing 'table' attribute";
const ERR_INVALID_STRATEGY: &str =
    "Invalid execution strategy: '{}'. Must be 'sequential' or 'parallel'";
const ERR_STRATEGY_NOT_STRING: &str = "strategy must be a string";
const ERR_MAX_CONCURRENCY_NOT_NUMBER: &str = "max_concurrency must be a number";
const ERR_MAX_CONCURRENCY_RANGE: &str = "max_concurrency must be between 1 and 100";
const ERR_MAX_CONCURRENCY_REQUIRED: &str =
    "max_concurrency is required when strategy is 'parallel'";
const ERR_INVALID_FAILURE_STRATEGY: &str =
    "Invalid failure strategy: '{}'. Must be 'fail_fast' or 'continue'";
const ERR_ON_FAILURE_NOT_STRING: &str = "on_failure must be a string";
const ERR_TIMEOUT_NOT_STRING: &str = "{} must be a string (e.g., '30s', '5m', '2h')";
const ERR_MISSING_CONNECTION: &str = "From block missing connection attribute";
const ERR_MISSING_TO_CONNECTION: &str = "To block missing connection attribute";
const ERR_INVALID_PIPELINE_DEPENDENCY: &str =
    "Invalid pipeline dependency reference: {}. Expected format: pipeline.name";
const ERR_PIPELINE_DEPS_MUST_BE_STRINGS: &str =
    "Pipeline dependencies must be string literals or pipeline references";

// Validation constants
const MAX_CONCURRENCY_MIN: u32 = 1;
const MAX_CONCURRENCY_MAX: u32 = 100;

// Dialect tuning: the `to/from { <dialect> { … } }` block names.
const DIALECT_POSTGRES: &str = "postgres";
const DIALECT_MYSQL: &str = "mysql";
const KNOWN_DIALECTS: &[&str] = &[DIALECT_POSTGRES, DIALECT_MYSQL];

/// Convert validated AST to execution plan
pub struct PlanBuilder {
    // For resolving references
    pub global_definitions: HashMap<String, DefinitionInfo>,
    pub connections: HashMap<String, Connection>,
    pub env: Arc<EnvContext>,
}

impl PlanBuilder {
    pub fn new(env: Arc<EnvContext>) -> Self {
        Self {
            global_definitions: HashMap::new(),
            connections: HashMap::new(),
            env,
        }
    }

    pub fn build_connection(
        &self,
        conn_block: &ConnectionBlock,
    ) -> Result<Connection, ConvertError> {
        let mut properties = Properties::new();
        let mut nested_configs = HashMap::new();

        for attr in &conn_block.attributes {
            let value = self.eval_with_definitions(&attr.value)?;
            properties.insert(attr.key.name.clone(), value);
        }

        for nested in &conn_block.nested_blocks {
            let mut nested_props = HashMap::new();
            for attr in &nested.attributes {
                let value = self.eval_with_definitions(&attr.value)?;
                nested_props.insert(attr.key.name.clone(), value);
            }
            nested_configs.insert(nested.kind.clone(), nested_props);
        }

        Ok(Connection {
            name: conn_block.name.clone(),
            driver: properties
                .get_string(ATTR_DRIVER)
                .ok_or_else(|| ConvertError::Connection(ERR_MISSING_DRIVER.to_string()))?,
            properties,
            nested_configs,
        })
    }

    pub fn build_execution_config(
        &self,
        exec_block: &ExecutionBlock,
    ) -> Result<ExecutionConfig, ConvertError> {
        let mut strategy = ExecutionStrategy::Sequential;
        let mut max_concurrency = None;
        let mut on_failure = FailureStrategy::FailFast;
        let mut pipeline_timeout = None;
        let mut total_timeout = None;

        for attr in &exec_block.attributes {
            let value = self.eval_with_definitions(&attr.value)?;

            match attr.key.name.as_str() {
                ATTR_STRATEGY => {
                    if let Value::String(s) = value {
                        strategy = ExecutionStrategy::from_str(&s).map_err(|_| {
                            ConvertError::Plan(ERR_INVALID_STRATEGY.replace("{}", &s))
                        })?;
                    } else {
                        return Err(ConvertError::Plan(ERR_STRATEGY_NOT_STRING.to_string()));
                    }
                }
                ATTR_MAX_CONCURRENCY => {
                    let concurrency = match value {
                        Value::Int(n) => n as u32,
                        Value::UInt(n) => n as u32,
                        Value::Float(f) => f as u32,
                        _ => {
                            return Err(ConvertError::Plan(
                                ERR_MAX_CONCURRENCY_NOT_NUMBER.to_string(),
                            ));
                        }
                    };

                    if !(MAX_CONCURRENCY_MIN..=MAX_CONCURRENCY_MAX).contains(&concurrency) {
                        return Err(ConvertError::Plan(ERR_MAX_CONCURRENCY_RANGE.to_string()));
                    }
                    max_concurrency = Some(concurrency);
                }
                ATTR_ON_FAILURE => {
                    if let Value::String(s) = value {
                        on_failure = FailureStrategy::from_str(&s).map_err(|_| {
                            ConvertError::Plan(ERR_INVALID_FAILURE_STRATEGY.replace("{}", &s))
                        })?;
                    } else {
                        return Err(ConvertError::Plan(ERR_ON_FAILURE_NOT_STRING.to_string()));
                    }
                }
                ATTR_PIPELINE_TIMEOUT => {
                    if let Value::String(s) = value {
                        pipeline_timeout = Some(parse_duration(&s)?);
                    } else {
                        return Err(ConvertError::Plan(
                            ERR_TIMEOUT_NOT_STRING.replace("{}", ATTR_PIPELINE_TIMEOUT),
                        ));
                    }
                }
                ATTR_TOTAL_TIMEOUT => {
                    if let Value::String(s) = value {
                        total_timeout = Some(parse_duration(&s)?);
                    } else {
                        return Err(ConvertError::Plan(
                            ERR_TIMEOUT_NOT_STRING.replace("{}", ATTR_TOTAL_TIMEOUT),
                        ));
                    }
                }
                _ => {
                    // Ignore unknown attributes for forward compatibility
                }
            }
        }

        // Validate that parallel strategy has max_concurrency
        if strategy == ExecutionStrategy::Parallel && max_concurrency.is_none() {
            return Err(ConvertError::Plan(ERR_MAX_CONCURRENCY_REQUIRED.to_string()));
        }

        Ok(ExecutionConfig {
            strategy,
            max_concurrency,
            on_failure,
            pipeline_timeout,
            total_timeout,
        })
    }

    /// Desugar a pipeline that lists several tables (`from { tables = [...] }`)
    /// into one full-copy pipeline per table.
    pub fn expand_pipeline(
        &self,
        block: &PipelineBlock,
    ) -> Result<Vec<PipelineBlock>, ConvertError> {
        let Some(from) = &block.from else {
            return Ok(vec![block.clone()]);
        };

        let Some(tables_attr) = from.attributes.iter().find(|a| a.key.name == ATTR_TABLES) else {
            return Ok(vec![block.clone()]);
        };

        let tables = self.read_table_list(&block.name, tables_attr)?;
        Self::validate_multi_table_guardrails(block, from)?;

        // Lowercased names for case-insensitive membership checks.
        let table_set: HashSet<String> = tables.iter().map(|t| t.to_ascii_lowercase()).collect();

        Self::validate_multi_table_named_selects(block, &table_set)?;
        let rename = self.multi_table_renames(block, &table_set)?;

        Ok(tables
            .iter()
            .map(|table| Self::expand_one_table(block, table, &rename))
            .collect())
    }

    pub fn build_pipeline(&self, pipeline_block: &PipelineBlock) -> Result<Pipeline, ConvertError> {
        let source = self.build_source(pipeline_block)?;
        let destination = self.build_destination(pipeline_block)?;
        let dependencies = self.build_dependencies(pipeline_block)?;
        let transformations = self.build_transformations(pipeline_block)?;
        let named_transformations = self.build_named_transformations(pipeline_block)?;
        let validation_rules = self.build_validation_rules(pipeline_block)?;
        let error_handling = self.build_error_handling(pipeline_block)?;
        let lifecycle = self.build_lifecycle(pipeline_block)?;
        let settings = self.build_settings(pipeline_block)?;
        let plugin_transforms = self.build_plugin_transforms(pipeline_block);

        Ok(Pipeline {
            name: pipeline_block.name.clone(),
            description: pipeline_block.description.clone(),
            dependencies,
            source,
            destination,
            transformations,
            named_transformations,
            validations: validation_rules,
            lifecycle: Some(lifecycle),
            error_handling: Some(error_handling),
            settings,
            plugin_transforms,
        })
    }

    fn build_source(&self, pipeline_block: &PipelineBlock) -> Result<DataSource, ConvertError> {
        let from = pipeline_block
            .from
            .as_ref()
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_FROM.to_string()))?;

        let connection = self.resolve_connection_from(from)?;

        let mut filters = Vec::new();
        for wc in &pipeline_block.where_clauses {
            for expr in &wc.conditions {
                filters.push(Filter {
                    label: wc.label.clone(),
                    condition: self.compile_expression(expr)?,
                });
            }
        }

        let mut joins = Vec::new();
        if let Some(with_block) = &pipeline_block.with_block {
            for j in &with_block.joins {
                let condition = match &j.condition {
                    Some(expr) => Some(self.compile_expression(expr)?),
                    None => None,
                };
                joins.push(Join {
                    alias: j.alias.name.clone(),
                    table: j.table.name.clone(),
                    condition,
                });
            }
        }

        let pagination = pipeline_block.paginate_block.as_ref().map(|p| {
            let strategy = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_STRATEGY_PAGINATION)
                .and_then(|a| self.eval_with_definitions(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| DEFAULT_PAGINATION_STRATEGY.to_string());

            let cursor = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_CURSOR)
                .and_then(|a| self.eval_with_definitions(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| DEFAULT_CURSOR.to_string());

            let tiebreaker = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_TIEBREAKER)
                .and_then(|a| self.eval_with_definitions(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            let timezone = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_TIMEZONE)
                .and_then(|a| self.eval_with_definitions(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            Pagination {
                strategy,
                column: cursor,
                tiebreaker,
                timezone,
            }
        });

        // Extract table name
        let table = from
            .attributes
            .iter()
            .find(|a| a.key.name == ATTR_TABLE)
            .and_then(|a| self.eval_with_definitions(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_TABLE.to_string()))?;

        let graph_references = self.build_graph_references(from)?;
        let conn = self.connections.get(&connection).cloned().ok_or_else(|| {
            ConvertError::Connection(format!("Connection `{}` not found", connection))
        })?;
        let tuning =
            self.build_endpoint_tuning(&from.nested_blocks, &conn.driver, "`from` block")?;

        Ok(DataSource {
            connection: conn,
            table,
            filters,
            joins,
            pagination,
            graph_references,
            tuning,
        })
    }

    /// Extract driver-specific tuning from a `from`/`to` endpoint's dialect
    /// blocks (e.g. `postgres { pk_creation = "post" }`).
    fn build_endpoint_tuning(
        &self,
        nested_blocks: &[NestedBlock],
        driver: &str,
        endpoint: &str,
    ) -> Result<HashMap<String, Value>, ConvertError> {
        let mut tuning = HashMap::new();
        let mut seen_dialect = false;

        let allowed = Self::allowed_tuning_keys(driver);

        for block in nested_blocks {
            if !KNOWN_DIALECTS.contains(&block.kind.as_str()) {
                continue; // not a dialect block - leave for other consumers
            }

            if block.kind != driver {
                return Err(ConvertError::Plan(format!(
                    "`{}` tuning block in a `{driver}` {endpoint} - a dialect block must match the endpoint's driver",
                    block.kind
                )));
            }

            if seen_dialect {
                return Err(ConvertError::Plan(format!(
                    "duplicate `{driver}` tuning block in {endpoint}"
                )));
            }
            seen_dialect = true;

            for attr in &block.attributes {
                let key = attr.key.name.clone();

                if !allowed.contains(&key.as_str()) {
                    return Err(ConvertError::Plan(format!(
                        "unknown `{driver}` tuning key `{key}` in {endpoint}; supported: {allowed:?}"
                    )));
                }

                let value = self.eval_with_definitions(&attr.value)?;
                Self::validate_tuning_value(&key, &value, driver, endpoint)?;

                tuning.insert(key, value);
            }
        }
        Ok(tuning)
    }

    fn build_destination(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<DataDestination, ConvertError> {
        let to = pipeline_block
            .to
            .as_ref()
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_TO.to_string()))?;

        let connection = self.resolve_connection_to(to)?;

        // Extract table name (optional - omitted in schema_only / graph-reference pipelines)
        let table = to
            .attributes
            .iter()
            .find(|a| a.key.name == ATTR_TABLE)
            .and_then(|a| self.eval_with_definitions(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .unwrap_or_default();

        let mode = match to.attributes.iter().find(|a| a.key.name == ATTR_MODE) {
            None => WriteMode::Insert,
            Some(attr) => match self.eval_with_definitions(&attr.value)? {
                Value::String(s) => match s.as_str() {
                    MODE_INSERT | MODE_APPEND => WriteMode::Insert,
                    MODE_REPLACE => WriteMode::Replace,
                    other => {
                        return Err(ConvertError::Plan(format!(
                            "unknown write mode '{other}'; expected 'append' or 'replace'"
                        )));
                    }
                },
                _ => return Err(ConvertError::Plan("mode must be a string".to_string())),
            },
        };

        let table_map = self.build_table_map(to)?;

        let conn = self.connections.get(&connection).cloned().ok_or_else(|| {
            ConvertError::Connection(format!("Connection `{}` not found", connection))
        })?;
        let tuning = self.build_endpoint_tuning(&to.nested_blocks, &conn.driver, "`to` block")?;

        Ok(DataDestination {
            connection: conn,
            table,
            mode,
            table_map,
            tuning,
        })
    }

    fn build_dependencies(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<String>, ConvertError> {
        // Extract dependencies from the 'after' field which contains pipeline names
        // The 'after' field is Vec<Expression>, where each expression is typically an array
        if let Some(after) = &pipeline_block.after {
            let mut deps = Vec::new();
            for expr in after {
                match &expr.kind {
                    // Handle array of dependencies: after = [pipeline.name1, pipeline.name2]
                    ExpressionKind::Array(items) => {
                        for item in items {
                            match &item.kind {
                                // String literal in array: after = ["pipeline1"]
                                ExpressionKind::Literal(Literal::String(s)) => {
                                    deps.push(s.clone());
                                }
                                // Dot notation in array: after = [pipeline.copy_actors]
                                ExpressionKind::DotNotation(dot_path) => {
                                    if dot_path.segments.len() == 2
                                        && dot_path.segments[0] == BLOCK_PIPELINE
                                    {
                                        deps.push(dot_path.segments[1].clone());
                                    } else {
                                        return Err(ConvertError::Plan(
                                            ERR_INVALID_PIPELINE_DEPENDENCY
                                                .replace("{}", &dot_path.segments.join(".")),
                                        ));
                                    }
                                }
                                _ => {
                                    return Err(ConvertError::Plan(
                                        ERR_PIPELINE_DEPS_MUST_BE_STRINGS.to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    // Handle single string literal: after = "pipeline1" (legacy/edge case)
                    ExpressionKind::Literal(Literal::String(s)) => {
                        deps.push(s.clone());
                    }
                    // Handle single dot notation: after = pipeline.name (legacy/edge case)
                    ExpressionKind::DotNotation(dot_path) => {
                        if dot_path.segments.len() == 2 && dot_path.segments[0] == BLOCK_PIPELINE {
                            deps.push(dot_path.segments[1].clone());
                        } else {
                            return Err(ConvertError::Plan(
                                ERR_INVALID_PIPELINE_DEPENDENCY
                                    .replace("{}", &dot_path.segments.join(".")),
                            ));
                        }
                    }
                    _ => {
                        return Err(ConvertError::Plan(
                            ERR_PIPELINE_DEPS_MUST_BE_STRINGS.to_string(),
                        ));
                    }
                }
            }
            Ok(deps)
        } else {
            Ok(Vec::new())
        }
    }

    fn build_transformations(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<Transformation>, ConvertError> {
        if let Some(select) = &pipeline_block.select_block {
            select
                .fields
                .iter()
                // Plugin calls are routed to `plugin_transforms`, not regular
                // expression transformations.
                .filter(|f| !matches!(f.value.kind, ExpressionKind::PluginCall(_)))
                .map(|f| {
                    Ok(Transformation {
                        target_field: f.name.name.clone(),
                        expression: self.compile_expression(&f.value)?,
                    })
                })
                .collect()
        } else {
            Ok(Vec::new())
        }
    }

    /// Extract `output = plugin.name({ field: src.col, ... })` entries from the
    /// select block into `PluginTransformCall`s.
    fn build_plugin_transforms(&self, pipeline_block: &PipelineBlock) -> Vec<PluginTransformCall> {
        let Some(select) = &pipeline_block.select_block else {
            return Vec::new();
        };
        select
            .fields
            .iter()
            .filter_map(|f| match &f.value.kind {
                ExpressionKind::PluginCall(call) => Some(PluginTransformCall {
                    plugin_name: call.plugin_name.clone(),
                    output_column: f.name.name.clone(),
                    input_mapping: Self::plugin_input_mapping(call),
                }),
                _ => None,
            })
            .collect()
    }

    /// Map a plugin call's input fields to `plugin_field -> source column name`.
    /// The source column is the last segment of the dotted reference (e.g.
    /// `orders.amount` -> `amount`), matching how records are keyed.
    fn plugin_input_mapping(call: &ppl_syntax::ast::expr::PluginCall) -> HashMap<String, String> {
        call.inputs
            .iter()
            .map(|input| {
                let column = input
                    .source_ref
                    .segments
                    .last()
                    .cloned()
                    .unwrap_or_default();
                (input.plugin_field.clone(), column)
            })
            .collect()
    }

    fn build_named_transformations(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<HashMap<String, Vec<Transformation>>, ConvertError> {
        let mut result = HashMap::new();
        for named in &pipeline_block.named_select_blocks {
            let transforms = named
                .fields
                .iter()
                .filter(|f| !matches!(f.value.kind, ExpressionKind::PluginCall(_)))
                .map(|f| {
                    Ok(Transformation {
                        target_field: f.name.name.clone(),
                        expression: self.compile_expression(&f.value)?,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?;
            result.insert(named.table.to_ascii_lowercase(), transforms);
        }
        Ok(result)
    }

    fn build_validation_rules(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<ValidationRule>, ConvertError> {
        let Some(validate) = &pipeline_block.validate_block else {
            return Ok(Vec::new());
        };

        validate
            .checks
            .iter()
            .map(|check| {
                let action = check
                    .body
                    .action
                    .as_ref()
                    .and_then(|a| Self::parse_validation_action(a))
                    .unwrap_or(ValidationAction::Warn);

                // A plugin call as the predicate routes to the WASM filter path;
                // any other expression is compiled for the expression engine.
                let (kind, message) = match &check.body.check.kind {
                    ExpressionKind::PluginCall(call) => {
                        let message = if check.body.message.is_empty() {
                            format!("row rejected by filter plugin '{}'", call.plugin_name)
                        } else {
                            check.body.message.clone()
                        };
                        (
                            RuleKind::WasmFilter {
                                plugin_name: call.plugin_name.clone(),
                                input_mapping: Self::plugin_input_mapping(call),
                            },
                            message,
                        )
                    }
                    _ => (
                        RuleKind::Assert {
                            check: self.compile_expression(&check.body.check)?,
                        },
                        check.body.message.clone(),
                    ),
                };

                Ok(ValidationRule {
                    label: check.label.clone(),
                    severity: match check.kind {
                        ValidationKind::Assert => ValidationSeverity::Assert,
                        ValidationKind::Warn => ValidationSeverity::Warn,
                    },
                    kind,
                    message,
                    action,
                })
            })
            .collect()
    }

    fn parse_validation_action(a: &str) -> Option<ValidationAction> {
        match a {
            ACTION_SKIP => Some(ValidationAction::Skip),
            ACTION_FAIL => Some(ValidationAction::Fail),
            ACTION_WARN => Some(ValidationAction::Warn),
            ACTION_CONTINUE => Some(ValidationAction::Continue),
            _ => None,
        }
    }

    fn build_error_handling(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<ErrorHandling, ConvertError> {
        if let Some(on_error) = &pipeline_block.on_error_block {
            let retry = on_error.retry.as_ref().map(|r| {
                // Extract retry config from attributes
                let max_attempts = r
                    .attributes
                    .iter()
                    .find(|a| a.key.name == ATTR_MAX_ATTEMPTS)
                    .and_then(|a| self.eval_with_definitions(&a.value).ok())
                    .and_then(|v| match v {
                        Value::Int(n) => Some(n as u32),
                        Value::Float(f) => Some(f as u32),
                        _ => None,
                    })
                    .unwrap_or(3);

                // Base retry delay in milliseconds (optional, defaults to 1000).
                let delay_ms = r
                    .attributes
                    .iter()
                    .find(|a| a.key.name == ATTR_DELAY_MS)
                    .and_then(|a| self.eval_with_definitions(&a.value).ok())
                    .and_then(|v| match v {
                        Value::Int(n) if n >= 0 => Some(n as u64),
                        Value::Float(f) if f >= 0.0 => Some(f as u64),
                        _ => None,
                    })
                    .unwrap_or(1000);

                RetryConfig {
                    max_attempts,
                    delay_ms,
                    // Backoff strategy is not yet configurable from PPL; the
                    // per-attempt delay grows exponentially from `delay_ms`.
                    backoff: BackoffStrategy::Exponential,
                }
            });

            let failed_rows = on_error
                .failed_rows
                .as_ref()
                .map(|fr| -> Result<FailedRowsConfig, ConvertError> {
                    // Extract destination from nested blocks or attributes
                    let destination = if let Some(table_block) =
                        fr.nested_blocks.iter().find(|b| b.kind == BLOCK_TABLE)
                    {
                        // Parse table block
                        let connection_name = table_block
                            .attributes
                            .iter()
                            .find(|a| a.key.name == ATTR_CONNECTION)
                            .and_then(|a| {
                                if let ExpressionKind::DotNotation(path) = &a.value.kind {
                                    // connection.name format
                                    if path.segments.len() == 2
                                        && path.segments[0] == KEYWORD_CONNECTION
                                    {
                                        Some(path.segments[1].clone())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            });

                        let schema = table_block
                            .attributes
                            .iter()
                            .find(|a| a.key.name == ATTR_SCHEMA)
                            .and_then(|a| self.eval_with_definitions(&a.value).ok())
                            .and_then(|v| match v {
                                Value::String(s) => Some(s),
                                _ => None,
                            });

                        let table = table_block
                            .attributes
                            .iter()
                            .find(|a| a.key.name == ATTR_TABLE)
                            .and_then(|a| self.eval_with_definitions(&a.value).ok())
                            .and_then(|v| match v {
                                Value::String(s) => Some(s),
                                _ => None,
                            });

                        if let (Some(conn_name), Some(tbl)) = (connection_name, table) {
                            // Look up the connection from the connections map
                            self.connections.get(&conn_name).map(|connection| {
                                FailedRowsDestination::Table {
                                    connection: connection.clone(),
                                    table: tbl,
                                    schema,
                                }
                            })
                        } else {
                            None
                        }
                    } else if let Some(file_block) =
                        fr.nested_blocks.iter().find(|b| b.kind == BLOCK_FILE)
                    {
                        // Parse file block
                        let path = file_block
                            .attributes
                            .iter()
                            .find(|a| a.key.name == ATTR_PATH)
                            .and_then(|a| self.eval_with_definitions(&a.value).ok())
                            .and_then(|v| match v {
                                Value::String(s) => Some(s),
                                _ => None,
                            });

                        let explicit_format = file_block
                            .attributes
                            .iter()
                            .find(|a| a.key.name == ATTR_FORMAT)
                            .and_then(|a| self.eval_with_definitions(&a.value).ok())
                            .and_then(|v| match v {
                                Value::String(s) => match s.as_str() {
                                    FORMAT_JSON => Some(FileFormat::Json),
                                    FORMAT_CSV => Some(FileFormat::Csv),
                                    FORMAT_PARQUET => Some(FileFormat::Parquet),
                                    _ => None,
                                },
                                _ => None,
                            });

                        match path {
                            Some(p) => {
                                // Resolve format: explicit wins, else infer from extension.
                                let format = explicit_format.unwrap_or_else(|| {
                                    let ext_csv = format!(".{}", FORMAT_CSV);
                                    let ext_parquet = format!(".{}", FORMAT_PARQUET);
                                    if p.ends_with(&ext_csv) {
                                        FileFormat::Csv
                                    } else if p.ends_with(&ext_parquet) {
                                        FileFormat::Parquet
                                    } else {
                                        FileFormat::Json
                                    }
                                });

                                // Only JSON dead-letter files are supported today;
                                // reject others at plan time instead of failing mid-run.
                                if !matches!(format, FileFormat::Json) {
                                    let name = match format {
                                        FileFormat::Csv => FORMAT_CSV,
                                        FileFormat::Parquet => FORMAT_PARQUET,
                                        FileFormat::Json => FORMAT_JSON,
                                    };
                                    return Err(ConvertError::Plan(format!(
                                        "dead-letter file format '{name}' is not supported yet; \
                                     only 'json' is available"
                                    )));
                                }

                                Some(FailedRowsDestination::File { path: p, format })
                            }
                            None => None,
                        }
                    } else {
                        None
                    };

                    Ok(FailedRowsConfig { destination })
                })
                .transpose()?;

            Ok(ErrorHandling { retry, failed_rows })
        } else {
            Ok(ErrorHandling {
                retry: None,
                failed_rows: None,
            })
        }
    }

    fn build_lifecycle(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<LifecycleHooks, ConvertError> {
        let before = pipeline_block
            .before_block
            .as_ref()
            .map(|b| b.sql.clone())
            .unwrap_or_default();

        let after = pipeline_block
            .after_block
            .as_ref()
            .map(|b| b.sql.clone())
            .unwrap_or_default();

        Ok(LifecycleHooks { before, after })
    }

    fn build_settings(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<HashMap<String, Value>, ConvertError> {
        if let Some(settings) = &pipeline_block.settings_block {
            let mut settings_map = HashMap::new();
            for attr in &settings.attributes {
                let value = self.eval_with_definitions(&attr.value)?;
                settings_map.insert(attr.key.name.clone(), value);
            }
            Ok(settings_map)
        } else {
            Ok(HashMap::new())
        }
    }

    fn build_graph_references(
        &self,
        from: &FromBlock,
    ) -> Result<Option<GraphReferences>, ConvertError> {
        let refs_block = match &from.references {
            Some(rb) => rb,
            None => return Ok(None),
        };

        let mut data_mode = DataMode::default();
        let mut depth = TraversalDepth::default();
        let mut exclude = Vec::new();
        let mut drop_constraints = false;

        for attr in &refs_block.attributes {
            // Bare identifiers (schema_only, all) are treated as string enum values
            let value = match &attr.value.kind {
                ExpressionKind::Identifier(name) => Value::String(name.clone()),
                _ => self.eval_with_definitions(&attr.value)?,
            };
            match attr.key.name.as_str() {
                ATTR_DATA => {
                    if let Value::String(s) = value {
                        data_mode = match s.as_str() {
                            DATA_MODE_CASCADE => DataMode::Cascade,
                            DATA_MODE_SCHEMA_ONLY => DataMode::SchemaOnly,
                            _ => {
                                return Err(ConvertError::Plan(format!(
                                    "Invalid data mode: '{}'. Must be 'cascade' or 'schema_only'",
                                    s
                                )));
                            }
                        };
                    }
                }
                ATTR_DEPTH => match value {
                    Value::String(s) if s == DEPTH_ALL => {
                        depth = TraversalDepth::All;
                    }
                    Value::Int(n) if n > 0 => {
                        depth = TraversalDepth::Limited(n as usize);
                    }
                    Value::UInt(n) if n > 0 => {
                        depth = TraversalDepth::Limited(n as usize);
                    }
                    Value::Float(f) if f > 0.0 => {
                        depth = TraversalDepth::Limited(f as usize);
                    }
                    _ => {
                        return Err(ConvertError::Plan(
                            "depth must be 'all' or a positive integer".to_string(),
                        ));
                    }
                },
                ATTR_EXCLUDE => {
                    if let Value::Array(items) = value {
                        for item in items {
                            if let Value::String(s) = item {
                                exclude.push(s);
                            }
                        }
                    }
                }
                ATTR_DROP_CONSTRAINTS => {
                    if let Value::Boolean(b) = value {
                        drop_constraints = b;
                    }
                }
                _ => {}
            }
        }

        Ok(Some(GraphReferences {
            data_mode,
            depth,
            exclude,
            drop_constraints,
        }))
    }

    fn build_table_map(&self, to: &ToBlock) -> Result<HashMap<String, String>, ConvertError> {
        let map_block = match &to.map_block {
            Some(mb) => mb,
            None => return Ok(HashMap::new()),
        };

        let mut table_map = HashMap::new();
        for mapping in &map_block.mappings {
            let target_value = self.eval_with_definitions(&mapping.value)?;
            if let Value::String(s) = target_value {
                table_map.insert(mapping.name.name.clone(), s);
            } else {
                return Err(ConvertError::Plan(format!(
                    "Table mapping value for '{}' must be a string",
                    mapping.name.name
                )));
            }
        }

        Ok(table_map)
    }

    fn resolve_connection_from(&self, from: &FromBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &from.attributes {
            if attr.key.name == ATTR_CONNECTION
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == KEYWORD_CONNECTION
            {
                return Ok(path.segments[1].clone());
            }
        }
        Err(ConvertError::Plan(ERR_MISSING_CONNECTION.to_string()))
    }

    fn resolve_connection_to(&self, to: &ToBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &to.attributes {
            if attr.key.name == ATTR_CONNECTION
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == KEYWORD_CONNECTION
            {
                return Ok(path.segments[1].clone());
            }
        }
        Err(ConvertError::Plan(ERR_MISSING_TO_CONNECTION.to_string()))
    }

    pub(crate) fn compile_expression(
        &self,
        expr: &Expression,
    ) -> Result<CompiledExpression, ConvertError> {
        match &expr.kind {
            ExpressionKind::Literal(lit) => match lit {
                Literal::String(s) => Ok(CompiledExpression::Literal(Value::String(s.clone()))),
                Literal::Int(i) => Ok(CompiledExpression::Literal(Value::Int(*i))),
                Literal::Number(n) => Ok(CompiledExpression::Literal(Value::Float(*n))),
                Literal::Boolean(b) => Ok(CompiledExpression::Literal(Value::Boolean(*b))),
                Literal::Null => Ok(CompiledExpression::Literal(Value::Null)),
            },
            ExpressionKind::Identifier(id) => Ok(CompiledExpression::Identifier(id.clone())),
            ExpressionKind::DotNotation(path) => {
                // Resolve define.X references
                if path.segments[0] == KEYWORD_DEFINE
                    && path.segments.len() == 2
                    && let Some(value) = self.global_definitions.get(&path.segments[1])
                {
                    return Ok(CompiledExpression::Literal(value.value.clone()));
                }

                Ok(CompiledExpression::DotPath(path.segments.clone()))
            }
            ExpressionKind::Binary {
                left,
                operator,
                right,
            } => Ok(CompiledExpression::Binary {
                op: self.convert_binop(*operator),
                left: Box::new(self.compile_expression(left)?),
                right: Box::new(self.compile_expression(right)?),
            }),
            ExpressionKind::FunctionCall { name, arguments } => {
                let compiled_args = arguments
                    .iter()
                    .map(|arg| self.compile_expression(arg))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(CompiledExpression::FunctionCall {
                    name: name.clone(),
                    args: compiled_args,
                })
            }
            ExpressionKind::Unary { operator, operand } => Ok(CompiledExpression::Unary {
                op: Self::convert_unop(*operator),
                operand: Box::new(self.compile_expression(operand)?),
            }),
            ExpressionKind::Array(elements) => {
                let compiled = elements
                    .iter()
                    .map(|e| self.compile_expression(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(CompiledExpression::Array(compiled))
            }
            ExpressionKind::WhenExpression {
                branches,
                else_value,
            } => {
                let branches = branches
                    .iter()
                    .map(|b| {
                        Ok(WhenBranch {
                            condition: self.compile_expression(&b.condition)?,
                            value: self.compile_expression(&b.value)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ConvertError>>()?;
                let else_expr = match else_value {
                    Some(e) => Some(Box::new(self.compile_expression(e)?)),
                    None => None,
                };
                Ok(CompiledExpression::When {
                    branches,
                    else_expr,
                })
            }
            ExpressionKind::IsNull(inner) => Ok(CompiledExpression::IsNull(Box::new(
                self.compile_expression(inner)?,
            ))),
            ExpressionKind::IsNotNull(inner) => Ok(CompiledExpression::IsNotNull(Box::new(
                self.compile_expression(inner)?,
            ))),
            ExpressionKind::Grouped(inner) => Ok(CompiledExpression::Grouped(Box::new(
                self.compile_expression(inner)?,
            ))),
            ExpressionKind::PluginCall(_) => Err(ConvertError::Expression(
                "plugin calls must be a top-level `select` field \
                 (e.g. `col = plugin.name({...})`), not nested inside an expression. \
                 Assign the plugin output to its own column first, then reference \
                 that column"
                    .to_string(),
            )),
        }
    }

    fn convert_unop(op: UnaryOperator) -> UnaryOp {
        match op {
            UnaryOperator::Not => UnaryOp::Not,
            UnaryOperator::Negate => UnaryOp::Negate,
        }
    }

    // Used for simple expressions only during plan building
    pub fn eval_expression(&self, expr: &Expression) -> Result<Value, ConvertError> {
        let definitions = HashMap::new();
        let env = self.env.clone();
        let env_getter = move |key: &str| env.get(key);
        expression_engine::eval_ast_expression(expr, &definitions, &env_getter)
            .map_err(|e| ConvertError::Expression(e.to_string()))
    }

    // Evaluate expression with access to global definitions (for define.X references)
    pub(crate) fn eval_with_definitions(&self, expr: &Expression) -> Result<Value, ConvertError> {
        let env = self.env.clone();
        let env_getter = move |key: &str| env.get(key);
        expression_engine::eval_ast_expression(expr, &self.global_definitions, &env_getter)
            .map_err(|e| ConvertError::Expression(e.to_string()))
    }

    /// Extract definitions with source information
    pub fn extract_definitions(
        &mut self,
        def_block: &DefineBlock,
    ) -> Result<HashMap<String, DefinitionInfo>, ConvertError> {
        use crate::plan::env::EnvVarCollector;

        let mut definitions = HashMap::new();
        for attr in &def_block.attributes {
            let value = self.eval_expression(&attr.value)?;
            let source = EnvVarCollector::analyze_value_source(&attr.value);

            definitions.insert(attr.key.name.clone(), DefinitionInfo { value, source });
        }
        Ok(definitions)
    }

    /// Convert a `plugin "name" { path = ..., allow_http = ..., ... }` block
    /// into a `PluginDecl`. Unknown attributes are ignored; `path` is required.
    pub fn build_plugin(&self, block: &PluginBlock) -> Result<PluginDecl, ConvertError> {
        let as_string = |v: &Value| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        };
        let as_bool = |v: &Value| matches!(v, Value::Boolean(true));
        let as_u64 = |v: &Value| match v {
            Value::Int(n) if *n >= 0 => Some(*n as u64),
            Value::UInt(n) => Some(*n),
            _ => None,
        };
        let as_str_list = |v: &Value| match v {
            Value::Array(items) => items.iter().filter_map(&as_string).collect::<Vec<_>>(),
            _ => Vec::new(),
        };

        let mut decl = PluginDecl {
            name: block.name.clone(),
            path: PathBuf::new(),
            allow_http: false,
            allow_http_hosts: Vec::new(),
            allow_kv: false,
            allow_log: true,
            allow_metrics: false,
            allow_fs_read: Vec::new(),
            allow_fs_write: Vec::new(),
            allow_env: Vec::new(),
            memory_limit_bytes: None,
            fuel_limit: None,
            timeout_ms: None,
            config_json: None,
        };

        for attr in &block.attributes {
            let value = self.eval_expression(&attr.value)?;
            match attr.key.name.as_str() {
                "path" => decl.path = as_string(&value).map(PathBuf::from).unwrap_or_default(),
                "allow_http" => decl.allow_http = as_bool(&value),
                "allow_http_hosts" => decl.allow_http_hosts = as_str_list(&value),
                "allow_kv" => decl.allow_kv = as_bool(&value),
                "allow_log" => decl.allow_log = as_bool(&value),
                "allow_metrics" => decl.allow_metrics = as_bool(&value),
                "memory_limit_bytes" => decl.memory_limit_bytes = as_u64(&value),
                "fuel_limit" => decl.fuel_limit = as_u64(&value),
                "timeout_ms" => decl.timeout_ms = as_u64(&value),
                "allow_fs_read" => {
                    decl.allow_fs_read =
                        as_str_list(&value).into_iter().map(PathBuf::from).collect()
                }
                "allow_fs_write" => {
                    decl.allow_fs_write =
                        as_str_list(&value).into_iter().map(PathBuf::from).collect()
                }
                "allow_env" => decl.allow_env = as_str_list(&value),
                _ => {}
            }
        }

        // Nested `config { ... }` block -> JSON bytes passed to the plugin at __paganel_initialize.
        if let Some(cfg) = block.nested_blocks.iter().find(|b| b.kind == "config") {
            let mut map = serde_json::Map::new();
            for attr in &cfg.attributes {
                let value = self.eval_expression(&attr.value)?;
                map.insert(attr.key.name.clone(), Self::value_to_plain_json(&value));
            }
            let bytes = serde_json::to_vec(&serde_json::Value::Object(map)).map_err(|e| {
                ConvertError::Plan(format!("plugin '{}' config serialization: {e}", block.name))
            })?;
            decl.config_json = Some(bytes);
        }

        if decl.path.as_os_str().is_empty() {
            return Err(ConvertError::Plan(format!(
                "plugin '{}' is missing required 'path'",
                block.name
            )));
        }
        Ok(decl)
    }

    /// Convert an evaluated PPL value into plain JSON for a plugin's config
    /// (natural scalars/arrays, not the `{type,value}` exchange envelope).
    fn value_to_plain_json(v: &Value) -> serde_json::Value {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Int(n) => serde_json::Value::from(*n),
            Value::UInt(n) => serde_json::Value::from(*n),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(Self::value_to_plain_json).collect())
            }
            other => serde_json::Value::String(format!("{other:?}")),
        }
    }

    fn convert_binop(&self, op: BinaryOperator) -> BinaryOp {
        match op {
            BinaryOperator::Add => BinaryOp::Add,
            BinaryOperator::Subtract => BinaryOp::Subtract,
            BinaryOperator::Multiply => BinaryOp::Multiply,
            BinaryOperator::Divide => BinaryOp::Divide,
            BinaryOperator::Modulo => BinaryOp::Modulo,
            BinaryOperator::Equal => BinaryOp::Equal,
            BinaryOperator::NotEqual => BinaryOp::NotEqual,
            BinaryOperator::GreaterThan => BinaryOp::GreaterThan,
            BinaryOperator::LessThan => BinaryOp::LessThan,
            BinaryOperator::GreaterOrEqual => BinaryOp::GreaterOrEqual,
            BinaryOperator::LessOrEqual => BinaryOp::LessOrEqual,
            BinaryOperator::And => BinaryOp::And,
            BinaryOperator::Or => BinaryOp::Or,
        }
    }

    fn allowed_tuning_keys(dialect: &str) -> &'static [&'static str] {
        match dialect {
            DIALECT_POSTGRES => &[TUNING_PK_CREATION, TUNING_COPY_FORMAT, TUNING_ON_CONFLICT],
            DIALECT_MYSQL => &[TUNING_ON_CONFLICT],
            _ => &[],
        }
    }

    fn validate_tuning_value(
        key: &str,
        value: &Value,
        driver: &str,
        endpoint: &str,
    ) -> Result<(), ConvertError> {
        match key {
            // Conflict handling is dialect-specific: Postgres routes through a
            // staging table + `ON CONFLICT`, MySQL uses a `LOAD DATA` modifier.
            TUNING_ON_CONFLICT => {
                let valid = match value {
                    Value::String(s) => match driver {
                        DIALECT_POSTGRES => PgConflictAction::parse(s).is_some(),
                        DIALECT_MYSQL => LoadDataConflict::parse(s).is_some(),
                        _ => false,
                    },
                    _ => false,
                };
                if !valid {
                    let expected = match driver {
                        DIALECT_POSTGRES => "\"do_nothing\" or \"do_update\"",
                        _ => "\"default\", \"replace\" or \"ignore\"",
                    };
                    return Err(ConvertError::Plan(format!(
                        "invalid `{TUNING_ON_CONFLICT}` in {endpoint}: expected {expected}"
                    )));
                }
            }
            TUNING_COPY_FORMAT => {
                let is_valid = matches!(value, Value::String(s) if CopyFormat::parse(s).is_some());
                if !is_valid {
                    return Err(ConvertError::Plan(format!(
                        "invalid `{TUNING_COPY_FORMAT}` in {endpoint}: expected \"binary\" or \"text\""
                    )));
                }
            }
            TUNING_PK_CREATION => {
                let is_valid = matches!(value, Value::String(s) if PkCreation::parse(s).is_some());
                if !is_valid {
                    return Err(ConvertError::Plan(format!(
                        "invalid `{TUNING_PK_CREATION}` in {endpoint}: expected \"pre\" or \"post\""
                    )));
                }
            }
            _ => {} // Other keys are either implicitly valid or unvalidated
        }
        Ok(())
    }

    fn read_table_list(
        &self,
        pipeline: &str,
        tables_attr: &Attribute,
    ) -> Result<Vec<String>, ConvertError> {
        let ExpressionKind::Array(items) = &tables_attr.value.kind else {
            return Err(ConvertError::Plan(format!(
                "pipeline `{pipeline}`: `tables` must be a list of table names"
            )));
        };

        let mut tables = Vec::with_capacity(items.len());
        for item in items {
            match self.eval_with_definitions(item)? {
                Value::String(s) => tables.push(s),
                other => {
                    return Err(ConvertError::Plan(format!(
                        "pipeline `{pipeline}`: `tables` must contain strings, found {other:?}"
                    )));
                }
            }
        }

        if tables.is_empty() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{pipeline}`: `tables` is empty"
            )));
        }

        let mut seen = HashSet::new();
        for t in &tables {
            if !seen.insert(t.to_ascii_lowercase()) {
                return Err(ConvertError::Plan(format!(
                    "pipeline `{pipeline}`: `tables` lists `{t}` more than once"
                )));
            }
        }

        Ok(tables)
    }

    fn validate_multi_table_guardrails(
        block: &PipelineBlock,
        from: &FromBlock,
    ) -> Result<(), ConvertError> {
        let name = &block.name;

        if from.attributes.iter().any(|a| a.key.name == ATTR_TABLE) {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: `table` and `tables` are mutually exclusive"
            )));
        }

        if from.references.is_some() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: `tables` cannot be combined with `with references`"
            )));
        }

        if block.with_block.is_some() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: `tables` cannot be combined with a `with {{ }}` join block"
            )));
        }

        if block.select_block.is_some() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: an unnamed `select {{ }}` is ambiguous with `tables`; use `select \"table\" {{ }}` per table"
            )));
        }

        if !block.where_clauses.is_empty() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: `where` is not supported with `tables`"
            )));
        }

        if block.validate_block.is_some() {
            return Err(ConvertError::Plan(format!(
                "pipeline `{name}`: `validate` is not supported with `tables`"
            )));
        }

        Ok(())
    }

    fn validate_multi_table_named_selects(
        block: &PipelineBlock,
        table_set: &HashSet<String>,
    ) -> Result<(), ConvertError> {
        for named in &block.named_select_blocks {
            if !table_set.contains(&named.table.to_ascii_lowercase()) {
                return Err(ConvertError::Plan(format!(
                    "pipeline `{}`: select \"{}\" refers to a table not in `tables`",
                    block.name, named.table
                )));
            }
        }
        Ok(())
    }

    fn multi_table_renames(
        &self,
        block: &PipelineBlock,
        table_set: &HashSet<String>,
    ) -> Result<HashMap<String, String>, ConvertError> {
        let rename = match &block.to {
            Some(to) => self.build_table_map(to)?,
            None => HashMap::new(),
        };

        for key in rename.keys() {
            if !table_set.contains(&key.to_ascii_lowercase()) {
                return Err(ConvertError::Plan(format!(
                    "pipeline `{}`: map key `{}` is not in `tables`",
                    block.name, key
                )));
            }
        }
        Ok(rename)
    }

    fn expand_one_table(
        block: &PipelineBlock,
        table: &str,
        rename: &HashMap<String, String>,
    ) -> PipelineBlock {
        let mut nb = block.clone();
        nb.name = format!("{}:{}", block.name, table);

        if let Some(f) = nb.from.as_mut() {
            f.attributes.retain(|a| a.key.name != ATTR_TABLES);
            f.attributes.push(string_attr(ATTR_TABLE, table, f.span));
        }

        let dest = rename
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(table))
            .map(|(_, v)| v.as_str())
            .unwrap_or(table);

        if let Some(t) = nb.to.as_mut() {
            t.attributes.retain(|a| a.key.name != ATTR_TABLE);
            t.attributes.push(string_attr(ATTR_TABLE, dest, t.span));
            t.map_block = None;
        }

        nb.select_block = block
            .named_select_blocks
            .iter()
            .find(|n| n.table.eq_ignore_ascii_case(table))
            .map(|n| SelectBlock {
                fields: n.fields.clone(),
                span: n.span,
            });
        nb.named_select_blocks = Vec::new();

        nb
    }
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new(Arc::new(EnvContext::empty()))
    }
}

/// Parse duration string (e.g., "30s", "5m", "2h") to seconds
pub(crate) fn parse_duration(s: &str) -> Result<u64, ConvertError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ConvertError::Plan("Empty duration string".to_string()));
    }

    let (num_str, unit) = if let Some(num_str) = s.strip_suffix("ms") {
        (num_str, "ms")
    } else if s.len() > 1 {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    } else {
        return Err(ConvertError::Plan(format!(
            "Invalid duration format: '{}'. Expected format like '30s', '5m', '2h'",
            s
        )));
    };

    let num: u64 = num_str.parse().map_err(|_| {
        ConvertError::Plan(format!(
            "Invalid number in duration: '{}'. Expected format like '30s', '5m', '2h'",
            s
        ))
    })?;

    let seconds = match unit {
        "ms" => num / 1000, // milliseconds to seconds
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => {
            return Err(ConvertError::Plan(format!(
                "Invalid duration unit: '{}'. Supported units: ms, s, m, h, d",
                unit
            )));
        }
    };

    Ok(seconds)
}

fn string_attr(key: &str, value: &str, span: Span) -> Attribute {
    Attribute {
        key: Identifier::new(key, span),
        value: Expression::new(
            ExpressionKind::Literal(Literal::String(value.to_string())),
            span,
        ),
        span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::execution::define::DefinitionSource;
    use ppl_syntax::ast::{
        attribute::Attribute,
        dotpath::DotPath,
        ident::Identifier,
        pipeline::{AfterBlock, BeforeBlock, NestedBlock, PaginateBlock, SettingsBlock},
        span::Span,
        validation::{
            FailedRowsBlock, OnErrorBlock, RetryBlock, ValidateBlock, ValidationBody,
            ValidationCheck,
        },
    };

    fn test_span() -> Span {
        Span::new(0, 10, 1, 1)
    }

    fn make_string_expr(s: &str) -> Expression {
        Expression::new(
            ExpressionKind::Literal(Literal::String(s.to_string())),
            test_span(),
        )
    }

    fn make_number_expr(n: f64) -> Expression {
        Expression::new(ExpressionKind::Literal(Literal::Number(n)), test_span())
    }

    fn make_bool_expr(b: bool) -> Expression {
        Expression::new(ExpressionKind::Literal(Literal::Boolean(b)), test_span())
    }

    fn make_ident_expr(s: &str) -> Expression {
        Expression::new(ExpressionKind::Identifier(s.to_string()), test_span())
    }

    fn make_dotpath_expr(segments: Vec<&str>) -> Expression {
        let path = DotPath::new(
            segments.into_iter().map(|s| s.to_string()).collect(),
            test_span(),
        );
        Expression::new(ExpressionKind::DotNotation(path), test_span())
    }

    fn make_attribute(key: &str, value: Expression) -> Attribute {
        Attribute {
            key: Identifier::new(key, test_span()),
            value,
            span: test_span(),
        }
    }

    fn make_nested_block(kind: &str, attributes: Vec<Attribute>) -> NestedBlock {
        NestedBlock {
            kind: kind.to_string(),
            attributes,
            span: test_span(),
        }
    }

    #[test]
    fn test_extract_definitions() {
        let mut builder = PlanBuilder::default();
        let def_block = DefineBlock {
            attributes: vec![
                make_attribute("tax_rate", make_number_expr(1.4)),
                make_attribute("country", make_string_expr("US")),
                make_attribute("enabled", make_bool_expr(true)),
            ],
            span: test_span(),
        };

        let result = builder.extract_definitions(&def_block).unwrap();
        assert_eq!(result.len(), 3);

        let definition_info = result.get("tax_rate").unwrap();
        assert_eq!(definition_info.value, Value::Float(1.4));

        let definition_info = result.get("country").unwrap();
        assert_eq!(definition_info.value, Value::String("US".to_string()));

        let definition_info = result.get("enabled").unwrap();
        assert_eq!(definition_info.value, Value::Boolean(true));
    }

    #[test]
    fn test_build_connection_basic() {
        let builder = PlanBuilder::default();
        let conn_block = ConnectionBlock {
            name: "postgres_prod".to_string(),
            attributes: vec![
                make_attribute("driver", make_string_expr("postgres")),
                make_attribute("host", make_string_expr("localhost")),
                make_attribute("port", make_number_expr(5432.0)),
            ],
            nested_blocks: vec![],
            span: test_span(),
        };

        let result = builder.build_connection(&conn_block).unwrap();
        assert_eq!(result.name, "postgres_prod");
        assert_eq!(result.driver, "postgres");
        assert_eq!(result.properties.len(), 3);
        assert_eq!(
            result.properties.get("host"),
            Some(&Value::String("localhost".to_string()))
        );
    }

    #[test]
    fn test_build_dependencies() {
        let builder = PlanBuilder::default();

        // Test with array of DotPath (modern syntax: after = [pipeline.name1, pipeline.name2])
        let array_expr = Expression::new(
            ExpressionKind::Array(vec![
                make_dotpath_expr(vec!["pipeline", "pipeline1"]),
                make_dotpath_expr(vec!["pipeline", "pipeline0"]),
            ]),
            test_span(),
        );

        let pipeline = PipelineBlock {
            name: "pipeline2".to_string(),
            description: None,
            after: Some(vec![array_expr]),
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let deps = builder.build_dependencies(&pipeline).unwrap();
        assert_eq!(deps.len(), 2);
        assert_eq!(deps[0], "pipeline1");
        assert_eq!(deps[1], "pipeline0");
    }

    #[test]
    fn test_build_lifecycle() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: Some(BeforeBlock {
                sql: vec!["CREATE TABLE IF NOT EXISTS temp".to_string()],
                span: test_span(),
            }),
            after_block: Some(AfterBlock {
                sql: vec!["DROP TABLE temp".to_string()],
                span: test_span(),
            }),
            settings_block: None,
            span: test_span(),
        };

        let lifecycle = builder.build_lifecycle(&pipeline).unwrap();
        assert_eq!(lifecycle.before.len(), 1);
        assert_eq!(lifecycle.before[0], "CREATE TABLE IF NOT EXISTS temp");
        assert_eq!(lifecycle.after.len(), 1);
        assert_eq!(lifecycle.after[0], "DROP TABLE temp");
    }

    #[test]
    fn test_build_settings() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: Some(SettingsBlock {
                attributes: vec![
                    make_attribute("batch_size", make_number_expr(100.0)),
                    make_attribute("parallel", make_bool_expr(true)),
                ],
                span: test_span(),
            }),
            span: test_span(),
        };

        let settings = builder.build_settings(&pipeline).unwrap();
        assert_eq!(settings.len(), 2);
        assert_eq!(settings.get("batch_size"), Some(&Value::Float(100.0)));
        assert_eq!(settings.get("parallel"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_compile_expression_literals() {
        let builder = PlanBuilder::default();

        // String literal
        let expr = make_string_expr("hello");
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected string literal"),
        }

        // Number literal
        let expr = make_number_expr(42.5);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Float(n)) => assert_eq!(n, 42.5),
            _ => panic!("Expected number literal"),
        }

        // Boolean literal
        let expr = make_bool_expr(true);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Boolean(b)) => assert!(b),
            _ => panic!("Expected boolean literal"),
        }
    }

    #[test]
    fn test_compile_when_expression() {
        let builder = PlanBuilder::default();

        // when { status == "active" then "on" else "off" }
        let condition = Expression::new(
            ExpressionKind::Binary {
                left: Box::new(make_ident_expr("status")),
                operator: BinaryOperator::Equal,
                right: Box::new(make_string_expr("active")),
            },
            test_span(),
        );
        let expr = Expression::new(
            ExpressionKind::WhenExpression {
                branches: vec![ppl_syntax::ast::expr::WhenBranch {
                    condition,
                    value: make_string_expr("on"),
                    span: test_span(),
                }],
                else_value: Some(Box::new(make_string_expr("off"))),
            },
            test_span(),
        );

        match builder.compile_expression(&expr).unwrap() {
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                assert_eq!(branches.len(), 1);
                assert!(matches!(
                    branches[0].condition,
                    CompiledExpression::Binary { .. }
                ));
                assert!(
                    matches!(&branches[0].value, CompiledExpression::Literal(Value::String(s)) if s == "on")
                );
                assert!(
                    matches!(else_expr.as_deref(), Some(CompiledExpression::Literal(Value::String(s))) if s == "off")
                );
            }
            other => panic!("expected When, got {other:?}"),
        }
    }

    #[test]
    fn test_compile_is_null_and_grouped() {
        let builder = PlanBuilder::default();

        // `x is null` must compile to IsNull, not silently to NULL.
        let is_null = Expression::new(
            ExpressionKind::IsNull(Box::new(make_ident_expr("x"))),
            test_span(),
        );
        assert!(matches!(
            builder.compile_expression(&is_null).unwrap(),
            CompiledExpression::IsNull(_)
        ));

        // Grouped `( x )` preserves the inner expression.
        let grouped = Expression::new(
            ExpressionKind::Grouped(Box::new(make_ident_expr("x"))),
            test_span(),
        );
        assert!(matches!(
            builder.compile_expression(&grouped).unwrap(),
            CompiledExpression::Grouped(_)
        ));
    }

    #[test]
    fn test_compile_expression_identifier() {
        let builder = PlanBuilder::default();
        let expr = make_ident_expr("customer_id");
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Identifier(id) => assert_eq!(id, "customer_id"),
            _ => panic!("Expected identifier"),
        }
    }

    #[test]
    fn test_compile_expression_dotpath() {
        let builder = PlanBuilder::default();
        let expr = make_dotpath_expr(vec!["customers", "email"]);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::DotPath(segments) => {
                assert_eq!(segments, vec!["customers", "email"]);
            }
            _ => panic!("Expected dotpath"),
        }
    }

    #[test]
    fn test_compile_expression_binary() {
        let builder = PlanBuilder::default();
        let left = make_number_expr(5.0);
        let right = make_number_expr(3.0);
        let expr = Expression::new(
            ExpressionKind::Binary {
                left: Box::new(left),
                operator: BinaryOperator::Add,
                right: Box::new(right),
            },
            test_span(),
        );

        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Binary { op, .. } => {
                assert!(matches!(op, BinaryOp::Add));
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_compile_expression_with_define_reference() {
        let mut builder = PlanBuilder::default();
        builder.global_definitions.insert(
            "tax_rate".to_string(),
            DefinitionInfo {
                value: Value::Float(1.4),
                source: DefinitionSource::Literal,
            },
        );

        let expr = make_dotpath_expr(vec!["define", "tax_rate"]);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Float(n)) => assert_eq!(n, 1.4),
            _ => panic!("Expected resolved define reference"),
        }
    }

    #[test]
    fn test_convert_binop_all_operators() {
        let builder = PlanBuilder::default();

        assert!(matches!(
            builder.convert_binop(BinaryOperator::Add),
            BinaryOp::Add
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Subtract),
            BinaryOp::Subtract
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Multiply),
            BinaryOp::Multiply
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Divide),
            BinaryOp::Divide
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Equal),
            BinaryOp::Equal
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::NotEqual),
            BinaryOp::NotEqual
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::And),
            BinaryOp::And
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Or),
            BinaryOp::Or
        ));
    }

    #[test]
    fn test_build_pagination_with_all_fields() {
        let mut builder = PlanBuilder::default();

        // Add a test connection
        let mut properties = Properties::new();
        properties.insert(
            "url".to_string(),
            Value::String("postgres://localhost/test".to_string()),
        );

        builder.connections.insert(
            "postgres".to_string(),
            Connection {
                name: "postgres".to_string(),
                driver: "postgres".to_string(),
                properties,
                nested_configs: HashMap::new(),
            },
        );

        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: Some(FromBlock {
                attributes: vec![
                    make_attribute(
                        "connection",
                        make_dotpath_expr(vec!["connection", "postgres"]),
                    ),
                    make_attribute("table", make_string_expr("customers")),
                ],
                nested_blocks: vec![],
                references: None,
                span: test_span(),
            }),
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: Some(PaginateBlock {
                attributes: vec![
                    make_attribute("strategy", make_string_expr("pk")),
                    make_attribute("cursor", make_string_expr("id")),
                ],
                span: test_span(),
            }),
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let source = builder.build_source(&pipeline).unwrap();
        let pagination = source.pagination.unwrap();
        assert_eq!(pagination.strategy, "pk");
        assert_eq!(pagination.column, "id");
    }

    #[test]
    fn test_build_validation_rules() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: Some(ValidateBlock {
                checks: vec![
                    ValidationCheck {
                        kind: ValidationKind::Assert,
                        label: "positive_amount".to_string(),
                        body: ValidationBody {
                            check: Expression::new(
                                ExpressionKind::Binary {
                                    left: Box::new(make_ident_expr("amount")),
                                    operator: BinaryOperator::GreaterThan,
                                    right: Box::new(make_number_expr(0.0)),
                                },
                                test_span(),
                            ),
                            message: "Amount must be positive".to_string(),
                            action: Some("fail".to_string()),
                        },
                        span: test_span(),
                    },
                    ValidationCheck {
                        kind: ValidationKind::Warn,
                        label: "large_amount".to_string(),
                        body: ValidationBody {
                            check: Expression::new(
                                ExpressionKind::Binary {
                                    left: Box::new(make_ident_expr("amount")),
                                    operator: BinaryOperator::LessThan,
                                    right: Box::new(make_number_expr(10000.0)),
                                },
                                test_span(),
                            ),
                            message: "Large amount detected".to_string(),
                            action: Some("continue".to_string()),
                        },
                        span: test_span(),
                    },
                ],
                span: test_span(),
            }),
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let rules = builder.build_validation_rules(&pipeline).unwrap();
        assert_eq!(rules.len(), 2);

        assert_eq!(rules[0].label, "positive_amount");
        assert!(matches!(rules[0].severity, ValidationSeverity::Assert));
        assert!(matches!(rules[0].action, ValidationAction::Fail));

        assert_eq!(rules[1].label, "large_amount");
        assert!(matches!(rules[1].severity, ValidationSeverity::Warn));
        assert!(matches!(rules[1].action, ValidationAction::Continue));
    }

    #[test]
    fn test_build_error_handling() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: Some(RetryBlock {
                    attributes: vec![
                        make_attribute("max_attempts", make_number_expr(5.0)),
                        make_attribute("delay_ms", make_number_expr(500.0)),
                    ],
                    span: test_span(),
                }),
                failed_rows: None,
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let retry = error_handling.retry.unwrap();
        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.delay_ms, 500);
        assert!(matches!(retry.backoff, BackoffStrategy::Exponential));
    }

    #[test]
    fn test_failed_rows_with_table_block_with_schema() {
        let mut builder = PlanBuilder::default();

        // Add a test connection to the builder's connections map
        builder.connections.insert(
            "warehouse".to_string(),
            Connection {
                name: "warehouse".to_string(),
                driver: "postgres".to_string(),
                properties: Properties::new(),
                nested_configs: HashMap::new(),
            },
        );

        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "table",
                        vec![
                            make_attribute(
                                "connection",
                                make_dotpath_expr(vec!["connection", "warehouse"]),
                            ),
                            make_attribute("schema", make_string_expr("dlq")),
                            make_attribute("table", make_string_expr("failed_orders")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                assert_eq!(connection.name, "warehouse");
                assert_eq!(connection.driver, "postgres");
                assert_eq!(table, "failed_orders");
                assert_eq!(schema, Some("dlq".to_string()));
            }
            _ => panic!("Expected Table destination"),
        }
    }

    #[test]
    fn test_failed_rows_with_table_block_without_schema() {
        let mut builder = PlanBuilder::default();

        // Add a test connection to the builder's connections map
        builder.connections.insert(
            "error_db".to_string(),
            Connection {
                name: "error_db".to_string(),
                driver: "mysql".to_string(),
                properties: Properties::new(),
                nested_configs: HashMap::new(),
            },
        );

        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "table",
                        vec![
                            make_attribute(
                                "connection",
                                make_dotpath_expr(vec!["connection", "error_db"]),
                            ),
                            make_attribute("table", make_string_expr("failed_rows")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                assert_eq!(connection.name, "error_db");
                assert_eq!(connection.driver, "mysql");
                assert_eq!(table, "failed_rows");
                assert_eq!(schema, None);
            }
            _ => panic!("Expected Table destination"),
        }
    }

    #[test]
    fn test_failed_rows_with_file_block_explicit_format() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "file",
                        vec![
                            make_attribute("path", make_string_expr("/data/errors.jsonl")),
                            make_attribute("format", make_string_expr("json")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::File { path, format } => {
                assert_eq!(path, "/data/errors.jsonl");
                assert!(matches!(format, FileFormat::Json));
            }
            _ => panic!("Expected File destination"),
        }
    }

    #[test]
    fn test_failed_rows_file_unsupported_format_is_rejected() {
        let builder = PlanBuilder::default();

        // Both an explicit csv format and a parquet extension must fail at
        // plan time, since only JSON dead-letter files are supported.
        for (path, format_attr) in [
            ("/data/errors.csv", Some("csv")),
            ("/data/errors.parquet", None),
        ] {
            let mut attrs = vec![make_attribute("path", make_string_expr(path))];
            if let Some(f) = format_attr {
                attrs.push(make_attribute("format", make_string_expr(f)));
            }
            let pipeline = PipelineBlock {
                name: "test".to_string(),
                description: None,
                after: None,
                from: None,
                to: None,
                where_clauses: vec![],
                with_block: None,
                select_block: None,
                named_select_blocks: vec![],
                validate_block: None,
                on_error_block: Some(OnErrorBlock {
                    retry: None,
                    failed_rows: Some(FailedRowsBlock {
                        attributes: vec![],
                        nested_blocks: vec![make_nested_block("file", attrs)],
                        span: test_span(),
                    }),
                    span: test_span(),
                }),
                paginate_block: None,
                before_block: None,
                after_block: None,
                settings_block: None,
                span: test_span(),
            };

            let err = builder
                .build_error_handling(&pipeline)
                .expect_err("non-json dead-letter file format should be rejected");
            assert!(
                err.to_string().contains("not supported"),
                "unexpected error for {path}: {err}"
            );
        }
    }

    #[test]
    fn test_failed_rows_with_file_block_auto_detect_format() {
        let builder = PlanBuilder::default();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "file",
                        vec![make_attribute(
                            "path",
                            make_string_expr("/logs/failed_rows.jsonl"),
                        )],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::File { path, format } => {
                assert_eq!(path, "/logs/failed_rows.jsonl");
                // Defaults to JSON when the extension is not recognized.
                assert!(matches!(format, FileFormat::Json));
            }
            _ => panic!("Expected File destination"),
        }
    }

    #[test]
    fn test_env_function_with_typed_defaults() {
        let mut env_ctx = EnvContext::empty();
        env_ctx.set("BATCH_SIZE".to_string(), "5000".to_string());
        env_ctx.set("CREATE_TABLES".to_string(), "true".to_string());
        env_ctx.set("THRESHOLD".to_string(), "0.95".to_string());
        let builder = PlanBuilder::new(Arc::new(env_ctx));

        // Test integer default - env var exists
        let expr_int = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("BATCH_SIZE"),
                    make_number_expr(1000.0), // default
                ],
            },
            test_span(),
        );
        let result_int = builder.eval_expression(&expr_int).unwrap();
        // Should parse as Uint since positive integer
        assert!(matches!(result_int, Value::UInt(5000)));

        // Test boolean default - env var exists
        let expr_bool = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("CREATE_TABLES"),
                    make_bool_expr(false), // default
                ],
            },
            test_span(),
        );
        let result_bool = builder.eval_expression(&expr_bool).unwrap();
        assert!(matches!(result_bool, Value::Boolean(true)));

        // Test float default - env var exists
        let expr_float = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("THRESHOLD"),
                    make_number_expr(0.5), // default
                ],
            },
            test_span(),
        );
        let result_float = builder.eval_expression(&expr_float).unwrap();
        assert!(matches!(result_float, Value::Float(v) if (v - 0.95).abs() < 0.001));

        // Test when env var doesn't exist - should return typed default
        let expr_missing_int = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("MISSING_VAR"),
                    make_number_expr(1234.0), // default
                ],
            },
            test_span(),
        );
        let result_missing = builder.eval_expression(&expr_missing_int).unwrap();
        assert!(matches!(result_missing, Value::Float(1234.0)));
    }

    #[test]
    fn test_env_function_parse_failure() {
        let mut env_ctx = EnvContext::empty();
        env_ctx.set("BAD_INT".to_string(), "not_a_number".to_string());
        let builder = PlanBuilder::new(Arc::new(env_ctx));

        // Try to parse invalid integer
        let expr = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![make_string_expr("BAD_INT"), make_number_expr(100.0)],
            },
            test_span(),
        );

        let result = builder.eval_expression(&expr);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to parse environment variable")
        );
    }

    // ----- expand_pipeline (multi-table `tables = [...]`) --------------------

    use ppl_syntax::ast::pipeline::{FieldMapping, FromBlock, MapBlock, NamedSelectBlock, ToBlock};

    fn make_array_expr(items: Vec<&str>) -> Expression {
        Expression::new(
            ExpressionKind::Array(items.into_iter().map(make_string_expr).collect()),
            test_span(),
        )
    }

    fn conn_from(attrs: Vec<Attribute>) -> FromBlock {
        let mut attributes = vec![make_attribute(
            "connection",
            make_dotpath_expr(vec!["connection", "src"]),
        )];
        attributes.extend(attrs);
        FromBlock {
            attributes,
            nested_blocks: vec![],
            references: None,
            span: test_span(),
        }
    }

    fn conn_to(map_block: Option<MapBlock>) -> ToBlock {
        ToBlock {
            attributes: vec![make_attribute(
                "connection",
                make_dotpath_expr(vec!["connection", "dst"]),
            )],
            nested_blocks: vec![],
            map_block,
            span: test_span(),
        }
    }

    fn tables_block(from: FromBlock, to: ToBlock) -> PipelineBlock {
        PipelineBlock {
            name: "warehouse".to_string(),
            description: None,
            after: None,
            from: Some(from),
            to: Some(to),
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        }
    }

    fn table_attr_of(block: &PipelineBlock, which: fn(&PipelineBlock) -> &[Attribute]) -> String {
        which(block)
            .iter()
            .find(|a| a.key.name == ATTR_TABLE)
            .and_then(|a| match &a.value.kind {
                ExpressionKind::Literal(Literal::String(s)) => Some(s.clone()),
                _ => None,
            })
            .expect("table attribute")
    }

    fn from_attrs(b: &PipelineBlock) -> &[Attribute] {
        &b.from.as_ref().unwrap().attributes
    }
    fn to_attrs(b: &PipelineBlock) -> &[Attribute] {
        &b.to.as_ref().unwrap().attributes
    }

    #[test]
    fn expand_without_tables_passes_through() {
        let builder = PlanBuilder::default();
        let block = tables_block(
            conn_from(vec![make_attribute("table", make_string_expr("orders"))]),
            conn_to(None),
        );
        let out = builder.expand_pipeline(&block).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "warehouse");
    }

    #[test]
    fn expand_tables_fans_out_one_per_table() {
        let builder = PlanBuilder::default();
        let block = tables_block(
            conn_from(vec![make_attribute(
                "tables",
                make_array_expr(vec!["actor", "film", "payment"]),
            )]),
            conn_to(None),
        );
        let out = builder.expand_pipeline(&block).unwrap();
        assert_eq!(out.len(), 3);

        let names: Vec<_> = out.iter().map(|p| p.name.clone()).collect();
        assert_eq!(
            names,
            ["warehouse:actor", "warehouse:film", "warehouse:payment"]
        );

        // Every expansion has a single-table `table` on both ends (== source name
        // with no map), no leftover `tables`, and no root select.
        for (p, t) in out.iter().zip(["actor", "film", "payment"]) {
            assert_eq!(table_attr_of(p, from_attrs), t);
            assert_eq!(table_attr_of(p, to_attrs), t);
            assert!(!from_attrs(p).iter().any(|a| a.key.name == ATTR_TABLES));
            assert!(p.select_block.is_none());
            assert!(p.named_select_blocks.is_empty());
        }
    }

    #[test]
    fn expand_named_select_becomes_root_and_map_renames_dest() {
        let builder = PlanBuilder::default();
        let mut block = tables_block(
            conn_from(vec![make_attribute(
                "tables",
                make_array_expr(vec!["customer", "film"]),
            )]),
            conn_to(Some(MapBlock {
                mappings: vec![FieldMapping {
                    name: Identifier::new("customer", test_span()),
                    value: make_string_expr("dim_customer"),
                    span: test_span(),
                }],
                span: test_span(),
            })),
        );
        block.named_select_blocks = vec![NamedSelectBlock {
            table: "customer".to_string(),
            fields: vec![FieldMapping {
                name: Identifier::new("given_name", test_span()),
                value: make_dotpath_expr(vec!["customer", "first_name"]),
                span: test_span(),
            }],
            span: test_span(),
        }];

        let out = builder.expand_pipeline(&block).unwrap();
        assert_eq!(out.len(), 2);

        let customer = out.iter().find(|p| p.name == "warehouse:customer").unwrap();
        // map renames the destination table only.
        assert_eq!(table_attr_of(customer, from_attrs), "customer");
        assert_eq!(table_attr_of(customer, to_attrs), "dim_customer");
        // the named select is promoted to the root select.
        let root = customer.select_block.as_ref().expect("root select");
        assert_eq!(root.fields.len(), 1);
        assert_eq!(root.fields[0].name.name, "given_name");

        // film has no named select and no rename -> full copy, dest == source.
        let film = out.iter().find(|p| p.name == "warehouse:film").unwrap();
        assert!(film.select_block.is_none());
        assert_eq!(table_attr_of(film, to_attrs), "film");
    }

    #[test]
    fn expand_rejects_unnamed_select_and_bad_references() {
        let builder = PlanBuilder::default();

        // Unnamed root select is ambiguous across tables.
        let mut block = tables_block(
            conn_from(vec![make_attribute(
                "tables",
                make_array_expr(vec!["a", "b"]),
            )]),
            conn_to(None),
        );
        block.select_block = Some(SelectBlock {
            fields: vec![],
            span: test_span(),
        });
        assert!(builder.expand_pipeline(&block).is_err());

        // A named select for a table that isn't listed.
        let mut block2 = tables_block(
            conn_from(vec![make_attribute("tables", make_array_expr(vec!["a"]))]),
            conn_to(None),
        );
        block2.named_select_blocks = vec![NamedSelectBlock {
            table: "zzz".to_string(),
            fields: vec![],
            span: test_span(),
        }];
        assert!(builder.expand_pipeline(&block2).is_err());

        // Duplicate table names.
        let dup = tables_block(
            conn_from(vec![make_attribute(
                "tables",
                make_array_expr(vec!["a", "a"]),
            )]),
            conn_to(None),
        );
        assert!(builder.expand_pipeline(&dup).is_err());

        // `table` and `tables` together.
        let both = tables_block(
            conn_from(vec![
                make_attribute("table", make_string_expr("a")),
                make_attribute("tables", make_array_expr(vec!["a"])),
            ]),
            conn_to(None),
        );
        assert!(builder.expand_pipeline(&both).is_err());
    }
}
