use crate::{
    ast::{
        attribute::Attribute,
        block::{
            ColumnMatch, ConnectionBlock, DefineBlock, ExecutionBlock, HookBlock, PairBlock,
            PairSideBlock, PluginBlock,
        },
        doc::PplDocument,
        dotpath::DotPath,
        expr::{Expression, ExpressionKind, PluginCall, PluginInputField, WhenBranch},
        ident::Identifier,
        literal::Literal,
        operator::BinaryOperator,
        pipeline::{
            AfterBlock, BeforeBlock, FieldMapping, FromBlock, JoinClause, MapBlock,
            NamedSelectBlock, NestedBlock, PaginateBlock, PipelineBlock, ReferencesBlock,
            SelectBlock, SettingsBlock, ToBlock, WhereClause, WithBlock,
        },
        span::Span,
        validation::{
            FailedRowsBlock, OnErrorBlock, RetryBlock, ValidateBlock, ValidationBody,
            ValidationCheck, ValidationKind,
        },
    },
    errors::BuildError,
    parser::{PplParser, Rule},
};
use pest::{
    Parser,
    iterators::{Pair, Pairs},
};

pub type BuildResult<T> = Result<T, BuildError>;

/// Parse PPL text into a typed AST
pub fn parse(input: &str) -> BuildResult<PplDocument> {
    let pairs = PplParser::parse(Rule::program, input).map_err(|e| BuildError {
        message: format!("Syntax error: {}", e),
        line: 1,
        column: 1,
    })?;

    build_document(pairs)
}

fn build_document(mut pairs: Pairs<Rule>) -> BuildResult<PplDocument> {
    let program = pairs.next().ok_or_else(|| BuildError {
        message: "Empty input".to_string(),
        line: 1,
        column: 1,
    })?;

    let span = pair_to_span(&program);
    let mut define_block = None;
    let mut execution_block = None;
    let mut connections = Vec::new();
    let mut pipelines = Vec::new();
    let mut plugins = Vec::new();
    let mut pairs = Vec::new();
    let mut hooks = Vec::new();

    for pair in program.into_inner() {
        match pair.as_rule() {
            Rule::define_block => {
                define_block = Some(build_define_block(pair)?);
            }
            Rule::execution_block => {
                execution_block = Some(build_execution_block(pair)?);
            }
            Rule::connection_block => {
                connections.push(build_connection_block(pair)?);
            }
            Rule::pipeline_block => {
                pipelines.push(build_pipeline_block(pair)?);
            }
            Rule::plugin_block => {
                plugins.push(build_plugin_block(pair)?);
            }
            Rule::pair_block => {
                pairs.push(build_pair_block(pair)?);
            }
            Rule::hook_block => {
                hooks.push(build_hook_block(pair)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(PplDocument {
        define_block,
        execution_block,
        connections,
        pipelines,
        plugins,
        pairs,
        hooks,
        span,
    })
}

fn pair_to_span(pair: &Pair<Rule>) -> crate::ast::span::Span {
    let (line, col) = pair.line_col();
    let span_pest = pair.as_span();
    Span::new(span_pest.start(), span_pest.end(), line, col)
}

fn build_define_block(pair: Pair<Rule>) -> BuildResult<DefineBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(DefineBlock { attributes, span })
}

fn build_execution_block(pair: Pair<Rule>) -> BuildResult<ExecutionBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(ExecutionBlock { attributes, span })
}

fn build_connection_block(pair: Pair<Rule>) -> BuildResult<ConnectionBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                name = parse_string_literal(inner.as_str());
            }
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(ConnectionBlock {
        name,
        attributes,
        nested_blocks,
        span,
    })
}

fn build_pipeline_block(pair: Pair<Rule>) -> BuildResult<PipelineBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut after = None;
    let mut from = None;
    let mut to = None;
    let mut where_clauses = Vec::new();
    let mut with_block = None;
    let mut select_block = None;
    let mut named_select_blocks = Vec::new();
    let mut validate_block = None;
    let mut on_error_block = None;
    let mut paginate_block = None;
    let mut before_block = None;
    let mut after_block = None;
    let mut settings_block = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                name = parse_string_literal(inner.as_str());
            }
            Rule::attribute => {
                let attr = build_attribute(inner)?;
                if attr.key.name == "after" {
                    after = Some(vec![attr.value]);
                }
            }
            Rule::from_block => {
                from = Some(build_from_block(inner)?);
            }
            Rule::to_block => {
                to = Some(build_to_block(inner)?);
            }
            Rule::where_block => {
                where_clauses.push(build_where_clause(inner)?);
            }
            Rule::with_block => {
                with_block = Some(build_with_block(inner)?);
            }
            Rule::select_block => {
                select_block = Some(build_select_block(inner)?);
            }
            Rule::named_select_block => {
                named_select_blocks.push(build_named_select_block(inner)?);
            }
            Rule::validate_block => {
                validate_block = Some(build_validate_block(inner)?);
            }
            Rule::on_error_block => {
                on_error_block = Some(build_on_error_block(inner)?);
            }
            Rule::paginate_block => {
                paginate_block = Some(build_paginate_block(inner)?);
            }
            Rule::before_block => {
                before_block = Some(build_before_block(inner)?);
            }
            Rule::after_block => {
                after_block = Some(build_after_block(inner)?);
            }
            Rule::settings_block => {
                settings_block = Some(build_settings_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(PipelineBlock {
        name,
        description: None,
        after,
        from,
        to,
        where_clauses,
        with_block,
        select_block,
        named_select_blocks,
        validate_block,
        on_error_block,
        paginate_block,
        before_block,
        after_block,
        settings_block,
        span,
    })
}

fn build_plugin_block(pair: Pair<Rule>) -> BuildResult<PluginBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => name = parse_string_literal(inner.as_str()),
            Rule::attribute => attributes.push(build_attribute(inner)?),
            Rule::nested_block => nested_blocks.push(build_nested_block(inner)?),
            _ => {}
        }
    }
    Ok(PluginBlock {
        name,
        attributes,
        nested_blocks,
        span,
    })
}

fn build_pair_block(pair: Pair<Rule>) -> BuildResult<PairBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut attributes = Vec::new();
    let mut sides = Vec::new();
    let mut map = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => name = parse_string_literal(inner.as_str()),
            Rule::attribute => attributes.push(build_attribute(inner)?),
            Rule::pair_side => sides.push(build_pair_side(inner)?),
            Rule::pair_map => {
                for line in inner.into_inner() {
                    if line.as_rule() == Rule::column_match {
                        map.push(build_column_match(line)?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(PairBlock {
        name,
        attributes,
        sides,
        map,
        span,
    })
}

fn build_hook_block(pair: Pair<Rule>) -> BuildResult<HookBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => name = parse_string_literal(inner.as_str()),
            Rule::attribute => attributes.push(build_attribute(inner)?),
            Rule::nested_block => nested_blocks.push(build_nested_block(inner)?),
            _ => {}
        }
    }
    Ok(HookBlock {
        name,
        attributes,
        nested_blocks,
        span,
    })
}

fn build_pair_side(pair: Pair<Rule>) -> BuildResult<PairSideBlock> {
    let span = pair_to_span(&pair);
    let mut kind = String::new();
    let mut attributes = Vec::new();
    let mut where_clauses = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => kind = inner.as_str().to_string(),
            Rule::attribute => attributes.push(build_attribute(inner)?),
            Rule::where_block => where_clauses.push(build_where_clause(inner)?),
            _ => {}
        }
    }

    Ok(PairSideBlock {
        kind,
        attributes,
        where_clauses,
        span,
    })
}

fn build_column_match(pair: Pair<Rule>) -> BuildResult<ColumnMatch> {
    let span = pair_to_span(&pair);
    let paths: Vec<DotPath> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::dotted_ident)
        .map(|p| DotPath::from_string(p.as_str(), pair_to_span(&p)))
        .collect();

    let [left, right] = <[DotPath; 2]>::try_from(paths).map_err(|_| BuildError {
        message: "map line must be `source.column = target.column`".to_string(),
        line: span.line,
        column: span.column,
    })?;

    Ok(ColumnMatch { left, right, span })
}

fn build_nested_block(pair: Pair<Rule>) -> BuildResult<NestedBlock> {
    let span = pair_to_span(&pair);
    let mut name = Identifier::new("", span);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            _ => {}
        }
    }

    Ok(NestedBlock {
        kind: name.to_string(),
        attributes,
        span,
    })
}

fn build_from_block(pair: Pair<Rule>) -> BuildResult<FromBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();
    let mut references = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::references_block => {
                references = Some(build_references_block(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(FromBlock {
        attributes,
        nested_blocks,
        references,
        span,
    })
}

fn build_to_block(pair: Pair<Rule>) -> BuildResult<ToBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();
    let mut map_block = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::map_block => {
                map_block = Some(build_map_block(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(ToBlock {
        attributes,
        nested_blocks,
        map_block,
        span,
    })
}

fn build_references_block(pair: Pair<Rule>) -> BuildResult<ReferencesBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(ReferencesBlock { attributes, span })
}

fn build_map_block(pair: Pair<Rule>) -> BuildResult<MapBlock> {
    let span = pair_to_span(&pair);
    let mut mappings = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::field_mapping {
            mappings.push(build_field_mapping(inner)?);
        }
    }

    Ok(MapBlock { mappings, span })
}

fn build_where_clause(pair: Pair<Rule>) -> BuildResult<WhereClause> {
    let span = pair_to_span(&pair);
    let mut label = None;
    let mut conditions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                label = Some(parse_string_literal(inner.as_str()));
            }
            Rule::expression => {
                conditions.push(build_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(WhereClause {
        label,
        conditions,
        span,
    })
}

fn build_with_block(pair: Pair<Rule>) -> BuildResult<WithBlock> {
    let span = pair_to_span(&pair);
    let mut joins = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::join_clause {
            joins.push(build_join_clause(inner)?);
        }
    }

    Ok(WithBlock { joins, span })
}

fn build_join_clause(pair: Pair<Rule>) -> BuildResult<JoinClause> {
    let span = pair_to_span(&pair);
    let mut alias = Identifier::new("", span);
    let mut table = Identifier::new("", span);
    let mut condition = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                // First ident is alias, second is table
                if alias.name.is_empty() {
                    alias = Identifier::new(inner.as_str(), pair_to_span(&inner));
                } else {
                    table = Identifier::new(inner.as_str(), pair_to_span(&inner));
                }
            }
            Rule::expression => {
                condition = Some(build_expression(inner)?);
            }
            _ => {} // Skip keywords like kw_from, kw_where
        }
    }

    Ok(JoinClause {
        alias,
        table,
        condition,
        span,
    })
}

fn build_select_block(pair: Pair<Rule>) -> BuildResult<SelectBlock> {
    let span = pair_to_span(&pair);
    let mut fields = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::field_mapping {
            fields.push(build_field_mapping(inner)?);
        }
    }

    Ok(SelectBlock { fields, span })
}

fn build_named_select_block(pair: Pair<Rule>) -> BuildResult<NamedSelectBlock> {
    let span = pair_to_span(&pair);
    let mut table = String::new();
    let mut fields = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                table = parse_string_literal(inner.as_str());
            }
            Rule::field_mapping => {
                fields.push(build_field_mapping(inner)?);
            }
            _ => {}
        }
    }

    Ok(NamedSelectBlock {
        table,
        fields,
        span,
    })
}

fn build_field_mapping(pair: Pair<Rule>) -> BuildResult<FieldMapping> {
    let span = pair_to_span(&pair);
    let mut name = Identifier::new("", span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::expression => {
                value = build_expression(inner)?;
            }
            _ => {}
        }
    }

    Ok(FieldMapping { name, value, span })
}

fn build_validate_block(pair: Pair<Rule>) -> BuildResult<ValidateBlock> {
    let span = pair_to_span(&pair);
    let mut checks = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::validation_check {
            checks.push(build_validation_check(inner)?);
        }
    }

    Ok(ValidateBlock { checks, span })
}

fn build_plugin_call_inner(pair: Pair<Rule>) -> BuildResult<PluginCall> {
    let span = pair_to_span(&pair);
    let mut plugin_name = String::new();
    let mut inputs = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => plugin_name = inner.as_str().to_string(),
            Rule::plugin_input_block => {
                for field_pair in inner.into_inner() {
                    if field_pair.as_rule() == Rule::plugin_input_field {
                        inputs.push(build_plugin_input_field(field_pair)?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(PluginCall {
        plugin_name,
        inputs,
        span,
    })
}

fn build_plugin_input_field(pair: Pair<Rule>) -> BuildResult<PluginInputField> {
    let span = pair_to_span(&pair);
    let mut plugin_field = String::new();
    let mut source_ref = DotPath::new(Vec::new(), span);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                if plugin_field.is_empty() {
                    plugin_field = inner.as_str().to_string();
                } else {
                    source_ref =
                        DotPath::new(vec![inner.as_str().to_string()], pair_to_span(&inner));
                }
            }
            Rule::dotted_ident => {
                let segments: Vec<String> =
                    inner.as_str().split('.').map(|s| s.to_string()).collect();
                source_ref = DotPath::new(segments, pair_to_span(&inner));
            }
            _ => {}
        }
    }

    Ok(crate::ast::expr::PluginInputField {
        plugin_field,
        source_ref,
        span,
    })
}

fn build_validation_check(pair: Pair<Rule>) -> BuildResult<ValidationCheck> {
    let span = pair_to_span(&pair);
    let mut kind = ValidationKind::Assert;
    let mut label = String::new();
    let mut check = None;
    let mut message = None;
    let mut action = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_assert => kind = ValidationKind::Assert,
            Rule::kw_warn => kind = ValidationKind::Warn,
            Rule::lit_string => {
                label = parse_string_literal(inner.as_str());
            }
            Rule::validation_body => {
                let mut pairs = inner.into_inner();
                while let Some(pair) = pairs.next() {
                    if pair.as_rule() == Rule::op_eq
                        && let Some(value_pair) = pairs.next()
                    {
                        match value_pair.as_rule() {
                            Rule::expression => {
                                check = Some(build_expression(value_pair)?);
                            }
                            Rule::lit_string => {
                                message = Some(parse_string_literal(value_pair.as_str()));
                            }
                            Rule::ident => {
                                action = Some(value_pair.as_str().to_string());
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ValidationCheck {
        kind,
        label,
        body: ValidationBody {
            check: check.unwrap_or_else(|| {
                Expression::new(ExpressionKind::Literal(Literal::Boolean(true)), span)
            }),
            message: message.unwrap_or_default(),
            action,
        },
        span,
    })
}

fn build_on_error_block(pair: Pair<Rule>) -> BuildResult<OnErrorBlock> {
    let span = pair_to_span(&pair);
    let mut retry = None;
    let mut failed_rows = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::retry_block => {
                retry = Some(build_retry_block(inner)?);
            }
            Rule::failed_rows_block => {
                failed_rows = Some(build_failed_rows_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(OnErrorBlock {
        retry,
        failed_rows,
        span,
    })
}

fn build_retry_block(pair: Pair<Rule>) -> BuildResult<RetryBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(RetryBlock { attributes, span })
}

fn build_failed_rows_block(pair: Pair<Rule>) -> BuildResult<FailedRowsBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(FailedRowsBlock {
        attributes,
        nested_blocks,
        span,
    })
}

fn build_paginate_block(pair: Pair<Rule>) -> BuildResult<PaginateBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(PaginateBlock { attributes, span })
}

fn build_before_block(pair: Pair<Rule>) -> BuildResult<BeforeBlock> {
    let span = pair_to_span(&pair);
    let mut sql = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::sql_attr {
            // sql_attr = { "sql" ~ op_eq ~ array_literal }
            for sql_inner in inner.into_inner() {
                if sql_inner.as_rule() == Rule::array_literal {
                    // Build the array expression and extract string literals
                    let array_expr = build_array_literal(sql_inner, span)?;
                    if let ExpressionKind::Array(elements) = array_expr.kind {
                        for elem in elements {
                            if let ExpressionKind::Literal(Literal::String(s)) = elem.kind {
                                sql.push(s);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(BeforeBlock { sql, span })
}

fn build_after_block(pair: Pair<Rule>) -> BuildResult<AfterBlock> {
    let span = pair_to_span(&pair);
    let mut sql = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::sql_attr {
            // sql_attr = { "sql" ~ op_eq ~ array_literal }
            for sql_inner in inner.into_inner() {
                if sql_inner.as_rule() == Rule::array_literal {
                    // Build the array expression and extract string literals
                    let array_expr = build_array_literal(sql_inner, span)?;
                    if let ExpressionKind::Array(elements) = array_expr.kind {
                        for elem in elements {
                            if let ExpressionKind::Literal(Literal::String(s)) = elem.kind {
                                sql.push(s);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(AfterBlock { sql, span })
}

fn build_settings_block(pair: Pair<Rule>) -> BuildResult<SettingsBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(SettingsBlock { attributes, span })
}

fn build_attribute(pair: Pair<Rule>) -> BuildResult<Attribute> {
    let span = pair_to_span(&pair);
    let mut key = Identifier::new("", span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                key = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::expression => {
                value = build_expression(inner)?;
            }
            _ => {}
        }
    }

    Ok(Attribute { key, value, span })
}

fn build_expression(pair: Pair<Rule>) -> BuildResult<Expression> {
    let span = pair_to_span(&pair);
    build_expression_inner(pair, span)
}

/// A parse tree that is missing an expected child.
fn missing_inner(span: Span, what: &str) -> BuildError {
    BuildError {
        message: format!("malformed {what}: expected a child node"),
        line: span.line,
        column: span.column,
    }
}

fn build_expression_inner(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    match pair.as_rule() {
        Rule::expression => {
            // Unwrap the top-level expression rule
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| missing_inner(span, "expression"))?;
            build_expression_inner(inner, span)
        }
        Rule::logical_or => build_binary_expression(pair, span),
        Rule::logical_and => build_binary_expression(pair, span),
        Rule::equality => build_binary_expression(pair, span),
        Rule::comparison => build_binary_expression(pair, span),
        Rule::additive => build_binary_expression(pair, span),
        Rule::multiplicative => build_binary_expression(pair, span),
        Rule::is_null_check => build_is_null_check(pair, span),
        Rule::primary => build_primary_expression(pair, span),
        _ => Err(BuildError {
            message: format!("Unexpected rule in expression: {:?}", pair.as_rule()),
            line: span.line,
            column: span.column,
        }),
    }
}

fn build_binary_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut inner = pair.into_inner();
    let left = inner
        .next()
        .ok_or_else(|| missing_inner(span, "binary expression operand"))?;

    // Check if there's an operator
    if let Some(op_pair) = inner.next() {
        let operator = match op_pair.as_str() {
            "||" => BinaryOperator::Or,
            "&&" => BinaryOperator::And,
            "==" => BinaryOperator::Equal,
            "!=" => BinaryOperator::NotEqual,
            ">" => BinaryOperator::GreaterThan,
            "<" => BinaryOperator::LessThan,
            ">=" => BinaryOperator::GreaterOrEqual,
            "<=" => BinaryOperator::LessOrEqual,
            "+" => BinaryOperator::Add,
            "-" => BinaryOperator::Subtract,
            "*" => BinaryOperator::Multiply,
            "/" => BinaryOperator::Divide,
            "%" => BinaryOperator::Modulo,
            op => {
                return Err(BuildError {
                    message: format!("Unknown operator: {}", op),
                    line: span.line,
                    column: span.column,
                });
            }
        };

        let right = inner
            .next()
            .ok_or_else(|| missing_inner(span, "binary expression right operand"))?;

        Ok(Expression::new(
            ExpressionKind::Binary {
                left: Box::new(build_expression_inner(left, span)?),
                operator,
                right: Box::new(build_expression_inner(right, span)?),
            },
            span,
        ))
    } else {
        // No operator, just recurse
        build_expression_inner(left, span)
    }
}

fn build_primary_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| missing_inner(span, "primary expression"))?;
    build_primary_inner(inner, span)
}

fn build_dot_notation(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    // dotted_ident is an atomic rule (@), so we need to parse the string manually
    let segments: Vec<String> = pair.as_str().split('.').map(|s| s.to_string()).collect();

    Ok(Expression::new(
        ExpressionKind::DotNotation(DotPath::new(segments, span)),
        span,
    ))
}

fn build_function_call(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut name = String::new();
    let mut arguments = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = inner.as_str().to_string();
            }
            Rule::expression => {
                arguments.push(build_expression_inner(inner, span)?);
            }
            _ => {}
        }
    }

    Ok(Expression::new(
        ExpressionKind::FunctionCall { name, arguments },
        span,
    ))
}

fn build_array_literal(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            elements.push(build_expression_inner(inner, span)?);
        }
    }

    Ok(Expression::new(ExpressionKind::Array(elements), span))
}

fn build_when_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut branches = Vec::new();
    let mut else_value = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::when_branch => {
                branches.push(build_when_branch(inner)?);
            }
            Rule::expression => {
                else_value = Some(Box::new(build_expression_inner(inner, span)?));
            }
            _ => {}
        }
    }

    Ok(Expression::new(
        ExpressionKind::WhenExpression {
            branches,
            else_value,
        },
        span,
    ))
}

fn build_when_branch(pair: Pair<Rule>) -> BuildResult<crate::ast::expr::WhenBranch> {
    let span = pair_to_span(&pair);
    let mut condition = Expression::new(ExpressionKind::Literal(Literal::Null), span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    let inner = pair.into_inner();

    for item in inner {
        if item.as_rule() == Rule::expression {
            // First expression is condition, second is value
            if matches!(condition.kind, ExpressionKind::Literal(Literal::Null)) {
                condition = build_expression_inner(item, span)?;
            } else {
                value = build_expression_inner(item, span)?;
            }
        }
    }

    Ok(WhenBranch {
        condition,
        value,
        span,
    })
}

fn build_is_null_check(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut inner = pair.into_inner().peekable();

    // Since primary is a silent rule, we need to handle grouped expressions specially
    // For grouped expressions: lparen ~ expression ~ rparen
    // For other primaries: just the token itself
    let primary_expr = if inner.peek().map(|p| p.as_rule()) == Some(Rule::lparen) {
        // Skip lparen
        inner.next();
        // Get the expression
        let expr = inner
            .next()
            .ok_or_else(|| missing_inner(span, "grouped expression"))?;
        let result = build_expression_inner(expr, span)?;
        // Skip rparen
        inner.next();
        result
    } else {
        // Regular primary expression
        let primary = inner
            .next()
            .ok_or_else(|| missing_inner(span, "primary expression"))?;
        build_primary_inner(primary, span)?
    };

    // Check if there are "is null" or "is not null" keywords following
    let mut has_is = false;
    let mut has_not = false;
    let mut has_null = false;

    for token in inner {
        match token.as_rule() {
            Rule::kw_is => has_is = true,
            Rule::kw_not => has_not = true,
            Rule::kw_null => has_null = true,
            _ => {}
        }
    }

    // Wrap it based on what keywords we found
    if has_is && has_not && has_null {
        // "primary is not null"
        Ok(Expression::new(
            ExpressionKind::IsNotNull(Box::new(primary_expr)),
            span,
        ))
    } else if has_is && has_null {
        // "primary is null"
        Ok(Expression::new(
            ExpressionKind::IsNull(Box::new(primary_expr)),
            span,
        ))
    } else {
        Ok(primary_expr)
    }
}

fn build_primary_inner(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    match pair.as_rule() {
        Rule::lit_number => {
            let s = pair.as_str();
            let literal = if s.contains('.') || s.contains('e') || s.contains('E') {
                // Parse as float
                let num = s.parse::<f64>().map_err(|_| BuildError {
                    message: format!("Invalid number: {}", s),
                    line: span.line,
                    column: span.column,
                })?;
                Literal::Number(num)
            } else {
                // Parse as integer
                let num = s.parse::<i64>().map_err(|_| BuildError {
                    message: format!("Invalid integer: {}", s),
                    line: span.line,
                    column: span.column,
                })?;
                Literal::Int(num)
            };
            Ok(Expression::new(ExpressionKind::Literal(literal), span))
        }
        Rule::lit_string => {
            let s = parse_string_literal(pair.as_str());
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::String(s)),
                span,
            ))
        }
        Rule::lit_boolean => {
            let b = pair.as_str() == "true";
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::Boolean(b)),
                span,
            ))
        }
        Rule::kw_null => Ok(Expression::new(
            ExpressionKind::Literal(Literal::Null),
            span,
        )),
        Rule::ident => {
            let name = pair.as_str().to_string();
            Ok(Expression::new(ExpressionKind::Identifier(name), span))
        }
        Rule::dotted_ident => Ok(build_dot_notation(pair, span)?),
        Rule::plugin_call => {
            let call = build_plugin_call_inner(pair)?;
            Ok(Expression::new(ExpressionKind::PluginCall(call), span))
        }
        Rule::fn_call => Ok(build_function_call(pair, span)?),
        Rule::array_literal => Ok(build_array_literal(pair, span)?),
        Rule::when_expr => Ok(build_when_expression(pair, span)?),
        Rule::expression => {
            // Grouped expression - just build the inner expression
            // (the grouping is represented by context, not by wrapping)
            build_expression_inner(pair, span)
        }
        _ => Err(BuildError {
            message: format!("Unexpected primary expression: {:?}", pair.as_rule()),
            line: span.line,
            column: span.column,
        }),
    }
}

fn parse_string_literal(s: &str) -> String {
    // Remove quotes and unescape
    let s = s.trim_matches('"');
    s.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
}
