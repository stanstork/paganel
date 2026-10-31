use ppl_syntax::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock},
        doc::PplDocument,
        expr::{Expression, ExpressionKind},
        operator::BinaryOperator,
        pipeline::{FromBlock, PipelineBlock, ToBlock, WhereClause},
    },
    builder::parse,
    errors::ValidationIssueKind,
    semantic::validator::validate,
};

use super::helpers::*;

#[test]
fn test_undefined_connection_reference() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![ConnectionBlock {
            name: "db1".to_string(),
            attributes: vec![
                Attribute {
                    key: ident("driver", span(2, 3)),
                    value: string_lit("mysql", span(2, 12)),
                    span: span(2, 3),
                },
                Attribute {
                    key: ident("url", span(3, 3)),
                    value: string_lit("localhost", span(3, 9)),
                    span: span(3, 3),
                },
            ],
            nested_blocks: vec![],
            span: span(1, 1),
        }],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync".to_string(),
            after: None,
            from: Some(FromBlock {
                attributes: vec![Attribute {
                    key: ident("connection", span(8, 5)),
                    value: dot_notation(&["connection", "nonexistent"], span(8, 18)),
                    span: span(8, 5),
                }],
                nested_blocks: vec![],
                references: None,
                span: span(7, 3),
            }),
            to: Some(ToBlock {
                attributes: vec![Attribute {
                    key: ident("connection", span(11, 5)),
                    value: dot_notation(&["connection", "db1"], span(11, 18)),
                    span: span(11, 5),
                }],
                nested_blocks: vec![],
                map_block: None,
                span: span(10, 3),
            }),
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
            span: span(6, 1),
        }],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedConnection { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}

#[test]
fn test_undefined_pipeline_reference() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync1".to_string(),
            after: Some(vec![dot_notation(
                &["pipeline", "nonexistent"],
                span(2, 12),
            )]),
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
            span: span(1, 1),
        }],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedPipeline { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}

#[test]
fn test_undefined_define_constant() {
    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![Attribute {
                key: ident("tax_rate", span(2, 3)),
                value: string_lit("1.4", span(2, 15)),
                span: span(2, 3),
            }],
            span: span(1, 1),
        }),
        execution_block: None,
        connections: vec![],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync".to_string(),
            after: None,
            from: None,
            to: None,
            where_clauses: vec![WhereClause {
                label: None,
                conditions: vec![Expression::new(
                    ExpressionKind::Binary {
                        left: Box::new(dot_notation(&["orders", "total"], span(7, 5))),
                        operator: BinaryOperator::Multiply,
                        right: Box::new(dot_notation(&["define", "nonexistent"], span(7, 21))),
                    },
                    span(7, 5),
                )],
                span: span(6, 3),
            }],
            with_block: None,
            select_block: None,
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: span(5, 1),
        }],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedDefineConstant { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}

#[test]
fn test_pipeline_uses_plugin_as_source_via_dotpath() {
    // `connection = plugin.fraud` where the plugin IS declared - no errors.
    let doc = parse(
        r#"
            plugin "fraud" {
                path = "./plugins/fraud.wasm"
            }
            connection "dst" {
                driver = "postgres"
                url    = "localhost"
            }
            pipeline "p" {
                from {
                    connection = plugin.fraud
                    table = "x"
                }
                to {
                    connection = connection.dst
                    table = "x"
                }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(
        !result.has_errors(),
        "unexpected errors: {:?}",
        result.errors
    );
}

#[test]
fn test_pipeline_references_undefined_plugin_in_from_block() {
    let doc = parse(
        r#"
            connection "dst" {
                driver = "postgres"
                url    = "localhost"
            }
            pipeline "p" {
                from { connection = plugin.missing table = "x" }
                to   { connection = connection.dst   table = "x" }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(result.has_errors());
    let plugin_errs: Vec<_> = result
        .errors
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                ValidationIssueKind::UndefinedPlugin { name } if name == "missing"
            )
        })
        .collect();
    assert_eq!(plugin_errs.len(), 1);
}

#[test]
fn test_select_block_plugin_call_undefined_plugin() {
    let doc = parse(
        r#"
            connection "src" { driver = "mysql"    url = "u" }
            connection "dst" { driver = "postgres" url = "u" }
            pipeline "p" {
                from { connection = connection.src table = "x" }
                to   { connection = connection.dst table = "x" }
                select {
                    score = plugin.score_risk({ amount: x.amount })
                }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(result.has_errors());
    let plugin_errs: Vec<_> = result
        .errors
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                ValidationIssueKind::UndefinedPlugin { name } if name == "score_risk"
            )
        })
        .collect();
    assert_eq!(plugin_errs.len(), 1);
}

#[test]
fn test_select_block_plugin_call_declared_plugin_passes() {
    let doc = parse(
        r#"
            plugin "score_risk" { path = "./score.wasm" }
            connection "src" { driver = "mysql"    url = "u" }
            connection "dst" { driver = "postgres" url = "u" }
            pipeline "p" {
                from { connection = connection.src table = "x" }
                to   { connection = connection.dst table = "x" }
                select {
                    score = plugin.score_risk({ amount: x.amount })
                }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(
        !result.has_errors(),
        "unexpected errors: {:?}",
        result.errors
    );
}

#[test]
fn test_validate_plugin_check_undefined_plugin() {
    let doc = parse(
        r#"
            connection "src" { driver = "mysql"    url = "u" }
            connection "dst" { driver = "postgres" url = "u" }
            pipeline "p" {
                from { connection = connection.src table = "x" }
                to   { connection = connection.dst table = "x" }
                validate {
                    assert "fraud" {
                        check  = plugin.check_fraud({ amount: x.amount })
                        action = skip
                    }
                }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(result.has_errors());
    let plugin_errs: Vec<_> = result
        .errors
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                ValidationIssueKind::UndefinedPlugin { name } if name == "check_fraud"
            )
        })
        .collect();
    assert_eq!(plugin_errs.len(), 1);
}

#[test]
fn test_pair_checks_both_sides() {
    let doc = parse(
        r#"
            connection "app" {
                driver = "postgres"
                url    = "localhost"
            }
            pair "orders" {
                source { connection = connection.app     table = "orders" }
                target { connection = connection.missing table = "orders" }
            }
            pair "no_target" {
                source { connection = connection.app table = "orders" }
            }
        "#,
    )
    .expect("parse ok");

    let result = validate(&doc);
    assert!(result.errors.iter().any(|e| matches!(
        &e.kind,
        ValidationIssueKind::UndefinedConnection { name } if name == "missing"
    )));
    assert!(result.errors.iter().any(|e| matches!(
        &e.kind,
        ValidationIssueKind::MissingRequiredField { block_type, field }
            if block_type == "pair" && field == "target"
    )));
    // `app` is used by a pair, so it is not reported unused.
    assert!(!result.warnings.iter().any(|w| matches!(
        &w.kind,
        ValidationIssueKind::UnusedConnection { name } if name == "app"
    )));
}
