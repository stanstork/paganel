use ppl_syntax::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock},
        doc::PplDocument,
        expr::{Expression, ExpressionKind},
        literal::Literal,
        operator::BinaryOperator,
        pipeline::{FieldMapping, FromBlock, PipelineBlock, SelectBlock, ToBlock},
    },
    errors::ValidationIssueKind,
    semantic::validator::validate,
};

use super::helpers::*;

#[test]
fn test_missing_required_fields_connection() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![ConnectionBlock {
            name: "db1".to_string(),
            attributes: vec![
                // Missing driver and url
            ],
            nested_blocks: vec![],
            span: span(1, 1),
        }],
        pipelines: vec![],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    assert_eq!(result.errors.len(), 2); // Missing driver and url
}

#[test]
fn test_missing_required_fields_pipeline() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync".to_string(),
            after: None,
            from: None, // Missing
            to: None,   // Missing
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
    assert_eq!(result.errors.len(), 2); // Missing from and to
}

#[test]
fn test_unused_connection_warning() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![ConnectionBlock {
            name: "unused_db".to_string(),
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
        pipelines: vec![],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(!result.has_errors());
    assert_eq!(result.warnings.len(), 1);
    assert!(matches!(
        result.warnings[0].kind,
        ValidationIssueKind::UnusedConnection { .. }
    ));
}

#[test]
fn test_unused_define_constant_warning() {
    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![Attribute {
                key: ident("unused_const", span(2, 3)),
                value: string_lit("1.4", span(2, 18)),
                span: span(2, 3),
            }],
            span: span(1, 1),
        }),
        execution_block: None,
        connections: vec![],
        pipelines: vec![],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(!result.has_errors());
    assert_eq!(result.warnings.len(), 1);
    assert!(matches!(
        result.warnings[0].kind,
        ValidationIssueKind::UnusedDefineConstant { .. }
    ));
}

#[test]
fn test_empty_define_block_warning() {
    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![],
            span: span(1, 1),
        }),
        execution_block: None,
        connections: vec![],
        pipelines: vec![],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(!result.has_errors());
    assert_eq!(result.warnings.len(), 1);
    assert!(matches!(
        result.warnings[0].kind,
        ValidationIssueKind::EmptyBlock { .. }
    ));
}

#[test]
fn test_valid_document_no_errors() {
    let s = span(1, 1);

    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![Attribute {
                key: ident("tax_rate", s),
                value: Expression::new(ExpressionKind::Literal(Literal::Number(1.4)), s),
                span: s,
            }],
            span: s,
        }),
        execution_block: None,
        connections: vec![ConnectionBlock {
            name: "db1".to_string(),
            attributes: vec![
                Attribute {
                    key: ident("driver", s),
                    value: string_lit("mysql", s),
                    span: s,
                },
                Attribute {
                    key: ident("url", s),
                    value: string_lit("localhost", s),
                    span: s,
                },
            ],
            nested_blocks: vec![],
            span: s,
        }],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync".to_string(),
            after: None,
            from: Some(FromBlock {
                attributes: vec![Attribute {
                    key: ident("connection", s),
                    value: dot_notation(&["connection", "db1"], s),
                    span: s,
                }],
                nested_blocks: vec![],
                references: None,
                span: s,
            }),
            to: Some(ToBlock {
                attributes: vec![Attribute {
                    key: ident("connection", s),
                    value: dot_notation(&["connection", "db1"], s),
                    span: s,
                }],
                nested_blocks: vec![],
                map_block: None,
                span: s,
            }),
            where_clauses: vec![],
            with_block: None,
            select_block: Some(SelectBlock {
                fields: vec![FieldMapping {
                    name: ident("total_with_tax", s),
                    value: Expression::new(
                        ExpressionKind::Binary {
                            left: Box::new(dot_notation(&["orders", "total"], s)),
                            operator: BinaryOperator::Multiply,
                            right: Box::new(dot_notation(&["define", "tax_rate"], s)),
                        },
                        s,
                    ),
                    span: s,
                }],
                span: s,
            }),
            named_select_blocks: vec![],
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: s,
        }],
        span: s,
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.is_valid());
    assert_eq!(result.errors.len(), 0);
    assert_eq!(result.warnings.len(), 0);
}

#[test]
fn test_multiple_errors_collected() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![
            ConnectionBlock {
                name: "db1".to_string(),
                attributes: vec![
                    // Missing driver and url
                ],
                nested_blocks: vec![],
                span: span(1, 1),
            },
            ConnectionBlock {
                name: "db1".to_string(), // Duplicate
                attributes: vec![],
                nested_blocks: vec![],
                span: span(5, 1),
            },
        ],
        pipelines: vec![PipelineBlock {
            description: None,
            name: "sync".to_string(),
            after: Some(vec![dot_notation(
                &["pipeline", "nonexistent"],
                span(10, 12),
            )]),
            from: None, // Missing
            to: None,   // Missing
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
            span: span(9, 1),
        }],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    // Should have multiple errors collected:
    // - Missing driver (db1)
    // - Missing url (db1)
    // - Duplicate connection (db1)
    // - Undefined pipeline reference
    // - Missing from block
    // - Missing to block
    assert!(result.has_errors());
    assert!(result.errors.len() >= 5);
}
