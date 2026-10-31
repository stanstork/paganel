use ppl_syntax::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock},
        doc::PplDocument,
        pipeline::PipelineBlock,
    },
    errors::ValidationIssueKind,
    semantic::validator::validate,
};

use super::helpers::*;

#[test]
fn test_duplicate_connection_names() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![
            ConnectionBlock {
                name: "db1".to_string(),
                attributes: vec![],
                nested_blocks: vec![],
                span: span(1, 1),
            },
            ConnectionBlock {
                name: "db1".to_string(),
                attributes: vec![],
                nested_blocks: vec![],
                span: span(5, 1),
            },
        ],
        pipelines: vec![],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    assert_eq!(result.errors.len(), 5);
    assert!(matches!(
        result.errors[0].kind,
        ValidationIssueKind::DuplicateConnection { .. }
    ));
}

#[test]
fn test_duplicate_pipeline_names() {
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![],
        pipelines: vec![
            PipelineBlock {
                description: None,
                name: "sync1".to_string(),
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
                settings_block: None,
                span: span(1, 1),
            },
            PipelineBlock {
                description: None,
                name: "sync1".to_string(),
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
                settings_block: None,
                span: span(10, 1),
            },
        ],
        span: span(1, 1),
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    assert!(matches!(
        result.errors[0].kind,
        ValidationIssueKind::DuplicatePipeline { .. }
    ));
}

#[test]
fn test_duplicate_define_attributes() {
    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![
                Attribute {
                    key: ident("tax_rate", span(2, 3)),
                    value: string_lit("1.4", span(2, 15)),
                    span: span(2, 3),
                },
                Attribute {
                    key: ident("tax_rate", span(3, 3)),
                    value: string_lit("1.5", span(3, 15)),
                    span: span(3, 3),
                },
            ],
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

    assert!(result.has_errors());
    assert!(matches!(
        result.errors[0].kind,
        ValidationIssueKind::DuplicateDefineAttribute { .. }
    ));
}
