use ppl_syntax::{
    ast::{
        attribute::Attribute,
        block::DefineBlock,
        doc::PplDocument,
        expr::{Expression, ExpressionKind},
        literal::Literal,
        operator::BinaryOperator,
        pipeline::PipelineBlock,
    },
    errors::ValidationIssueKind,
    semantic::validator::validate,
};

use super::helpers::*;

#[test]
fn test_circular_define_dependency() {
    let s = span(1, 1);

    // a = define.b * 2
    // b = define.a + 1
    let doc = PplDocument {
        define_block: Some(DefineBlock {
            attributes: vec![
                Attribute {
                    key: ident("a", s),
                    value: Expression::new(
                        ExpressionKind::Binary {
                            left: Box::new(dot_notation(&["define", "b"], s)),
                            operator: BinaryOperator::Multiply,
                            right: Box::new(Expression::new(
                                ExpressionKind::Literal(Literal::Number(2.0)),
                                s,
                            )),
                        },
                        s,
                    ),
                    span: s,
                },
                Attribute {
                    key: ident("b", s),
                    value: Expression::new(
                        ExpressionKind::Binary {
                            left: Box::new(dot_notation(&["define", "a"], s)),
                            operator: BinaryOperator::Add,
                            right: Box::new(Expression::new(
                                ExpressionKind::Literal(Literal::Number(1.0)),
                                s,
                            )),
                        },
                        s,
                    ),
                    span: s,
                },
            ],
            span: s,
        }),
        execution_block: None,
        connections: vec![],
        pipelines: vec![],
        span: s,
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let circular_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::CircularDefineDependency { .. }))
        .collect();
    assert!(!circular_errors.is_empty());
}

#[test]
fn test_circular_pipeline_dependency() {
    let s = span(1, 1);

    // pipeline1 depends on pipeline2
    // pipeline2 depends on pipeline1
    let doc = PplDocument {
        define_block: None,
        execution_block: None,
        connections: vec![],
        pipelines: vec![
            PipelineBlock {
                description: None,
                name: "pipeline1".to_string(),
                after: Some(vec![dot_notation(&["pipeline", "pipeline2"], s)]),
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
                span: s,
            },
            PipelineBlock {
                description: None,
                name: "pipeline2".to_string(),
                after: Some(vec![dot_notation(&["pipeline", "pipeline1"], s)]),
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
                span: s,
            },
        ],
        span: s,
        plugins: vec![],
        pairs: vec![],
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let circular_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| {
            matches!(
                e.kind,
                ValidationIssueKind::CircularPipelineDependency { .. }
            )
        })
        .collect();
    assert!(!circular_errors.is_empty());
}
