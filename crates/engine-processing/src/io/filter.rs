use connectors::{drivers::csv::filter::CsvFilter, sql::filter::SqlFilter};
use model::execution::{
    expr::{BinaryOp, CompiledExpression},
    pipeline,
};

#[derive(Debug, Clone)]
pub enum Filter {
    Sql(SqlFilter),
    Csv(CsvFilter),
}

/// AND every `where` condition together, as SQL does with a `WHERE`:
/// `where { age > 18 } where { status == "active" }` becomes
/// `(age > 18) AND (status == "active")`. `None` when there are none.
pub fn combine_filters(filters: &[pipeline::Filter]) -> Option<CompiledExpression> {
    filters
        .iter()
        .map(|f| f.condition.clone())
        .reduce(|left, right| CompiledExpression::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(name: &str) -> pipeline::Filter {
        pipeline::Filter {
            label: None,
            condition: CompiledExpression::Identifier(name.into()),
        }
    }

    fn and(left: CompiledExpression, right: CompiledExpression) -> CompiledExpression {
        CompiledExpression::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    #[test]
    fn conditions_are_anded_in_order() {
        let id = |n: &str| CompiledExpression::Identifier(n.into());

        assert!(combine_filters(&[]).is_none());
        assert!(matches!(
            combine_filters(&[cond("a")]),
            Some(CompiledExpression::Identifier(n)) if n == "a"
        ));
        assert_eq!(
            format!("{:?}", combine_filters(&[cond("a"), cond("b"), cond("c")])),
            format!("{:?}", Some(and(and(id("a"), id("b")), id("c"))))
        );
    }
}
