use super::NormalSql;
use query_builder::ast::{
    common::TableRef,
    expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
};

/// Postgres functions take at most 100 arguments.
const CONCAT_CHUNK: usize = 50;

pub(super) fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall(FunctionCall {
        name: name.to_string(),
        args,
        wildcard: false,
    })
}

/// A SQL string constant.
pub(super) fn text(s: &str) -> Expr {
    Expr::Literal(format!("'{}'", s.replace('\'', "''")))
}

pub(super) fn cast(expr: Expr, data_type: &str) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        data_type: data_type.to_string(),
    }
}

/// `~` for NULL, else `<octet length>:<text>`.
pub(super) fn framed(sql: &dyn NormalSql, value: Expr) -> Expr {
    Expr::Case {
        when_branches: vec![(
            Expr::IsNull {
                expr: Box::new(value.clone()),
                negated: false,
            },
            text("~"),
        )],
        else_expr: Some(Box::new(sql.concat(vec![
            sql.byte_length(value.clone()),
            text(":"),
            value,
        ]))),
    }
}

/// `CONCAT` over any number of arguments.
pub(super) fn concat_all(mut parts: Vec<Expr>) -> Expr {
    while parts.len() > CONCAT_CHUNK {
        parts = parts
            .chunks(CONCAT_CHUNK)
            .map(|chunk| func("CONCAT", chunk.to_vec()))
            .collect();
    }
    func("CONCAT", parts)
}

pub(super) fn op(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::BinaryOp(Box::new(BinaryOp { left, op, right }))
}

pub(super) fn column(qualifier: Option<&str>, name: &str) -> Expr {
    Expr::Identifier(Ident {
        qualifier: qualifier.map(str::to_string),
        name: name.to_string(),
    })
}

pub(super) fn table(name: &str) -> TableRef {
    TableRef {
        schema: None,
        name: name.to_string(),
    }
}
