use crate::{
    ast::expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
    renderer::{Render, Renderer},
};

impl Render for Expr {
    fn render(&self, r: &mut Renderer) {
        match self {
            Expr::Identifier(ident) => ident.render(r),
            Expr::Value(val) => r.add_param(val.clone()),
            Expr::BinaryOp(op) => op.render(r),
            Expr::FunctionCall(func) => func.render(r),
            Expr::Alias { expr, alias } => {
                expr.render(r);
                r.sql.push_str(" AS ");
                r.sql.push_str(&r.dialect.quote_identifier(alias));
            }
            Expr::Cast { expr, data_type } => {
                r.sql.push_str("CAST(");
                expr.render(r); // Render the inner expression (e.g., the placeholder)
                r.sql.push_str(" AS ");
                r.sql.push_str(data_type);
                r.sql.push(')');
            }
            Expr::Literal(lit) => {
                r.sql.push_str(lit);
            }
            Expr::Case {
                when_branches,
                else_expr,
            } => {
                r.sql.push_str("CASE");
                for (condition, value) in when_branches {
                    r.sql.push_str(" WHEN ");
                    condition.render(r);
                    r.sql.push_str(" THEN ");
                    value.render(r);
                }
                if let Some(else_val) = else_expr {
                    r.sql.push_str(" ELSE ");
                    else_val.render(r);
                }
                r.sql.push_str(" END");
            }
            Expr::FilteredAggregate { function, filter } => {
                function.render(r);
                r.sql.push_str(" FILTER (WHERE ");
                filter.render(r);
                r.sql.push(')');
            }
            Expr::Not(expr) => {
                r.sql.push_str("NOT (");
                expr.render(r);
                r.sql.push(')');
            }
            Expr::In { expr, values } => {
                expr.render(r);
                r.sql.push_str(" IN (");
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        r.sql.push_str(", ");
                    }
                    value.render(r);
                }
                r.sql.push(')');
            }
            Expr::IsNull { expr, negated } => {
                expr.render(r);
                r.sql
                    .push_str(if *negated { " IS NOT NULL" } else { " IS NULL" });
            }
        }
    }
}

impl Render for Ident {
    fn render(&self, r: &mut Renderer) {
        if let Some(qualifier) = &self.qualifier {
            r.sql.push_str(&r.dialect.quote_identifier(qualifier));
            r.sql.push('.');
        }
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));
    }
}

impl Render for BinaryOp {
    fn render(&self, r: &mut Renderer) {
        r.sql.push('(');
        self.left.render(r);

        let op_str = match self.op {
            BinaryOperator::Eq => " = ",
            BinaryOperator::NotEq => " <> ",
            BinaryOperator::Lt => " < ",
            BinaryOperator::LtEq => " <= ",
            BinaryOperator::Gt => " > ",
            BinaryOperator::GtEq => " >= ",
            BinaryOperator::And => " AND ",
            BinaryOperator::Or => " OR ",
            BinaryOperator::Plus => " + ",
            BinaryOperator::Minus => " - ",
            BinaryOperator::Multiply => " * ",
            BinaryOperator::Modulo => " % ",
            BinaryOperator::IntDiv => r.dialect.int_div_operator(),
        };
        r.sql.push_str(op_str);

        self.right.render(r);
        r.sql.push(')');
    }
}

impl Render for FunctionCall {
    fn render(&self, r: &mut Renderer) {
        // Handle dialect-specific function names
        let function_name = if self.name == "RANDOM" && self.args.is_empty() {
            // Use dialect-specific random function (RANDOM() for PostgreSQL, RAND() for MySQL)
            r.dialect.random_fn().trim_end_matches("()")
        } else {
            &self.name
        };

        r.sql.push_str(function_name);
        r.sql.push('(');
        if self.wildcard {
            r.sql.push('*');
        } else {
            for (i, arg) in self.args.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                arg.render(r);
            }
        }
        r.sql.push(')');
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::expr::{Expr, Ident},
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    fn field(name: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: None,
            name: name.to_string(),
        })
    }

    #[test]
    fn renders_is_null_and_is_not_null() {
        let pg = Postgres;

        let mut r = Renderer::new(&pg);
        Expr::IsNull {
            expr: Box::new(field("return_date")),
            negated: false,
        }
        .render(&mut r);
        let (sql, params) = r.finish();
        assert!(sql.ends_with(" IS NULL"), "got: {sql}");
        assert!(sql.contains("return_date"));
        assert!(params.is_empty(), "null checks bind no params");

        let mut r = Renderer::new(&pg);
        Expr::IsNull {
            expr: Box::new(field("return_date")),
            negated: true,
        }
        .render(&mut r);
        assert!(r.finish().0.ends_with(" IS NOT NULL"));
    }
}
