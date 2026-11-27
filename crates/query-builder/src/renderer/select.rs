use crate::{
    ast::{
        common::{JoinKind, OrderDir},
        select::{FromClause, JoinClause, OrderByExpr, Select, TableSource},
    },
    renderer::{Render, Renderer},
};

impl Render for Select {
    fn render(&self, r: &mut Renderer) {
        // 1. SELECT clause
        r.sql.push_str("SELECT ");
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(", ");
            }
            col.render(r);
        }

        // 2. FROM
        if let Some(from) = &self.from {
            r.sql.push(' ');
            from.render(r);
        }

        // 3. JOIN
        for join in &self.joins {
            r.sql.push(' ');
            join.render(r);
        }

        // 4. WHERE
        if let Some(where_clause) = &self.where_clause {
            r.sql.push_str(" WHERE ");
            where_clause.render(r);
        }

        // 5. GROUP BY
        if !self.group_by.is_empty() {
            r.sql.push_str(" GROUP BY ");
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                expr.render(r);
            }
        }

        // 6. ORDER BY
        if !self.order_by.is_empty() {
            r.sql.push_str(" ORDER BY ");
            for (i, order) in self.order_by.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                order.render(r);
            }
        }

        // 7. LIMIT
        if let Some(limit) = &self.limit {
            r.sql.push_str(" LIMIT ");
            limit.render(r);
        }

        // 8. OFFSET
        if let Some(offset) = &self.offset {
            r.sql.push_str(" OFFSET ");
            offset.render(r);
        }
    }
}

impl Render for FromClause {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("FROM ");
        match &self.table {
            TableSource::Table(table) => {
                r.sql.push_str(&r.dialect.quote_identifier(&table.name));
            }
            TableSource::Subquery(select) => {
                r.sql.push('(');
                select.render(r);
                r.sql.push(')');
            }
            TableSource::Values { rows, .. } => {
                let keyword = r.dialect.values_row_keyword();
                r.sql.push_str("(VALUES ");
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        r.sql.push_str(", ");
                    }
                    r.sql.push_str(keyword);
                    r.sql.push('(');
                    for (j, expr) in row.iter().enumerate() {
                        if j > 0 {
                            r.sql.push_str(", ");
                        }
                        expr.render(r);
                    }
                    r.sql.push(')');
                }
                r.sql.push(')');
            }
        }

        if let Some(alias) = &self.alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }
        if let TableSource::Values { columns, .. } = &self.table {
            r.sql.push_str(" (");
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                r.sql.push_str(&r.dialect.quote_identifier(column));
            }
            r.sql.push(')');
        }
    }
}

impl Render for JoinClause {
    fn render(&self, r: &mut Renderer) {
        let join_str = match self.kind {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL OUTER JOIN",
        };
        r.sql.push_str(&format!("{join_str} "));
        r.sql
            .push_str(&r.dialect.quote_identifier(&self.table.name));
        if let Some(alias) = &self.alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }
        r.sql.push_str(" ON ");
        self.on.render(r);
    }
}

impl Render for OrderByExpr {
    fn render(&self, r: &mut Renderer) {
        self.expr.render(r);
        if let Some(dir) = &self.direction {
            let dir_str = match dir {
                OrderDir::Asc => "ASC",
                OrderDir::Desc => "DESC",
            };
            r.sql.push(' ');
            r.sql.push_str(dir_str);
        }
    }
}

#[cfg(test)]
mod tests {
    use model::core::value::Value;

    use crate::{
        ast::{
            common::{JoinKind, OrderDir, TableRef},
            expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
            select::{FromClause, JoinClause, OrderByExpr, Select, TableSource},
        },
        builder::select::SelectBuilder,
        dialect::{MySql, Postgres, QueryDialect},
        renderer::{Render, Renderer},
    };

    fn ident(name: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: None,
            name: name.to_string(),
        })
    }

    fn qual_ident(qualifier: &str, name: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: Some(qualifier.to_string()),
            name: name.to_string(),
        })
    }

    fn value(val: Value) -> Expr {
        Expr::Value(val)
    }

    #[test]
    fn test_simple_select_postgres() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Eq,
                right: value(Value::Int(123)),
            }))),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(sql, r#"SELECT "id", "name" FROM "users" WHERE ("id" = $1)"#);
        assert_eq!(params, vec![Value::Int(123)]);
    }

    #[test]
    fn test_simple_select_mysql() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Eq,
                right: value(Value::String("abc".to_string())),
            }))),
            ..Default::default()
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(sql, "SELECT `id`, `name` FROM `users` WHERE (`id` = ?)");
        assert_eq!(params, vec![Value::String("abc".to_string())]);
    }

    #[test]
    fn test_complex_select_postgres() {
        let ast = Select {
            columns: vec![
                qual_ident("u", "id"),
                Expr::Alias {
                    expr: Box::new(Expr::FunctionCall(FunctionCall {
                        name: "COUNT".to_string(),
                        args: vec![qual_ident("p", "id")],
                        wildcard: false,
                    })),
                    alias: "post_count".to_string(),
                },
            ],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: Some("u".to_string()),
            }),
            joins: vec![JoinClause {
                kind: JoinKind::Left,
                table: TableRef {
                    schema: None,
                    name: "posts".to_string(),
                },
                alias: Some("p".to_string()),
                on: Expr::BinaryOp(Box::new(BinaryOp {
                    left: qual_ident("u", "id"),
                    op: BinaryOperator::Eq,
                    right: qual_ident("p", "user_id"),
                })),
            }],
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: qual_ident("u", "status"),
                op: BinaryOperator::NotEq,
                right: value(Value::String("inactive".to_string())),
            }))),
            order_by: vec![OrderByExpr {
                expr: qual_ident("u", "created_at"),
                direction: Some(OrderDir::Desc),
            }],
            group_by: vec![],
            limit: Some(value(Value::Int(10))),
            offset: Some(value(Value::Int(20))),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql = r#"SELECT "u"."id", COUNT("p"."id") AS "post_count" FROM "users" AS "u" LEFT JOIN "posts" AS "p" ON ("u"."id" = "p"."user_id") WHERE ("u"."status" <> $1) ORDER BY "u"."created_at" DESC LIMIT $2 OFFSET $3"#;
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("inactive".to_string()),
                Value::Int(10),
                Value::Int(20)
            ]
        );
    }

    #[test]
    fn test_order_by_random_postgres() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: None,
            }),
            order_by: vec![OrderByExpr {
                expr: Expr::FunctionCall(FunctionCall::random()),
                direction: None,
            }],
            limit: Some(value(Value::Int(10))),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" ORDER BY RANDOM() LIMIT $1"#
        );
        assert_eq!(params, vec![Value::Int(10)]);
    }

    #[test]
    fn test_order_by_random_mysql() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "products".to_string(),
                }),
                alias: None,
            }),
            order_by: vec![OrderByExpr {
                expr: Expr::FunctionCall(FunctionCall::random()),
                direction: None,
            }],
            limit: Some(value(Value::Int(5))),
            ..Default::default()
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `products` ORDER BY RAND() LIMIT ?"
        );
        assert_eq!(params, vec![Value::Int(5)]);
    }

    #[test]
    fn test_where_in_postgres() {
        let ast = Select {
            columns: vec![ident("id"), ident("name"), ident("email")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(ident("id").in_list(vec![
                value(Value::Int(1)),
                value(Value::Int(5)),
                value(Value::Int(10)),
            ])),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            r#"SELECT "id", "name", "email" FROM "users" WHERE "id" IN ($1, $2, $3)"#
        );
        assert_eq!(params, vec![Value::Int(1), Value::Int(5), Value::Int(10)]);
    }

    #[test]
    fn test_where_in_mysql() {
        let ast = Select {
            columns: vec![ident("product_id"), ident("name")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "products".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(ident("status").in_list(vec![
                value(Value::String("active".to_string())),
                value(Value::String("pending".to_string())),
            ])),
            ..Default::default()
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            "SELECT `product_id`, `name` FROM `products` WHERE `status` IN (?, ?)"
        );
        assert_eq!(
            params,
            vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string())
            ]
        );
    }

    #[test]
    fn test_where_in_with_and_condition_postgres() {
        let in_expr = ident("category").in_list(vec![
            value(Value::String("electronics".to_string())),
            value(Value::String("computers".to_string())),
        ]);

        let price_filter = Expr::BinaryOp(Box::new(BinaryOp {
            left: ident("price"),
            op: BinaryOperator::Lt,
            right: value(Value::Int(1000)),
        }));

        let combined = Expr::BinaryOp(Box::new(BinaryOp {
            left: in_expr,
            op: BinaryOperator::And,
            right: price_filter,
        }));

        let ast = Select {
            columns: vec![ident("id"), ident("name"), ident("price")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "products".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(combined),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            r#"SELECT "id", "name", "price" FROM "products" WHERE ("category" IN ($1, $2) AND ("price" < $3))"#
        );
        assert_eq!(
            params,
            vec![
                Value::String("electronics".to_string()),
                Value::String("computers".to_string()),
                Value::Int(1000)
            ]
        );
    }

    #[test]
    fn test_where_in_with_random_order_postgres() {
        // Combined: WHERE id IN (...) ORDER BY RANDOM() LIMIT N
        let ast = Select {
            columns: vec![ident("id"), ident("name"), ident("email")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(ident("id").in_list(vec![
                value(Value::Int(10)),
                value(Value::Int(20)),
                value(Value::Int(30)),
            ])),
            order_by: vec![OrderByExpr {
                expr: Expr::FunctionCall(FunctionCall::random()),
                direction: None,
            }],
            limit: Some(value(Value::Int(3))),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            r#"SELECT "id", "name", "email" FROM "users" WHERE "id" IN ($1, $2, $3) ORDER BY RANDOM() LIMIT $4"#
        );
        assert_eq!(
            params,
            vec![
                Value::Int(10),
                Value::Int(20),
                Value::Int(30),
                Value::Int(3)
            ]
        );
    }

    #[test]
    fn test_where_in_with_random_order_mysql() {
        // Combined: WHERE status IN (...) ORDER BY RAND() LIMIT N
        let ast = Select {
            columns: vec![ident("id"), ident("title")],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "posts".to_string(),
                }),
                alias: None,
            }),
            where_clause: Some(ident("status").in_list(vec![
                value(Value::String("published".to_string())),
                value(Value::String("featured".to_string())),
            ])),
            order_by: vec![OrderByExpr {
                expr: Expr::FunctionCall(FunctionCall::random()),
                direction: None,
            }],
            limit: Some(value(Value::Int(5))),
            ..Default::default()
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            "SELECT `id`, `title` FROM `posts` WHERE `status` IN (?, ?) ORDER BY RAND() LIMIT ?"
        );
        assert_eq!(
            params,
            vec![
                Value::String("published".to_string()),
                Value::String("featured".to_string()),
                Value::Int(5)
            ]
        );
    }

    fn render(select: &Select, dialect: &dyn QueryDialect) -> (String, Vec<Value>) {
        let mut r = Renderer::new(dialect);
        select.render(&mut r);
        r.finish()
    }

    fn op(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
        Expr::BinaryOp(Box::new(BinaryOp { left, op, right }))
    }

    fn orders() -> TableRef {
        TableRef {
            schema: None,
            name: "orders".into(),
        }
    }

    fn segments() -> Select {
        let seg = op(
            op(ident("id"), BinaryOperator::Minus, value(Value::Int(1))),
            BinaryOperator::IntDiv,
            value(Value::Int(20)),
        );
        let inner = SelectBuilder::new()
            .select(vec![seg.alias("seg")])
            .from(orders(), None)
            .build();
        SelectBuilder::new()
            .select(vec![
                ident("seg"),
                Expr::FunctionCall(FunctionCall::count_all()).alias("n"),
            ])
            .from_source(TableSource::Subquery(Box::new(inner)), "v")
            .group_by(vec![ident("seg")])
            .order_by(ident("seg"), None)
            .build()
    }

    #[test]
    fn group_by_over_a_derived_table() {
        let (sql, params) = render(&segments(), &Postgres);
        assert_eq!(
            sql,
            r#"SELECT "seg", COUNT(*) AS "n" FROM (SELECT (("id" - $1) / $2) AS "seg" FROM "orders") AS "v" GROUP BY "seg" ORDER BY "seg""#
        );
        assert_eq!(params, vec![Value::Int(1), Value::Int(20)]);

        let (sql, _) = render(&segments(), &MySql);
        assert_eq!(
            sql,
            "SELECT `seg`, COUNT(*) AS `n` FROM (SELECT ((`id` - ?) DIV ?) AS `seg` FROM `orders`) AS `v` GROUP BY `seg` ORDER BY `seg`"
        );
    }

    /// Narrowing's shape: a `VALUES` list of ranges joined to the table.
    fn ranges() -> Select {
        let rows = vec![
            vec![
                value(Value::Int(7)),
                value(Value::Int(140)),
                value(Value::Int(160)),
            ],
            vec![
                value(Value::Int(9)),
                value(Value::Int(180)),
                value(Value::Int(200)),
            ],
        ];
        let on = op(
            op(
                qual_ident("t", "id"),
                BinaryOperator::GtEq,
                qual_ident("r", "lo"),
            ),
            BinaryOperator::And,
            op(
                qual_ident("t", "id"),
                BinaryOperator::LtEq,
                qual_ident("r", "hi"),
            ),
        );
        SelectBuilder::new()
            .select(vec![
                qual_ident("r", "seg"),
                Expr::FunctionCall(FunctionCall::count_all()).alias("n"),
            ])
            .from_source(
                TableSource::Values {
                    columns: vec!["seg".into(), "lo".into(), "hi".into()],
                    rows,
                },
                "r",
            )
            .join(JoinKind::Inner, orders(), Some("t"), on)
            .group_by(vec![qual_ident("r", "seg")])
            .build()
    }

    #[test]
    fn values_list_as_a_table() {
        let (sql, params) = render(&ranges(), &Postgres);
        assert_eq!(
            sql,
            r#"SELECT "r"."seg", COUNT(*) AS "n" FROM (VALUES ($1, $2, $3), ($4, $5, $6)) AS "r" ("seg", "lo", "hi") INNER JOIN "orders" AS "t" ON (("t"."id" >= "r"."lo") AND ("t"."id" <= "r"."hi")) GROUP BY "r"."seg""#
        );
        assert_eq!(params.len(), 6);

        let (sql, _) = render(&ranges(), &MySql);
        assert_eq!(
            sql,
            "SELECT `r`.`seg`, COUNT(*) AS `n` FROM (VALUES ROW(?, ?, ?), ROW(?, ?, ?)) AS `r` (`seg`, `lo`, `hi`) INNER JOIN `orders` AS `t` ON ((`t`.`id` >= `r`.`lo`) AND (`t`.`id` <= `r`.`hi`)) GROUP BY `r`.`seg`"
        );
    }
}
