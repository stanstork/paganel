use crate::{
    ast::insert::{ConflictAction, Insert, OnConflict},
    renderer::Render,
};

impl Render for Insert {
    fn render(&self, r: &mut super::Renderer) {
        // 1. INSERT INTO table (...)
        r.sql.push_str("INSERT INTO ");
        r.render_table_ref(&self.table);
        r.sql.push_str(" (");
        let quoted_columns: Vec<String> = self
            .columns
            .iter()
            .map(|c| r.dialect.quote_identifier(c))
            .collect();
        r.sql.push_str(&quoted_columns.join(", "));
        r.sql.push(')');

        // 2. VALUES (...)
        if !self.values.is_empty() {
            render_values(self, r);
        } else if let Some(select) = &self.select {
            r.sql.push(' ');
            select.render(r);
        }

        if let Some(on_conflict) = &self.on_conflict {
            render_on_conflict(on_conflict, r);
        }
        r.sql.push(';');
    }
}

fn render_values(insert: &Insert, r: &mut super::Renderer) {
    r.sql.push_str(" VALUES ");
    for (i, row) in insert.values.iter().enumerate() {
        if i > 0 {
            r.sql.push_str(", ");
        }
        r.sql.push('(');
        for (j, val) in row.iter().enumerate() {
            if j > 0 {
                r.sql.push_str(", ");
            }
            val.render(r);
        }
        r.sql.push(')');
    }
}

fn render_on_conflict(on_conflict: &OnConflict, r: &mut super::Renderer) {
    if on_conflict.columns.is_empty() {
        return;
    }

    r.sql.push_str(" ON CONFLICT (");
    let quoted: Vec<String> = on_conflict
        .columns
        .iter()
        .map(|c| r.dialect.quote_identifier(c))
        .collect();
    r.sql.push_str(&quoted.join(", "));
    r.sql.push(')');

    match &on_conflict.action {
        ConflictAction::DoNothing => r.sql.push_str(" DO NOTHING"),
        ConflictAction::DoUpdate { assignments } => {
            if assignments.is_empty() {
                r.sql.push_str(" DO NOTHING");
                return;
            }

            r.sql.push_str(" DO UPDATE SET ");
            for (i, assignment) in assignments.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                r.sql
                    .push_str(&r.dialect.quote_identifier(&assignment.column));
                r.sql.push_str(" = ");
                assignment.value.render(r);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use model::core::value::Value;

    use crate::{
        ast::{
            common::TableRef,
            expr::{Expr, Ident},
            insert::{ConflictAction, ConflictAssignment, Insert, OnConflict},
            select::{FromClause, Select, TableSource},
        },
        dialect::{MySql, Postgres},
        renderer::{Render, Renderer},
    };

    fn value(val: Value) -> Expr {
        Expr::Value(val)
    }

    #[test]
    fn test_render_batch_insert_postgres() {
        let ast = Insert {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec!["name".to_string(), "is_active".to_string()],
            values: vec![
                vec![
                    value(Value::String("Alice".to_string())),
                    value(Value::Boolean(true)),
                ],
                vec![
                    value(Value::String("Bob".to_string())),
                    value(Value::Boolean(false)),
                ],
            ],
            select: None,
            on_conflict: None,
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql =
            r#"INSERT INTO "users" ("name", "is_active") VALUES ($1, $2), ($3, $4);"#;
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("Alice".to_string()),
                Value::Boolean(true),
                Value::String("Bob".to_string()),
                Value::Boolean(false)
            ]
        );
    }

    #[test]
    fn test_render_batch_insert_mysql() {
        let ast = Insert {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec!["name".to_string(), "is_active".to_string()],
            values: vec![
                vec![
                    value(Value::String("Alice".to_string())),
                    value(Value::Boolean(true)),
                ],
                vec![
                    value(Value::String("Bob".to_string())),
                    value(Value::Boolean(false)),
                ],
            ],
            select: None,
            on_conflict: None,
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql = "INSERT INTO `users` (`name`, `is_active`) VALUES (?, ?), (?, ?);";
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("Alice".to_string()),
                Value::Boolean(true),
                Value::String("Bob".to_string()),
                Value::Boolean(false)
            ]
        );
    }

    #[test]
    fn test_render_insert_select_on_conflict() {
        let select = Select {
            columns: vec![
                Expr::Identifier(Ident {
                    qualifier: Some("s".to_string()),
                    name: "id".to_string(),
                }),
                Expr::Identifier(Ident {
                    qualifier: Some("s".to_string()),
                    name: "name".to_string(),
                }),
            ],
            from: Some(FromClause {
                table: TableSource::Table(TableRef {
                    schema: None,
                    name: "users_stage".to_string(),
                }),
                alias: Some("s".to_string()),
            }),
            ..Default::default()
        };

        let ast = Insert {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![],
            select: Some(select),
            on_conflict: Some(OnConflict {
                columns: vec!["id".to_string()],
                action: ConflictAction::DoUpdate {
                    assignments: vec![ConflictAssignment {
                        column: "name".to_string(),
                        value: Expr::Literal("EXCLUDED.\"name\"".to_string()),
                    }],
                },
            }),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            concat!(
                "INSERT INTO \"users\" (\"id\", \"name\") ",
                "SELECT \"s\".\"id\", \"s\".\"name\" FROM \"users_stage\" AS \"s\" ",
                "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\";"
            )
        );
        assert!(params.is_empty());
    }
}
