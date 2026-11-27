use model::{
    core::{convert::IntoCanonical, types::Type, value::Value},
    records::Record,
};
use query_builder::{
    ast::{
        common::TypeName,
        copy::{CopyDirection, CopyEndpoint},
        create_index::IndexColumnExpr,
        expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
        insert::{ConflictAction, ConflictAssignment, Insert, OnConflict},
        load_data::LoadDataConflict,
        merge::MergeAssignment,
    },
    builder::{
        alter_table::AlterTableBuilder, copy::CopyBuilder, create_enum::CreateEnumBuilder,
        create_index::CreateIndexBuilder, create_sequence::CreateSequenceBuilder,
        create_table::CreateTableBuilder, drop_table::DropTableBuilder, insert::InsertBuilder,
        load_data::LoadDataBuilder, merge::MergeBuilder, select::SelectBuilder,
        truncate_table::TruncateTableBuilder,
    },
    dialect::QueryDialect,
    renderer::{Render, Renderer},
};
use query_builder::{table_ref, value};
use std::collections::HashSet;

use crate::{
    add_joins, add_where, ident, join_on_expr,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::{
            coercion::coerce_value,
            column::ColumnDef,
            constraint::{CheckConstraintDef, UniqueConstraintDef},
            fk::ForeignKeyDef,
            index::IndexDef,
            sequence::SequenceDef,
        },
        request::FetchRowsRequest,
    },
    sql_filter_expr,
};

pub struct QueryGenerator<'a> {
    dialect: &'a dyn QueryDialect,
}

impl<'a> QueryGenerator<'a> {
    pub fn new(dialect: &'a dyn QueryDialect) -> Self {
        Self { dialect }
    }

    /// Generates a SQL SELECT statement and its parameters.
    pub fn select(&self, request: &FetchRowsRequest) -> (String, Vec<Value>) {
        let alias = request.alias.as_deref().unwrap_or(&request.table);
        let table = table_ref!(&request.table);

        let columns = request
            .columns
            .iter()
            .map(|c| ident!(c))
            .collect::<Vec<_>>();

        // Start building the query
        let mut select = SelectBuilder::new()
            .select(columns)
            .from(table, Some(alias));

        // Apply joins and where clause
        select = add_joins!(select, &request.joins);
        select = add_where!(select, &request.filter);

        // Apply IN clause if specified
        if let Some((ref column, ref values)) = request.in_clause {
            let in_expr = Expr::Identifier(Ident {
                qualifier: Some(alias.to_string()),
                name: column.clone(),
            })
            .in_list(values.iter().map(|v| Expr::Value(v.clone())).collect());

            // Combine with existing filter using AND
            select = match select.ast.where_clause.take() {
                Some(existing) => select.where_clause(Expr::BinaryOp(Box::new(BinaryOp {
                    left: existing,
                    op: BinaryOperator::And,
                    right: in_expr,
                }))),
                None => select.where_clause(in_expr),
            };
        }

        // Apply key ranges if specified: (col >= lo AND col <= hi) OR ...
        if let Some((ref column, ref ranges)) = request.key_ranges
            && !ranges.is_empty()
        {
            let col = || {
                Expr::Identifier(Ident {
                    qualifier: Some(alias.to_string()),
                    name: column.clone(),
                })
            };
            let bound = |left: Expr, op: BinaryOperator, value: &Value| {
                Expr::BinaryOp(Box::new(BinaryOp {
                    left,
                    op,
                    right: Expr::Value(value.clone()),
                }))
            };
            let both = |left: Expr, right: Expr, op: BinaryOperator| {
                Expr::BinaryOp(Box::new(BinaryOp { left, op, right }))
            };

            let ranges_expr = ranges
                .iter()
                .map(|(lo, hi)| {
                    both(
                        bound(col(), BinaryOperator::GtEq, lo),
                        bound(col(), BinaryOperator::LtEq, hi),
                        BinaryOperator::And,
                    )
                })
                .reduce(|acc, range| both(acc, range, BinaryOperator::Or));

            if let Some(ranges_expr) = ranges_expr {
                select = match select.ast.where_clause.take() {
                    Some(existing) => {
                        select.where_clause(both(existing, ranges_expr, BinaryOperator::And))
                    }
                    None => select.where_clause(ranges_expr),
                };
            }
        }

        // Apply random ordering if requested
        if request.order_random {
            select = select.order_by_random();
        }

        // Build the final AST
        // Note: When using random ordering, we skip pagination to avoid adding ORDER BY clauses
        // that would conflict with ORDER BY RANDOM()
        let limit = i64::try_from(request.limit).unwrap_or(i64::MAX);
        let select_ast = if request.order_random {
            select.limit(value!(Value::Int(limit))).build()
        } else {
            select
                .limit(value!(Value::Int(limit)))
                .paginate(request.strategy.clone(), &request.cursor, request.limit)
                .build()
        };

        self.render_ast(select_ast)
    }

    /// `SELECT MIN(col) AS lo, MAX(col) AS hi FROM table` - the range probe used
    /// to split an integer key into parallel scan lanes.
    pub fn select_key_range(&self, table: &str, column: &str) -> (String, Vec<Value>) {
        let lo = Expr::FunctionCall(FunctionCall::min(Expr::column(column))).alias("lo");
        let hi = Expr::FunctionCall(FunctionCall::max(Expr::column(column))).alias("hi");
        let select_ast = SelectBuilder::new()
            .select(vec![lo, hi])
            .from(table_ref!(table), None)
            .build();

        self.render_ast(select_ast)
    }

    pub fn insert_batch<T>(
        &self,
        meta: &TableMetadata,
        rows: &[Record],
        type_converter: &T,
    ) -> (String, Vec<Value>)
    where
        T: IntoCanonical<ColumnMeta = ColumnMetadata>,
    {
        if rows.is_empty() {
            return (String::new(), Vec::new());
        }

        // Sorting ensures the column order is always consistent; exclude generated columns
        let mut sorted_columns: Vec<_> = meta
            .columns
            .values()
            .filter(|col| !col.is_generated)
            .collect();
        sorted_columns.sort_by_key(|col| col.ordinal);

        // Only write destination columns the rows actually carry.
        sorted_columns.retain(|col| rows[0].index_of(&col.name).is_some());

        if sorted_columns.is_empty() {
            return (String::new(), Vec::new());
        }

        let col_names = sorted_columns
            .iter()
            .map(|col| col.name.clone())
            .collect::<Vec<_>>();

        let mut builder = InsertBuilder::new(table_ref!(meta.name))
            .columns(&col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        // Per-column canonical type.
        let col_plan: Vec<(&ColumnMetadata, _)> = sorted_columns
            .iter()
            .map(|col_meta| {
                let canonical = type_converter.to_canonical(col_meta).canonical;
                (*col_meta, canonical)
            })
            .collect();

        for row in rows.iter() {
            let ordered_values: Vec<Expr> = col_plan
                .iter()
                .map(|(col_meta, data_type)| {
                    let value = row.value(&col_meta.name).cloned().unwrap_or(Value::Null);
                    map_value_to_expr(value, col_meta, data_type, self.dialect)
                })
                .collect();

            builder = builder.values(ordered_values);
        }

        self.render_ast(builder.build())
    }

    pub fn copy_from_stdin_text(&self, table: &str, columns: &[ColumnMetadata]) -> String {
        self.copy_from_stdin_with(table, columns, "csv, NULL '\\N'")
    }

    /// `COPY ... FROM STDIN WITH (FORMAT binary)` for the binary COPY fast path.
    pub fn copy_from_stdin_binary(&self, table: &str, columns: &[ColumnMetadata]) -> String {
        self.copy_from_stdin_with(table, columns, "binary")
    }

    fn copy_from_stdin_with(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        format: &str,
    ) -> String {
        let column_names = columns
            .iter()
            .filter(|col| !col.is_generated)
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>();

        let copy_ast = CopyBuilder::new(table_ref!(table))
            .columns(&column_names)
            .direction(CopyDirection::From)
            .endpoint(CopyEndpoint::Stdin)
            .option("FORMAT", Some(format))
            .build();

        let (sql, _) = self.render_ast(copy_ast);
        sql
    }

    pub fn load_data_infile(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        on_conflict: LoadDataConflict,
    ) -> String {
        let column_names = columns
            .iter()
            .filter(|col| !col.is_generated)
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>();

        let load_ast = LoadDataBuilder::new(table_ref!(table))
            .columns(&column_names)
            .on_conflict(on_conflict)
            .build();

        let (sql, _) = self.render_ast(load_ast);
        sql
    }

    pub fn merge_from_staging(
        &self,
        meta: &TableMetadata,
        staging: &str,
        columns: &[ColumnMetadata],
        do_update: bool,
    ) -> (String, Vec<Value>) {
        let target_ref = table_ref!(meta.name);
        let staging_ref = table_ref!(staging);
        let target_alias = "t";
        let staging_alias = "s";

        let pk_set = primary_key_set(meta);

        let mut builder = MergeBuilder::new(target_ref, staging_ref)
            .target_alias(target_alias)
            .source_alias(staging_alias)
            .on(build_pk_match_expr(meta, target_alias, staging_alias));

        let assignments: Vec<MergeAssignment> = columns
            .iter()
            .filter(|col| !pk_set.contains(&col.name.to_lowercase()))
            .map(|col| MergeAssignment {
                column: col.name.clone(),
                value: aliased_ident(staging_alias, &col.name),
            })
            .collect();

        builder = if !do_update || assignments.is_empty() {
            builder.when_matched_do_nothing()
        } else {
            builder.when_matched_update(assignments)
        };

        let insert_columns = columns.iter().map(|c| c.name.clone()).collect();
        let insert_values = columns
            .iter()
            .map(|c| aliased_ident(staging_alias, &c.name))
            .collect();

        builder = builder.when_not_matched_insert(insert_columns, insert_values);

        self.render_ast(builder.build())
    }

    pub fn upsert_from_staging(
        &self,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &[ColumnMetadata],
        do_update: bool,
    ) -> (String, Vec<Value>) {
        let pk_set = primary_key_set(meta);
        let staging_alias = "s";
        let select_columns = columns
            .iter()
            .map(|col| aliased_ident(staging_alias, &col.name))
            .collect();

        let select_ast = SelectBuilder::new()
            .select(select_columns)
            .from(table_ref!(staging_table), Some(staging_alias))
            .build();

        let conflict_clause = if meta.primary_keys.is_empty() {
            None
        } else {
            Some(OnConflict {
                columns: meta.primary_keys.clone(),
                action: if do_update {
                    self.build_conflict_action(columns, &pk_set)
                } else {
                    ConflictAction::DoNothing
                },
            })
        };

        let insert_ast = Insert {
            table: table_ref!(meta.name),
            columns: columns.iter().map(|c| c.name.clone()).collect(),
            values: vec![],
            select: Some(select_ast),
            on_conflict: conflict_clause,
        };

        self.render_ast(insert_ast)
    }

    pub fn toggle_triggers(&self, table: &str, enable: bool) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table)).toggle_triggers(enable);
        self.render_ast(builder.build())
    }

    pub fn add_column(&self, table: &str, column: ColumnDef) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table));
        let query_ast = builder
            .add_column(&column.name, column.data_type)
            .add()
            .build();

        self.render_ast(query_ast)
    }

    pub fn create_table(
        &self,
        table: &str,
        columns: &[ColumnDef],
        skip_primary_key: bool,
        temp: bool,
    ) -> (String, Vec<Value>) {
        let primary_keys: Vec<String> = if skip_primary_key {
            Vec::new()
        } else {
            columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect()
        };

        // MySQL rejects AUTO_INCREMENT on a column that belongs to no key (1075),
        // which is exactly what `skip_primary_key` produces. Drop the attribute
        // rather than emit invalid DDL. PostgreSQL's SERIAL needs no key.
        let drop_auto_inc = primary_keys.is_empty() && self.dialect.auto_inc_requires_key();

        let mut initial_builder = CreateTableBuilder::new(table_ref!(table));
        if temp {
            initial_builder = initial_builder.temporary();
        }

        let builder_with_cols = columns.iter().fold(initial_builder, |builder, col| {
            let mut data_type = col.data_type.clone();

            if drop_auto_inc && let Type::Int { auto_increment, .. } = &mut data_type {
                *auto_increment = false;
            }

            let mut col_builder = builder.column(&col.name, data_type, col.char_max_length);

            // Only add PRIMARY KEY to the column definition if it's the *only* primary key.
            if primary_keys.len() == 1 && primary_keys[0] == col.name.as_str() {
                col_builder = col_builder.primary_key();
            }
            if col.is_nullable {
                col_builder = col_builder.nullable();
            }
            if let Some(default_val) = &col.default {
                // Serial types (serial, bigserial, smallserial) already imply a
                // DEFAULT nextval(...) - adding an explicit DEFAULT is a conflict.
                if !col.data_type.is_auto_increment() {
                    col_builder = col_builder.default_value(Expr::Literal(default_val.clone()));
                }
            }
            if let Some(expr) = &col.generated_expression {
                col_builder = col_builder.generated(expr, col.is_stored);
            }

            col_builder.add() // .add() returns the CreateTableBuilder for the next fold iteration
        });

        // Add the composite primary key constraint at the table level if necessary
        let final_builder = if primary_keys.len() > 1 {
            builder_with_cols.primary_key(primary_keys)
        } else {
            builder_with_cols
        };

        self.render_ast(final_builder.build())
    }

    pub fn drop_table(&self, table: &str, if_exists: bool) -> (String, Vec<Value>) {
        let mut builder = DropTableBuilder::new(table_ref!(table));
        if if_exists {
            builder = builder.if_exists();
        }
        self.render_ast(builder.build())
    }

    pub fn truncate_table(&self, table: &str) -> (String, Vec<Value>) {
        self.render_ast(TruncateTableBuilder::new(table_ref!(table)).build())
    }

    /// `ALTER TABLE … ADD PRIMARY KEY (cols)` for the given table.
    pub fn add_primary_key(&self, table: &str, columns: &[String]) -> (String, Vec<Value>) {
        let ast = AlterTableBuilder::new(table_ref!(table))
            .add_primary_key(columns)
            .build();
        self.render_ast(ast)
    }

    /// Dialect specific DDL to drop a table's primary key.
    pub fn drop_primary_key(&self, table: &str) -> String {
        self.dialect.drop_primary_key(table)
    }

    pub fn add_foreign_key(
        &self,
        table: &str,
        foreign_key: &ForeignKeyDef,
    ) -> (String, Vec<Value>) {
        let columns: Vec<&str> = foreign_key.columns.iter().map(|s| s.as_str()).collect();
        let referenced_columns: Vec<&str> = foreign_key
            .referenced_columns
            .iter()
            .map(|s| s.as_str())
            .collect();

        let on_delete = foreign_key.on_delete.to_string();
        let on_update = foreign_key.on_update.to_string();

        let query_ast = AlterTableBuilder::new(table_ref!(table))
            .add_named_foreign_key(
                foreign_key.constraint_name.clone(),
                &columns,
                table_ref!(&foreign_key.referenced_table),
                &referenced_columns,
                on_delete,
                on_update,
            )
            .build();

        self.render_ast(query_ast)
    }

    pub fn drop_foreign_key(&self, table: &str, constraint_name: &str) -> String {
        self.dialect.drop_foreign_key(table, constraint_name)
    }

    pub fn create_enum(&self, name: &str, values: &[String]) -> (String, Vec<Value>) {
        let builder = CreateEnumBuilder::new(
            TypeName {
                schema: None,
                name: name.to_string(),
            },
            values,
        );
        self.render_ast(builder.build())
    }

    pub fn create_index(&self, index: &IndexDef) -> (String, Vec<Value>) {
        let columns: Vec<IndexColumnExpr> = index
            .columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let sort_order = col.sort_order.to_string();
                let nulls = col.nulls_order.to_string();

                IndexColumnExpr {
                    expr: name,
                    sort_order,
                    nulls,
                    prefix_length: col.prefix_length,
                }
            })
            .collect();

        let mut builder = CreateIndexBuilder::new(&index.name, table_ref!(&index.table))
            .columns(columns)
            .if_not_exists();

        if index.unique {
            builder = builder.unique();
        }
        if let Some(idx_type) = &index.index_type {
            builder = builder.using(idx_type.as_str());
        }
        if let Some(condition) = &index.condition {
            builder = builder.condition(condition.clone());
        }

        self.render_ast(builder.build())
    }

    pub fn create_sequence(&self, seq: &SequenceDef) -> (String, Vec<Value>) {
        let mut builder = CreateSequenceBuilder::new(&seq.name).if_not_exists();

        if let Some(start) = seq.start {
            builder = builder.start(start);
        }
        if let Some(inc) = seq.increment {
            builder = builder.increment(inc);
        }
        if let Some(min) = seq.min_value {
            builder = builder.min_value(min);
        }
        if let Some(max) = seq.max_value {
            builder = builder.max_value(max);
        }
        if let Some((ref table, ref column)) = seq.owned_by {
            builder = builder.owned_by(table, column);
        }

        self.render_ast(builder.build())
    }

    pub fn add_unique_constraint(
        &self,
        table: &str,
        constraint: &UniqueConstraintDef,
    ) -> (String, Vec<Value>) {
        let cols: Vec<&str> = constraint.columns.iter().map(|s| s.as_str()).collect();
        let ast = AlterTableBuilder::new(table_ref!(table))
            .add_unique(constraint.constraint_name.clone(), &cols)
            .build();
        self.render_ast(ast)
    }

    pub fn add_check_constraint(
        &self,
        table: &str,
        constraint: &CheckConstraintDef,
    ) -> (String, Vec<Value>) {
        let ast = AlterTableBuilder::new(table_ref!(table))
            .add_check(
                constraint.constraint_name.clone(),
                constraint.expression.clone(),
            )
            .build();
        self.render_ast(ast)
    }

    pub fn key_existence(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        self.dialect
            .key_existence_query(table_name, key_columns, keys_batch)
    }

    /// Generates a validation estimation query that counts failures and total rows
    ///
    /// Returns a query that selects:
    /// - `failures`: Count of rows that fail the validation (NOT validation_expr)
    /// - `total`: Total count of rows sampled
    ///
    /// For PostgreSQL, uses: `COUNT(*) FILTER (WHERE NOT (validation_expr))`
    /// For MySQL, uses: `SUM(CASE WHEN NOT (validation_expr) THEN 1 ELSE 0 END)`
    ///
    /// # Arguments
    /// - `request`: FetchRowsRequest containing table, alias, and joins (filters are NOT applied)
    /// - `validation_expr`: The validation expression to check
    /// - `sample_size`: Maximum number of rows to sample for estimation
    ///
    /// # Note
    /// This function applies joins but NOT filters from the request.
    ///
    /// **Why include joins?** Validation expressions can reference columns from joined tables
    /// (e.g., `table_a.col1 == 1 && table_b.col2 == 2`). Without the joins, those columns
    /// won't exist and the query will fail.
    ///
    /// **Why no filters?** Filters select a subset of data. We want to measure the quality of
    /// the entire source table (with its joins), not just the filtered portion, to get an
    /// unbiased estimate.
    ///
    /// # Example
    /// ```sql
    /// -- PostgreSQL output (with joins):
    /// SELECT
    ///   COUNT(*) FILTER (WHERE NOT ((table_a.age >= $1 AND table_b.verified = $2))) AS failures,
    ///   COUNT(*) AS total
    /// FROM users AS table_a
    /// LEFT JOIN profiles AS table_b ON table_a.id = table_b.user_id
    /// LIMIT $3
    ///
    /// -- MySQL output (without joins):
    /// SELECT
    ///   SUM(CASE WHEN NOT ((age >= ?)) THEN ? ELSE ? END) AS failures,
    ///   COUNT(*) AS total
    /// FROM users
    /// LIMIT ?
    /// ```
    pub fn validation_estimation(
        &self,
        request: &FetchRowsRequest,
        validation_expr: Expr,
        sample_size: usize,
    ) -> (String, Vec<Value>) {
        let alias = request.alias.as_deref().unwrap_or(&request.table);
        let table = table_ref!(&request.table);

        // Build the NOT expression for failed validations
        let not_validation = Expr::not(validation_expr);

        let is_pg = self.dialect.name().contains("PostgreSQL");

        // Build failure count expression based on database
        let failures_expr = if is_pg {
            // PostgreSQL: COUNT(*) FILTER (WHERE NOT (validation))
            FunctionCall::count_all()
                .with_filter(not_validation)
                .alias("failures")
        } else {
            // MySQL: SUM(CASE WHEN NOT (validation) THEN 1 ELSE 0 END)
            let case_expr = Expr::case_when(
                not_validation,
                Expr::Value(Value::Int(1)),
                Some(Expr::Value(Value::Int(0))),
            );
            Expr::FunctionCall(FunctionCall::sum(case_expr)).alias("failures")
        };

        // Build total count expression
        let total_expr = Expr::FunctionCall(FunctionCall::count_all()).alias("total");

        // Build the SELECT query with joins but NO filters
        let mut select = SelectBuilder::new()
            .select(vec![failures_expr, total_expr])
            .from(table, Some(alias));

        // Apply joins so validation expressions can reference joined table columns
        select = add_joins!(select, &request.joins);

        // Add sample size limit
        let select_ast = select.limit(value!(Value::Int(sample_size as i64))).build();

        self.render_ast(select_ast)
    }

    fn render_ast(&self, ast: impl Render) -> (String, Vec<Value>) {
        let mut renderer = Renderer::new(self.dialect);
        ast.render(&mut renderer);
        renderer.finish()
    }

    fn build_conflict_action(
        &self,
        columns: &[ColumnMetadata],
        pk_set: &HashSet<String>,
    ) -> ConflictAction {
        let assignments: Vec<ConflictAssignment> = columns
            .iter()
            .filter(|col| !pk_set.contains(&col.name.to_lowercase()))
            .map(|col| ConflictAssignment {
                column: col.name.clone(),
                value: self.excluded_column_expr(&col.name),
            })
            .collect();

        if assignments.is_empty() {
            ConflictAction::DoNothing
        } else {
            ConflictAction::DoUpdate { assignments }
        }
    }

    fn excluded_column_expr(&self, column: &str) -> Expr {
        Expr::Literal(format!(
            "EXCLUDED.{}",
            self.dialect.quote_identifier(column)
        ))
    }
}

/// Maps a `Value` to a query `Expr` based on column metadata and canonical type.
///
/// This function contains all the specific logic for handling different data types,
/// like casting enums or parsing string representations of arrays.
fn map_value_to_expr(
    value: Value,
    col_meta: &ColumnMetadata,
    data_type: &Type,
    dialect: &dyn QueryDialect,
) -> Expr {
    // For NULL, PostgreSQL casts so it can infer the column type in ambiguous
    // contexts (e.g. bytea). MySQL's CAST rejects type names like `varchar` /
    // `text` / `float`, and a bare NULL is unambiguous under an explicit column
    // list - so it emits a plain NULL.
    if let Value::Null = value {
        let null_lit = Expr::Literal("NULL".to_string());
        return if dialect.cast_null_literals() {
            Expr::Cast {
                expr: Box::new(null_lit),
                data_type: col_meta.data_type.clone(), // raw destination type string
            }
        } else {
            null_lit
        };
    }

    let coerced_value = coerce_value(value, data_type);

    // Handle type-specific expression wrapping
    match data_type {
        // For set/array types, parse comma-separated string if needed
        Type::Set { .. } | Type::Array { .. } => {
            let string_array: Vec<String> = match coerced_value {
                Value::String(s) => s.split(',').map(|item| item.trim().to_string()).collect(),
                Value::Set(arr) => arr,
                _ => vec![],
            };
            Expr::Value(Value::Set(string_array))
        }

        Type::Enum { name, .. } => {
            let base_expr = match coerced_value {
                Value::Enum { value: v, .. } => Expr::Value(Value::String(v)),
                Value::String(s) => Expr::Value(Value::String(s)),
                other => Expr::Value(other),
            };

            // MySQL enum columns accept the plain string variant directly.
            // PostgreSQL needs the value cast into its named enum type
            // (`$1::TEXT::enum_type`).
            if !dialect.supports_enums() {
                return base_expr;
            }

            let enum_type_name = if name.is_empty() {
                col_meta.name.clone() // Fallback to column name
            } else {
                name.clone()
            };

            Expr::Cast {
                data_type: enum_type_name,
                expr: Box::new(Expr::Cast {
                    expr: Box::new(base_expr),
                    data_type: "TEXT".into(),
                }),
            }
        }

        // For all other types, just use the value directly
        _ => Expr::Value(coerced_value),
    }
}

fn primary_key_set(meta: &TableMetadata) -> HashSet<String> {
    meta.primary_keys
        .iter()
        .map(|pk| pk.to_lowercase())
        .collect()
}

fn aliased_ident(alias: &str, column: &str) -> Expr {
    Expr::Identifier(Ident {
        qualifier: Some(alias.to_string()),
        name: column.to_string(),
    })
}

fn build_pk_match_expr(meta: &TableMetadata, target_alias: &str, source_alias: &str) -> Expr {
    let mut pk_iter = meta.primary_keys.iter().map(|pk| {
        Expr::BinaryOp(Box::new(BinaryOp {
            left: aliased_ident(target_alias, pk),
            op: BinaryOperator::Eq,
            right: aliased_ident(source_alias, pk),
        }))
    });

    let first = match pk_iter.next() {
        Some(expr) => expr,
        None => Expr::Literal("TRUE".to_string()),
    };

    pk_iter.fold(first, |acc, expr| {
        Expr::BinaryOp(Box::new(BinaryOp {
            left: acc,
            op: BinaryOperator::And,
            right: expr,
        }))
    })
}

/// Maximum rows one parameterized INSERT may carry without exceeding the
/// backend's prepared-statement placeholder limit.
pub fn max_rows_per_insert(meta: &TableMetadata, max_params: usize) -> usize {
    let cols = meta
        .columns
        .values()
        .filter(|c| !c.is_generated)
        .count()
        .max(1);
    (max_params / cols).max(1)
}

#[cfg(test)]
mod tests {
    use crate::sql::{metadata::fk::ForeignKeyAction, request::FetchRowsRequestBuilder};

    use super::*;
    use model::pagination::cursor::Cursor;
    use query_builder::{
        dialect::{MySql, Postgres},
        offsets::DefaultOffset,
    };
    use std::sync::Arc;

    #[test]
    fn max_rows_per_insert_respects_placeholder_limit() {
        use crate::sql::metadata::{column::ColumnMetadata, table::TableMetadata};
        use std::collections::HashMap;

        let mut columns = HashMap::new();
        for i in 0..15 {
            columns.insert(
                format!("c{i}"),
                ColumnMetadata {
                    name: format!("c{i}"),
                    ..Default::default()
                },
            );
        }
        // A generated column must NOT count toward the placeholder budget
        // (insert_batch excludes it from the statement).
        columns.insert(
            "gen".to_string(),
            ColumnMetadata {
                name: "gen".to_string(),
                is_generated: true,
                ..Default::default()
            },
        );

        let meta = TableMetadata {
            name: "orders".to_string(),
            schema: None,
            columns,
            primary_keys: vec![],
            foreign_keys: vec![],
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        };

        // 15 non-generated columns, 65535 placeholders -> 4369 rows per statement.
        assert_eq!(max_rows_per_insert(&meta, 65535), 65535 / 15);
        // Never zero, even when a single row's columns exceed the budget.
        assert_eq!(max_rows_per_insert(&meta, 5), 1);
    }

    #[test]
    fn test_validation_estimation_postgres_simple() {
        let generator = QueryGenerator::new(&Postgres);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("users".to_string())
            .limit(10000)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .build();

        // Validation: age >= 18
        let validation_expr = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let (sql, params) = generator.validation_estimation(&request, validation_expr, 10000);

        assert_eq!(
            sql,
            "SELECT COUNT(*) FILTER (WHERE NOT ((\"age\" >= $1))) AS \"failures\", COUNT(*) AS \"total\" FROM \"users\" AS \"users\" LIMIT $2"
        );
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], Value::Int(18));
        assert_eq!(params[1], Value::Int(10000));
    }

    #[test]
    fn test_validation_estimation_mysql_simple() {
        let generator = QueryGenerator::new(&MySql);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("users".to_string())
            .limit(10000)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .build();

        // Validation: age >= 18
        let validation_expr = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let (sql, params) = generator.validation_estimation(&request, validation_expr, 10000);

        assert_eq!(
            sql,
            "SELECT SUM(CASE WHEN NOT ((`age` >= ?)) THEN ? ELSE ? END) AS `failures`, COUNT(*) AS `total` FROM `users` AS `users` LIMIT ?"
        );
        assert_eq!(params.len(), 4);
        assert_eq!(params[0], Value::Int(18));
        assert_eq!(params[1], Value::Int(1));
        assert_eq!(params[2], Value::Int(0));
        assert_eq!(params[3], Value::Int(10000));
    }

    #[test]
    fn test_validation_estimation_postgres_complex() {
        let generator = QueryGenerator::new(&Postgres);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("users".to_string())
            .limit(5000)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .build();

        // Validation: age >= 18 AND verified = true
        let age_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let verified_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "verified".to_string(),
            }),
            op: BinaryOperator::Eq,
            right: Expr::Value(Value::Boolean(true)),
        }));

        let validation_expr = Expr::BinaryOp(Box::new(BinaryOp {
            left: age_check,
            op: BinaryOperator::And,
            right: verified_check,
        }));

        let (sql, params) = generator.validation_estimation(&request, validation_expr, 5000);

        assert_eq!(
            sql,
            "SELECT COUNT(*) FILTER (WHERE NOT (((\"age\" >= $1) AND (\"verified\" = $2)))) AS \"failures\", COUNT(*) AS \"total\" FROM \"users\" AS \"users\" LIMIT $3"
        );
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], Value::Int(18));
        assert_eq!(params[1], Value::Boolean(true));
        assert_eq!(params[2], Value::Int(5000));
    }

    #[test]
    fn test_validation_estimation_mysql_complex() {
        let generator = QueryGenerator::new(&MySql);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("users".to_string())
            .limit(5000)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .build();

        // Validation: age >= 18 AND verified = true
        let age_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let verified_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "verified".to_string(),
            }),
            op: BinaryOperator::Eq,
            right: Expr::Value(Value::Boolean(true)),
        }));

        let validation_expr = Expr::BinaryOp(Box::new(BinaryOp {
            left: age_check,
            op: BinaryOperator::And,
            right: verified_check,
        }));

        let (sql, params) = generator.validation_estimation(&request, validation_expr, 5000);

        assert_eq!(
            sql,
            "SELECT SUM(CASE WHEN NOT (((`age` >= ?) AND (`verified` = ?))) THEN ? ELSE ? END) AS `failures`, COUNT(*) AS `total` FROM `users` AS `users` LIMIT ?"
        );
        assert_eq!(params.len(), 5);
        assert_eq!(params[0], Value::Int(18));
        assert_eq!(params[1], Value::Boolean(true));
        assert_eq!(params[2], Value::Int(1));
        assert_eq!(params[3], Value::Int(0));
        assert_eq!(params[4], Value::Int(5000));
    }

    #[test]
    fn test_select_with_in_clause_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("u".to_string())
            .limit(10)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .in_clause(
                "id".to_string(),
                vec![Value::Int(1), Value::Int(5), Value::Int(10)],
            )
            .build();

        let (sql, params) = generator.select(&request);

        assert!(sql.contains(r#""u"."id" IN ($1, $2, $3)"#));
        assert!(sql.contains("LIMIT $4"));
        assert_eq!(params[0], Value::Int(1));
        assert_eq!(params[1], Value::Int(5));
        assert_eq!(params[2], Value::Int(10));
        assert_eq!(params[3], Value::Int(10));
    }

    #[test]
    fn test_select_with_in_clause_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let request = FetchRowsRequestBuilder::new("products".to_string())
            .alias("p".to_string())
            .limit(20)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .in_clause(
                "status".to_string(),
                vec![
                    Value::String("active".to_string()),
                    Value::String("pending".to_string()),
                ],
            )
            .build();

        let (sql, params) = generator.select(&request);

        assert!(sql.contains("`p`.`status` IN (?, ?)"));
        assert!(sql.contains("LIMIT ?"));
        assert_eq!(params[0], Value::String("active".to_string()));
        assert_eq!(params[1], Value::String("pending".to_string()));
        assert_eq!(params[2], Value::Int(20));
    }

    #[test]
    fn test_select_with_random_order_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("users".to_string())
            .limit(5)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .order_random(true)
            .build();

        let (sql, params) = generator.select(&request);

        assert!(sql.contains("ORDER BY RANDOM()"));
        assert!(sql.contains("LIMIT"));
        // Should not have additional ORDER BY for pagination (only RANDOM)
        assert!(!sql.contains("ASC"));
        assert!(!sql.contains("DESC"));
        // When using random ordering, pagination is skipped, so only LIMIT param (as Int)
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], Value::Int(5));
    }

    #[test]
    fn test_select_with_random_order_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let request = FetchRowsRequestBuilder::new("posts".to_string())
            .alias("posts".to_string())
            .limit(10)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .order_random(true)
            .build();

        let (sql, params) = generator.select(&request);

        assert!(sql.contains("ORDER BY RAND()"));
        assert!(sql.contains("LIMIT"));
        // Should not have additional ORDER BY for pagination (only RAND)
        assert!(!sql.contains("ASC"));
        assert!(!sql.contains("DESC"));
        // When using random ordering, pagination is skipped, so only LIMIT param (as Int)
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], Value::Int(10));
    }

    #[test]
    fn test_select_with_in_clause_and_random_order_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let request = FetchRowsRequestBuilder::new("users".to_string())
            .alias("u".to_string())
            .limit(3)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .in_clause(
                "id".to_string(),
                vec![
                    Value::Int(10),
                    Value::Int(20),
                    Value::Int(30),
                    Value::Int(40),
                    Value::Int(50),
                ],
            )
            .order_random(true)
            .build();

        let (sql, params) = generator.select(&request);

        // Should have both IN clause and random ordering
        assert!(sql.contains(r#""u"."id" IN ($1, $2, $3, $4, $5)"#));
        assert!(sql.contains("ORDER BY RANDOM()"));
        assert!(sql.contains("LIMIT"));
        // Should not have additional ORDER BY for pagination
        assert!(!sql.contains("ASC"));
        assert!(!sql.contains("DESC"));
        // IN values (5) + limit (1) = 6 params (no offset when using random ordering)
        assert_eq!(params.len(), 6);
        assert_eq!(params[0], Value::Int(10));
        assert_eq!(params[1], Value::Int(20));
        assert_eq!(params[2], Value::Int(30));
        assert_eq!(params[3], Value::Int(40));
        assert_eq!(params[4], Value::Int(50));
        assert_eq!(params[5], Value::Int(3)); // limit
    }

    #[test]
    fn test_select_with_in_clause_and_random_order_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let request = FetchRowsRequestBuilder::new("posts".to_string())
            .alias("p".to_string())
            .limit(5)
            .cursor(Cursor::Default { offset: 0 })
            .strategy(Arc::new(DefaultOffset { offset: 0 }))
            .in_clause(
                "category".to_string(),
                vec![
                    Value::String("tech".to_string()),
                    Value::String("news".to_string()),
                ],
            )
            .order_random(true)
            .build();

        let (sql, params) = generator.select(&request);

        // Should have both IN clause and random ordering
        assert!(sql.contains("`p`.`category` IN (?, ?)"));
        assert!(sql.contains("ORDER BY RAND()"));
        assert!(sql.contains("LIMIT"));
        // Should not have additional ORDER BY for pagination
        assert!(!sql.contains("ASC"));
        assert!(!sql.contains("DESC"));
        // IN values (2) + limit (1) = 3 params (no offset when using random ordering)
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], Value::String("tech".to_string()));
        assert_eq!(params[1], Value::String("news".to_string()));
        assert_eq!(params[2], Value::Int(5)); // limit
    }

    #[test]
    fn test_add_foreign_key_single_column_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let fk = ForeignKeyDef {
            constraint_name: None,
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
            on_update: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
        };

        let (sql, _) = generator.add_foreign_key("orders", &fk);

        assert!(sql.contains(r#"ALTER TABLE "orders""#));
        assert!(sql.contains(r#"ADD FOREIGN KEY"#));
        assert!(sql.contains(r#"FOREIGN KEY ("user_id")"#));
        assert!(sql.contains(r#"REFERENCES "users" ("id")"#));
    }

    #[test]
    fn test_add_foreign_key_multiple_columns_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let fk = ForeignKeyDef {
            constraint_name: None,
            columns: vec!["tenant_id".to_string(), "user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["tenant_id".to_string(), "id".to_string()],
            on_delete: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
            on_update: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
        };

        let (sql, _) = generator.add_foreign_key("orders", &fk);

        assert!(sql.contains(r#"ALTER TABLE "orders""#));
        assert!(sql.contains(r#"ADD FOREIGN KEY"#));
        assert!(sql.contains(r#"FOREIGN KEY ("tenant_id", "user_id")"#));
        assert!(sql.contains(r#"REFERENCES "users" ("tenant_id", "id")"#));
    }

    #[test]
    fn test_add_foreign_key_single_column_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let fk = ForeignKeyDef {
            constraint_name: None,
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
            on_update: crate::sql::metadata::fk::ForeignKeyAction::NoAction,
        };

        let (sql, _) = generator.add_foreign_key("orders", &fk);

        assert!(sql.contains("ALTER TABLE `orders`"));
        assert!(sql.contains("ADD FOREIGN KEY"));
        assert!(sql.contains("FOREIGN KEY (`user_id`)"));
        assert!(sql.contains("REFERENCES `users` (`id`)"));
    }

    #[test]
    fn test_add_foreign_key_multiple_columns_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let fk = ForeignKeyDef {
            constraint_name: None,
            columns: vec!["tenant_id".to_string(), "user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["tenant_id".to_string(), "id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
        };

        let (sql, _) = generator.add_foreign_key("orders", &fk);

        assert!(sql.contains("ALTER TABLE `orders`"));
        assert!(sql.contains("ADD FOREIGN KEY"));
        assert!(sql.contains("FOREIGN KEY (`tenant_id`, `user_id`)"));
        assert!(sql.contains("REFERENCES `users` (`tenant_id`, `id`)"));
    }

    #[test]
    fn test_add_unique_constraint_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let uc = UniqueConstraintDef {
            constraint_name: Some("uq_users_email".to_string()),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
        };

        let (sql, _) = generator.add_unique_constraint("users", &uc);

        assert!(sql.contains(r#"ALTER TABLE "users""#));
        assert!(sql.contains("ADD CONSTRAINT"));
        assert!(sql.contains(r#""uq_users_email""#));
        assert!(sql.contains("UNIQUE"));
        assert!(sql.contains(r#""email""#));
    }

    #[test]
    fn test_add_unique_constraint_multi_column_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let uc = UniqueConstraintDef {
            constraint_name: Some("uq_orders_tenant_user".to_string()),
            table: "orders".to_string(),
            columns: vec!["tenant_id".to_string(), "user_id".to_string()],
        };

        let (sql, _) = generator.add_unique_constraint("orders", &uc);

        assert!(sql.contains("ALTER TABLE `orders`"));
        assert!(sql.contains("UNIQUE"));
        assert!(sql.contains("`tenant_id`"));
        assert!(sql.contains("`user_id`"));
    }

    #[test]
    fn test_add_check_constraint_postgres() {
        let generator = QueryGenerator::new(&Postgres);

        let cc = CheckConstraintDef {
            constraint_name: Some("chk_users_age".to_string()),
            table: "users".to_string(),
            expression: "age >= 18".to_string(),
        };

        let (sql, _) = generator.add_check_constraint("users", &cc);

        assert!(sql.contains(r#"ALTER TABLE "users""#));
        assert!(sql.contains("ADD CONSTRAINT"));
        assert!(sql.contains(r#""chk_users_age""#));
        assert!(sql.contains("CHECK"));
        assert!(sql.contains("age >= 18"));
    }

    #[test]
    fn test_add_check_constraint_mysql() {
        let generator = QueryGenerator::new(&MySql);

        let cc = CheckConstraintDef {
            constraint_name: Some("chk_price_positive".to_string()),
            table: "products".to_string(),
            expression: "price > 0".to_string(),
        };

        let (sql, _) = generator.add_check_constraint("products", &cc);

        assert!(sql.contains("ALTER TABLE `products`"));
        assert!(sql.contains("CHECK"));
        assert!(sql.contains("price > 0"));
    }
}
