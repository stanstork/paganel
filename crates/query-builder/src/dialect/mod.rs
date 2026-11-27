mod mysql;
mod postgres;

pub use mysql::MySql;
pub use postgres::Postgres;

use model::core::types::Type;

pub trait QueryDialect: Send + Sync {
    /// Wraps an identifier (like a table or column name) in the correct
    /// quotation marks for the dialect.
    ///
    /// - PostgreSQL uses double quotes: `"my_column"`
    /// - MySQL uses backticks: `` `my_column` ``
    fn quote_identifier(&self, ident: &str) -> String;

    /// Returns the placeholder for a parameterized query.
    ///
    /// - PostgreSQL uses `$1`, `$2`, etc.
    /// - MySQL uses `?`
    fn placeholder(&self, index: usize) -> String;

    /// Renders a generic `Type` into a database-specific SQL type string.
    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String;

    /// Whether a raw introspected column type (as this dialect spells it) is an
    /// integer eligible for range-lane splitting. Dialects spell these
    /// differently - PostgreSQL has `int4`/`serial`, MySQL has `mediumint` and
    /// display widths like `int(11)` - so each answers for its own type names.
    fn is_integer_type(&self, data_type: &str) -> bool;

    /// Returns the name of the dialect (e.g., "PostgreSQL", "MySQL").
    fn name(&self) -> String;

    /// Generates the SQL query and a corresponding list of parameters to bind
    /// for efficiently checking the existence of multiple composite keys.
    fn key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String;

    /// Full DDL to drop a table's primary key. Dialects differ: MySQL has a
    /// direct `ALTER TABLE … DROP PRIMARY KEY`, while PostgreSQL has no such form
    /// and must drop the constraint by its catalog name.
    fn drop_primary_key(&self, table: &str) -> String;

    /// DDL to drop a named foreign-key constraint.
    fn drop_foreign_key(&self, table: &str, constraint: &str) -> String;

    /// Returns the random function name for this dialect.
    ///
    /// - PostgreSQL uses `RANDOM()`
    /// - MySQL uses `RAND()`
    /// - SQLite uses `RANDOM()`
    fn random_fn(&self) -> &'static str;

    /// Whether `CREATE INDEX ... IF NOT EXISTS` is supported.
    /// MySQL has no such clause (MariaDB does); PostgreSQL does.
    fn supports_index_if_not_exists(&self) -> bool {
        true
    }

    /// Whether `CREATE INDEX CONCURRENTLY` is supported (PostgreSQL only).
    fn supports_index_concurrently(&self) -> bool {
        true
    }

    /// Whether partial indexes (`CREATE INDEX ... WHERE <cond>`) are supported.
    /// PostgreSQL yes; MySQL no.
    fn supports_partial_index(&self) -> bool {
        true
    }

    /// Whether per-column `NULLS FIRST/LAST` ordering is supported in an index.
    /// PostgreSQL yes; MySQL no.
    fn supports_index_nulls(&self) -> bool {
        true
    }

    /// Whether the index method (`USING btree`) is written *before* the column
    /// list, as in PostgreSQL (`ON tbl USING btree (col)`). MySQL requires it
    /// after the column list (`ON tbl (col) USING BTREE`).
    fn index_method_before_cols(&self) -> bool {
        true
    }

    /// Whether an index column may carry a key prefix length, e.g. `` `col`(255) ``.
    /// MySQL requires one to index TEXT/BLOB columns; PostgreSQL has no such syntax.
    fn supports_index_prefix(&self) -> bool {
        false
    }

    /// Whether an auto-incrementing column must belong to a key.
    ///
    /// MySQL rejects `AUTO_INCREMENT` on a column that is not the first column of
    /// some key (error 1075), so the attribute must be dropped when a table is
    /// created without constraints. PostgreSQL's `SERIAL` is only a column default
    /// and needs no key.
    fn auto_inc_requires_key(&self) -> bool {
        false
    }

    /// Whether the dialect has standalone enum types (`CREATE TYPE ... AS ENUM`).
    ///
    /// PostgreSQL does. MySQL spells the variants inline in the column
    /// (`ENUM('a','b')`), so emitting a `CREATE TYPE` for it is a syntax error.
    fn supports_enums(&self) -> bool {
        false
    }

    /// Whether a `NULL` in an `INSERT ... VALUES` should be wrapped in a
    /// `CAST(NULL AS <type>)`.
    ///
    /// PostgreSQL casts so it can infer the column type in ambiguous contexts
    /// (e.g. `bytea`). MySQL's `CAST` does not accept type names like `varchar`
    /// / `text` / `float`, and a bare `NULL` is unambiguous under an explicit
    /// column list - so MySQL keeps the default (bare `NULL`).
    fn cast_null_literals(&self) -> bool {
        false
    }

    /// The keyword before each row of a `VALUES` list used as a table:
    /// MySQL 8.0.19+ needs `ROW(1, 2)`, PostgreSQL takes `(1, 2)`.
    fn values_row_keyword(&self) -> &'static str;

    /// The integer-division operator, spaces included: MySQL `DIV` (its `/`
    /// returns a decimal), PostgreSQL `/` (integer on integer operands).
    fn int_div_operator(&self) -> &'static str;
}
