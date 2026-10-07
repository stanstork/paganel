use async_trait::async_trait;
use connectors::{
    drivers::{
        csv::metadata::CsvMetadata, mysql::types::MySqlTypeConverter,
        postgres::types::PgTypeConverter,
    },
    error::DriverError,
    sql::metadata::{
        capabilities::Capabilities,
        column::ColumnMetadata,
        constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
        fk::ForeignKeyMetadata,
        index::IndexMetadata,
        table::TableMetadata,
    },
    traits::{
        driver::{Driver, DriverInfo},
        introspector::SchemaIntrospector,
    },
};
use engine_schema::type_registry::Dialect;
use model::core::{convert::FromCanonical, types::Type};
use std::collections::HashMap;

static INFO: DriverInfo = DriverInfo {
    id: "csv",
    name: "CSV file source",
    schemes: &["csv"],
};

/// Read-only introspector backed by a CSV file's inferred schema, so a
/// `csv -> db` pipeline can create the destination table. Each column's
/// canonical type is rendered in the destination dialect's native DDL form.
pub struct CsvIntrospector {
    meta: TableMetadata,
    capabilities: Capabilities,
}

impl CsvIntrospector {
    pub fn new(csv_meta: &CsvMetadata, dest_dialect: Dialect) -> Self {
        let columns = csv_meta
            .columns
            .iter()
            .map(|col| {
                let ddl = match dest_dialect {
                    Dialect::Postgres => PgTypeConverter.to_ddl(&col.data_type).ddl,
                    Dialect::MySql => MySqlTypeConverter.to_ddl(&col.data_type).ddl,
                };
                // Populate the metadata the way real introspection would so the
                // destination dialect's `to_canonical` can round-trip it: the
                // base type name in `data_type`, the parametrized form in
                // `full_column_type` (MySQL keys `tinyint(1)` -> boolean off it),
                // and precision/scale/length in their own fields.
                let base = ddl.split('(').next().unwrap_or(&ddl).trim().to_lowercase();
                let (num_precision, num_scale) = match &col.data_type {
                    Type::Decimal { precision, scale } => {
                        (precision.map(|p| p as u32), scale.map(|s| s as u32))
                    }
                    _ => (None, None),
                };
                let char_max_length = match &col.data_type {
                    Type::Varchar { length, .. } | Type::Char { length, .. } => *length,
                    _ => None,
                };
                (
                    col.name.clone(),
                    ColumnMetadata {
                        ordinal: col.ordinal,
                        name: col.name.clone(),
                        data_type: base,
                        full_column_type: Some(ddl.to_lowercase()),
                        is_nullable: col.is_nullable,
                        is_primary_key: col.is_primary_key,
                        num_precision,
                        num_scale,
                        char_max_length,
                        ..Default::default()
                    },
                )
            })
            .collect();

        let primary_keys = csv_meta
            .columns
            .iter()
            .filter(|col| col.is_primary_key)
            .map(|col| col.name.clone())
            .collect();

        let meta = TableMetadata {
            name: String::new(),
            schema: None,
            columns,
            primary_keys,
            foreign_keys: Vec::new(),
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        };

        Self {
            meta,
            capabilities: Capabilities::default(),
        }
    }
}

impl Driver for CsvIntrospector {
    fn info(&self) -> &DriverInfo {
        &INFO
    }

    fn version(&self) -> &str {
        "0"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
}

#[async_trait]
impl SchemaIntrospector for CsvIntrospector {
    async fn table_exists(&self, _table: &str) -> Result<bool, DriverError> {
        Ok(true)
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DriverError> {
        Ok(Vec::new())
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        let mut meta = self.meta.clone();
        meta.name = table.to_string();
        Ok(meta)
    }

    async fn index_metadata(&self, _table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn fk_metadata(&self, _table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn referencing_tables(&self, _table: &str) -> Result<Vec<String>, DriverError> {
        Ok(Vec::new())
    }

    async fn table_size_bytes(&self, _table: &str) -> Result<u64, DriverError> {
        Ok(0)
    }

    async fn unique_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        Ok(Vec::new())
    }

    async fn check_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        Ok(Vec::new())
    }
}
