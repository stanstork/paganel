use crate::{
    drivers::mysql::{driver::MySqlDriver, queries},
    error::DriverError,
    sql::metadata::{
        column::ColumnMetadata,
        constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
        fk::ForeignKeyMetadata,
        index::{IndexColumn, IndexMetadata, IndexType, NullsOrder, SortOrder},
        provider::MetadataProvider,
        table::TableMetadata,
    },
    traits::introspector::SchemaIntrospector,
};
use async_trait::async_trait;
use mysql_async::{Row as MySqlRow, prelude::Queryable};
use std::collections::{HashMap, LinkedList, hash_map::Entry};

#[async_trait]
impl SchemaIntrospector for MySqlDriver {
    async fn table_exists(&self, table: &str) -> Result<bool, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let exists: Option<(bool,)> = conn.exec_first(queries::TABLE_EXISTS_SQL, (table,)).await?;
        Ok(exists.map(|row| row.0).unwrap_or(false))
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn.query(queries::LIST_TABLES_SQL).await?;

        rows.into_iter()
            .map(|row| {
                row.get_opt::<String, _>(0)
                    .and_then(|res| res.ok())
                    .ok_or_else(|| DriverError::Unknown("failed to read table name".to_string()))
            })
            .collect()
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn
            .exec(queries::TABLE_METADATA_SQL, (table, table, table))
            .await?;
        let columns = rows
            .iter()
            .map(|row| {
                let column_metadata = ColumnMetadata::from_row(row);
                Ok((column_metadata.name.clone(), column_metadata))
            })
            .collect::<Result<HashMap<_, _>, DriverError>>()?;

        drop(conn);

        let fks = self.fk_metadata(table).await?;

        MetadataProvider::construct_table_metadata(table, columns, fks)
    }

    async fn fk_metadata(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn.exec(queries::FK_METADATA_SQL, (table,)).await?;

        let fks = rows
            .iter()
            .map(|row| Ok(ForeignKeyMetadata::from_row(row)))
            .collect::<Result<Vec<_>, DriverError>>()?;

        Ok(fks)
    }

    async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn.exec(queries::INDEX_METADATA_SQL, (table,)).await?;

        // MySQL returns one row per column; group by index name preserving order.
        let mut order: LinkedList<String> = LinkedList::new();
        let mut by_name: HashMap<String, IndexMetadata> = HashMap::new();

        for row in &rows {
            let index_name: String = row.get("index_name").unwrap_or_default();
            let column_name: String = row.get("column_name").unwrap_or_default();

            let sort_order =
                SortOrder::parse(&row.get::<String, _>("sort_order").unwrap_or_default());

            let col = IndexColumn {
                name: column_name,
                sort_order,
                nulls_order: NullsOrder::Default,
            };

            match by_name.entry(index_name.clone()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().columns.push(col);
                }
                Entry::Vacant(entry) => {
                    order.push_back(index_name.clone());

                    let is_unique: bool = row.get::<u8, _>("is_unique").unwrap_or(0) != 0;
                    let is_primary: bool = row.get::<u8, _>("is_primary").unwrap_or(0) != 0;
                    let index_type_str: String = row.get("index_type").unwrap_or_default();
                    let index_type = IndexType::parse(&index_type_str).unwrap_or(IndexType::BTree);

                    entry.insert(IndexMetadata {
                        name: index_name,
                        table: table.to_string(),
                        schema: String::new(),
                        index_type,
                        columns: vec![col],
                        is_unique,
                        is_primary,
                        condition: None,
                        tablespace: None,
                        fill_factor: None,
                        size_bytes: None,
                        comment: None,
                    });
                }
            }
        }

        Ok(order
            .into_iter()
            .filter_map(|n| by_name.remove(&n))
            .collect())
    }

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn.exec(queries::REFERRING_TABLES_SQL, (table,)).await?;

        rows.into_iter()
            .map(|row| {
                row.get_opt::<String, _>("referencing_table")
                    .and_then(|res| res.ok())
                    .ok_or_else(|| {
                        DriverError::Unknown(
                            "missing referencing_table column in metadata".to_string(),
                        )
                    })
            })
            .collect()
    }

    async fn table_size_bytes(&self, table: &str) -> Result<u64, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let row: Option<MySqlRow> = conn.exec_first(queries::TABLE_SIZE_SQL, (table,)).await?;
        let size_bytes: u64 = match row {
            Some(row) => row.get("size_bytes").unwrap_or(0),
            None => 0,
        };
        Ok(size_bytes)
    }

    async fn unique_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn
            .exec(queries::UNIQUE_CONSTRAINT_METADATA_SQL, (table,))
            .await?;

        let constraints = rows
            .iter()
            .map(|row| Ok(UniqueConstraintMetadata::from_row(row)))
            .collect::<Result<Vec<_>, DriverError>>()?;

        Ok(constraints)
    }

    async fn check_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn
            .exec(queries::CHECK_CONSTRAINT_METADATA_SQL, (table,))
            .await?;

        let constraints = rows
            .iter()
            .map(|row| Ok(CheckConstraintMetadata::from_row(row)))
            .collect::<Result<Vec<_>, DriverError>>()?;

        Ok(constraints)
    }
}
