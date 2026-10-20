use crate::error::StateStoreError;
use crate::store::receipts::{delete_pipeline_receipts, upsert_receipt};
use crate::store::{StateStore, to_ser, to_sqlite};
use model::integrity::{
    import::{ImportRecord, import_pipeline},
    receipt::VerificationReceipt,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

/// Persistence for `pag state import` baselines.
impl StateStore {
    /// Save an import, replacing any earlier import of the same name.
    pub async fn save_import(
        &self,
        record: &ImportRecord,
        receipt: &VerificationReceipt,
    ) -> Result<(), StateStoreError> {
        let pipeline = import_pipeline(&record.name);
        debug_assert_eq!(
            receipt.pipeline_name, pipeline,
            "an import's receipt is stored under its import pipeline"
        );

        let name = record.name.clone();
        let connection = record.connection.clone();
        let value = serde_json::to_string(record).map_err(to_ser)?;
        let receipt = receipt.clone();

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;

            // A re-import may hash a different table under the same name.
            delete_pipeline_receipts(&tx, &pipeline)?;
            upsert_receipt(&tx, &receipt)?;

            tx.prepare_cached(
                "INSERT INTO imports (name, connection, record) VALUES (?1, ?2, ?3)
                 ON CONFLICT (name) DO UPDATE
                 SET connection = excluded.connection, record = excluded.record",
            )
            .map_err(to_sqlite)?
            .execute(params![name, connection, value])
            .map_err(to_sqlite)?;

            tx.commit().map_err(to_sqlite)
        })
        .await
    }

    pub async fn load_import(&self, name: &str) -> Result<Option<ImportRecord>, StateStoreError> {
        let name = name.to_string();

        self.with_conn(move |conn| {
            conn.prepare_cached("SELECT record FROM imports WHERE name = ?1")
                .map_err(to_sqlite)?
                .query_row(params![name], |row| row.get::<_, String>(0))
                .optional()
                .map_err(to_sqlite)?
                .map(|json| serde_json::from_str(&json).map_err(to_ser))
                .transpose()
        })
        .await
    }

    pub async fn list_imports(&self) -> Result<Vec<ImportRecord>, StateStoreError> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare_cached("SELECT record FROM imports ORDER BY name")
                .map_err(to_sqlite)?;

            stmt.query_map([], |row| row.get::<_, String>(0))
                .map_err(to_sqlite)?
                .map(|row| {
                    let json = row.map_err(to_sqlite)?;
                    serde_json::from_str(&json).map_err(to_ser)
                })
                .collect()
        })
        .await
    }

    pub async fn delete_import(&self, name: &str) -> Result<bool, StateStoreError> {
        let name = name.to_string();

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;

            let deleted = tx
                .prepare_cached("DELETE FROM imports WHERE name = ?1")
                .map_err(to_sqlite)?
                .execute(params![name])
                .map_err(to_sqlite)?;

            delete_pipeline_receipts(&tx, &import_pipeline(&name))?;

            tx.commit().map_err(to_sqlite)?;
            Ok(deleted > 0)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::algorithm::HashAlgorithm;
    use tempfile::tempdir;

    fn record(name: &str, table: &str, rows: u64) -> ImportRecord {
        ImportRecord {
            name: name.into(),
            connection: "prod".into(),
            table: table.into(),
            key_columns: vec!["id".into()],
            imported_at: chrono::Utc::now(),
            rows,
        }
    }

    fn receipt(record: &ImportRecord) -> VerificationReceipt {
        VerificationReceipt {
            run_id: "import-20261020T000000Z".into(),
            pipeline_name: import_pipeline(&record.name),
            table_name: record.table.clone(),
            table_root: [7; 32],
            column_order: vec!["id".into(), "name".into()],
            key_columns: record.key_columns.clone(),
            total_rows: record.rows,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    async fn save(store: &StateStore, record: &ImportRecord) {
        store.save_import(record, &receipt(record)).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_import_saves_its_record_and_receipt_together() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        let orders = record("orders", "orders", 10);
        save(&store, &orders).await;

        assert_eq!(store.load_import("orders").await.unwrap(), Some(orders));
        let stored = store
            .load_receipt("@import/orders", "orders")
            .await
            .unwrap()
            .expect("receipt saved with the record");
        assert_eq!(stored.total_rows, 10);
        assert_eq!(store.load_import("missing").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reimporting_a_name_replaces_its_baseline() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        save(&store, &record("orders", "orders", 10)).await;

        // Same name, another table: the old table's receipt must not linger.
        save(&store, &record("orders", "orders_v2", 20)).await;

        let current = store.load_import("orders").await.unwrap().unwrap();
        assert_eq!((current.table.as_str(), current.rows), ("orders_v2", 20));
        assert!(
            store
                .load_receipt("@import/orders", "orders")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .load_receipt("@import/orders", "orders_v2")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn imports_list_by_name() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        save(&store, &record("users", "users", 1)).await;
        save(&store, &record("orders", "orders", 1)).await;

        let names: Vec<String> = store
            .list_imports()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names, ["orders", "users"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deleting_an_import_removes_only_its_own_receipts() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        save(&store, &record("orders", "orders", 1)).await;
        save(&store, &record("users", "users", 1)).await;

        // A pipeline that happens to share the table name keeps its receipt.
        let mut pipeline_receipt = receipt(&record("orders", "orders", 1));
        pipeline_receipt.pipeline_name = "migrate_orders".into();
        store.save_receipt(&pipeline_receipt).await.unwrap();

        assert!(store.delete_import("orders").await.unwrap());
        assert!(!store.delete_import("orders").await.unwrap());

        assert_eq!(store.load_import("orders").await.unwrap(), None);
        assert!(
            store
                .load_receipt("@import/orders", "orders")
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.load_import("users").await.unwrap().is_some());
        assert!(
            store
                .load_receipt("migrate_orders", "orders")
                .await
                .unwrap()
                .is_some()
        );
    }
}
