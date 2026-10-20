use crate::error::StateStoreError;
use crate::store::StateStore;
use crate::store::{to_ser, to_sqlite};
use model::integrity::receipt::VerificationReceipt;
use rusqlite::{Connection, OptionalExtension, params};

/// Persistence for verification receipts.
impl StateStore {
    /// Persist a `VerificationReceipt` after a pipeline completes with --integrity.
    pub async fn save_receipt(&self, receipt: &VerificationReceipt) -> Result<(), StateStoreError> {
        let receipt = receipt.clone();
        self.with_conn(move |conn| upsert_receipt(conn, &receipt))
            .await
    }

    /// Load the receipt written by the most recent `apply --integrity` run
    /// for this pipeline+table pair.
    pub async fn load_receipt(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<Option<VerificationReceipt>, StateStoreError> {
        let pipeline = pipeline_name.to_string();
        let table = table_name.to_string();

        self.with_conn(move |conn| {
            conn.prepare_cached(
                "SELECT receipt FROM receipts WHERE pipeline = ?1 AND table_name = ?2",
            )
            .map_err(to_sqlite)?
            .query_row(params![pipeline, table], |row| row.get::<_, String>(0))
            .optional()
            .map_err(to_sqlite)?
            .map(|json| serde_json::from_str(&json).map_err(to_ser))
            .transpose()
        })
        .await
    }

    /// List all receipts across all pipelines and tables.
    pub async fn list_receipts(&self) -> Result<Vec<VerificationReceipt>, StateStoreError> {
        let raw_receipts: Vec<String> = self
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare_cached("SELECT receipt FROM receipts ORDER BY pipeline, table_name")
                    .map_err(to_sqlite)?;

                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(to_sqlite)?;

                let mut json = Vec::new();
                for row in rows {
                    json.push(row.map_err(to_sqlite)?);
                }

                Ok(json)
            })
            .await?;

        let mut receipts = Vec::with_capacity(raw_receipts.len());
        for json in raw_receipts {
            receipts.push(serde_json::from_str(&json).map_err(to_ser)?);
        }

        Ok(receipts)
    }

    /// Remove every receipt belonging to `pipeline_name`,
    /// returning the count of deleted receipts.
    pub async fn delete_receipts(&self, pipeline_name: &str) -> Result<usize, StateStoreError> {
        let pipeline = pipeline_name.to_string();
        self.with_conn(move |conn| delete_pipeline_receipts(conn, &pipeline))
            .await
    }
}

/// Insert or replace one receipt. Takes a connection so it can join a
/// caller's transaction (a `Transaction` derefs to `Connection`).
pub(super) fn upsert_receipt(
    conn: &Connection,
    receipt: &VerificationReceipt,
) -> Result<(), StateStoreError> {
    let value = serde_json::to_string(receipt).map_err(to_ser)?;

    conn.prepare_cached(
        "INSERT INTO receipts (pipeline, table_name, receipt) VALUES (?1, ?2, ?3)
         ON CONFLICT (pipeline, table_name) DO UPDATE SET receipt = excluded.receipt",
    )
    .map_err(to_sqlite)?
    .execute(params![receipt.pipeline_name, receipt.table_name, value])
    .map_err(to_sqlite)?;

    Ok(())
}

/// Delete every receipt of `pipeline`, returning how many there were.
pub(super) fn delete_pipeline_receipts(
    conn: &Connection,
    pipeline: &str,
) -> Result<usize, StateStoreError> {
    conn.prepare_cached("DELETE FROM receipts WHERE pipeline = ?1")
        .map_err(to_sqlite)?
        .execute(params![pipeline])
        .map_err(to_sqlite)
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::algorithm::HashAlgorithm;
    use tempfile::tempdir;

    fn receipt(pipeline: &str, table: &str, rows: u64) -> VerificationReceipt {
        VerificationReceipt {
            run_id: "run".into(),
            pipeline_name: pipeline.into(),
            table_name: table.into(),
            table_root: [7; 32],
            column_order: vec!["id".into(), "name".into()],
            key_columns: vec!["id".into()],
            total_rows: rows,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_receipt_per_table_wins() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        store
            .save_receipt(&receipt("p", "actor", 10))
            .await
            .unwrap();
        store
            .save_receipt(&receipt("p", "actor", 20))
            .await
            .unwrap();

        let loaded = store.load_receipt("p", "actor").await.unwrap().unwrap();
        assert_eq!(loaded.total_rows, 20);
        assert_eq!(loaded.table_root, [7; 32]);
        assert!(store.load_receipt("p", "film").await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_receipts_only_touches_that_pipeline() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        store.save_receipt(&receipt("p", "actor", 1)).await.unwrap();
        store.save_receipt(&receipt("p", "film", 1)).await.unwrap();
        store
            .save_receipt(&receipt("p:x", "actor", 1))
            .await
            .unwrap();

        assert_eq!(store.delete_receipts("p").await.unwrap(), 2);

        let left = store.list_receipts().await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].pipeline_name, "p:x");
    }
}
