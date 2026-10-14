use crate::StateStore;
use crate::error::StateStoreError;
use crate::models::{Checkpoint, CheckpointStage, CheckpointSummary, RunState, WalEntry};
use crate::store::{to_ser, to_sqlite, to_storage};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::HashMap;

const SELECT_CHECKPOINT_STAGE: &str = "SELECT batch_id, stage FROM checkpoints
     WHERE run_id = ?1 AND item_id = ?2 AND part_id = ?3";

const UPSERT_CHECKPOINT: &str = "INSERT INTO checkpoints
     (run_id, item_id, part_id, batch_id, stage, rows_done, data)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
     ON CONFLICT (run_id, item_id, part_id) DO UPDATE SET
         batch_id = excluded.batch_id,
         stage = excluded.stage,
         rows_done = excluded.rows_done,
         data = excluded.data";

impl StateStore {
    pub async fn last_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<CheckpointSummary>, StateStoreError> {
        Ok(self
            .load_checkpoint(run_id, item_id, part_id)
            .await?
            .map(CheckpointSummary::from))
    }

    pub async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), StateStoreError> {
        let cp = cp.clone();

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;
            write_checkpoint(&tx, &cp)?;
            tx.commit().map_err(to_sqlite)
        })
        .await
    }

    pub async fn commit_batch(
        &self,
        entry: &WalEntry,
        cp: &Checkpoint,
    ) -> Result<(), StateStoreError> {
        let run_id = entry.run_id().to_string();
        let entry = serde_json::to_string(entry).map_err(to_ser)?;
        let cp = cp.clone();

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;

            insert_wal(&tx, &run_id, &entry)?;
            write_checkpoint(&tx, &cp)?;

            tx.commit().map_err(to_sqlite)
        })
        .await
    }

    pub async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, StateStoreError> {
        let run_id = run_id.to_string();
        let item_id = item_id.to_string();
        let part_id = part_id.to_string();

        let json_opt = self
            .with_conn(move |conn| {
                conn.prepare_cached(
                    "SELECT data FROM checkpoints WHERE run_id = ?1 AND item_id = ?2 AND part_id = ?3",
                )
                .map_err(to_sqlite)?
                .query_row(params![run_id, item_id, part_id], |row| {
                    row.get::<_, String>(0)
                })
                .optional()
                .map_err(to_sqlite)
            })
            .await?;

        json_opt
            .map(|json| serde_json::from_str(&json).map_err(to_ser))
            .transpose()
    }

    pub async fn total_rows_done(
        &self,
        run_id: &str,
        item_id: &str,
    ) -> Result<u64, StateStoreError> {
        let (run_id, item_id) = (run_id.to_string(), item_id.to_string());

        // Sum `rows_done` over every part of this item.
        let total: i64 = self
            .with_conn(move |conn| {
                conn.prepare_cached(
                    "SELECT COALESCE(SUM(rows_done), 0) FROM checkpoints
                     WHERE run_id = ?1 AND item_id = ?2",
                )
                .map_err(to_sqlite)?
                .query_row(params![run_id, item_id], |row| row.get(0))
                .map_err(to_sqlite)
            })
            .await?;

        u64::try_from(total).map_err(to_storage)
    }

    pub async fn rows_done_by_item(
        &self,
        run_id: &str,
    ) -> Result<HashMap<String, u64>, StateStoreError> {
        let run_id = run_id.to_string();

        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT item_id, SUM(rows_done) FROM checkpoints
                     WHERE run_id = ?1 GROUP BY item_id",
                )
                .map_err(to_sqlite)?;

            let rows = stmt
                .query_map(params![run_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(to_sqlite)?;

            let mut counts = HashMap::new();
            for row in rows {
                let (item, rows_done) = row.map_err(to_sqlite)?;
                counts.insert(item, u64::try_from(rows_done).map_err(to_storage)?);
            }

            Ok(counts)
        })
        .await
    }

    pub async fn append_wal(&self, entry: &WalEntry) -> Result<(), StateStoreError> {
        let run_id = entry.run_id().to_string();
        let value = serde_json::to_string(entry).map_err(to_ser)?;

        self.with_conn(move |conn| insert_wal(conn, &run_id, &value))
            .await
    }

    pub async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, StateStoreError> {
        let run_id = run_id.to_string();

        let raw_entries: Vec<String> = self
            .with_conn(move |conn| {
                let mut stmt = conn
                    .prepare_cached("SELECT entry FROM wal WHERE run_id = ?1 ORDER BY seq")
                    .map_err(to_sqlite)?;
                let rows = stmt
                    .query_map(params![run_id], |row| row.get::<_, String>(0))
                    .map_err(to_sqlite)?;

                let mut strings = Vec::new();
                for row in rows {
                    strings.push(row.map_err(to_sqlite)?);
                }
                Ok(strings)
            })
            .await?;

        let mut entries = Vec::with_capacity(raw_entries.len());
        for json in raw_entries {
            entries.push(serde_json::from_str(&json).map_err(to_ser)?);
        }

        Ok(entries)
    }

    pub async fn save_run_state(&self, state: &RunState) -> Result<(), StateStoreError> {
        let run_id = state.run_id.clone();
        let value = serde_json::to_string(state).map_err(to_ser)?;
        let updated_at = chrono::Utc::now().to_rfc3339();

        self.with_conn(move |conn| {
            conn.prepare_cached(
                "INSERT INTO runs (run_id, state, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT (run_id) DO UPDATE SET
                     state = excluded.state,
                     updated_at = excluded.updated_at",
            )
            .map_err(to_sqlite)?
            .execute(params![run_id, value, updated_at])
            .map_err(to_sqlite)?;

            Ok(())
        })
        .await
    }

    pub async fn load_run_state(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError> {
        let run_id = run_id.to_string();

        let json_opt = self
            .with_conn(move |conn| {
                conn.prepare_cached("SELECT state FROM runs WHERE run_id = ?1")
                    .map_err(to_sqlite)?
                    .query_row(params![run_id], |row| row.get::<_, String>(0))
                    .optional()
                    .map_err(to_sqlite)
            })
            .await?;

        json_opt
            .map(|json| serde_json::from_str(&json).map_err(to_ser))
            .transpose()
    }

    pub async fn list_runs(&self) -> Result<Vec<RunState>, StateStoreError> {
        let raw_states: Vec<String> = self
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare_cached("SELECT state FROM runs ORDER BY run_id")
                    .map_err(to_sqlite)?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(to_sqlite)?;

                let mut strings = Vec::new();
                for row in rows {
                    strings.push(row.map_err(to_sqlite)?);
                }
                Ok(strings)
            })
            .await?;

        let mut runs = Vec::with_capacity(raw_states.len());
        for json in raw_states {
            runs.push(serde_json::from_str(&json).map_err(to_ser)?);
        }

        Ok(runs)
    }

    /// Delete all state for a given run: run state, checkpoints, and WAL entries.
    pub async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError> {
        let id = run_id.to_string();

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;

            for sql in [
                "DELETE FROM runs WHERE run_id = ?1",
                "DELETE FROM checkpoints WHERE run_id = ?1",
                "DELETE FROM wal WHERE run_id = ?1",
            ] {
                tx.prepare_cached(sql)
                    .map_err(to_sqlite)?
                    .execute(params![id])
                    .map_err(to_sqlite)?;
            }

            tx.commit().map_err(to_sqlite)
        })
        .await?;

        Ok(())
    }
}

fn insert_wal(conn: &Connection, run_id: &str, entry: &str) -> Result<(), StateStoreError> {
    conn.prepare_cached("INSERT INTO wal (run_id, entry) VALUES (?1, ?2)")
        .map_err(to_sqlite)?
        .execute(params![run_id, entry])
        .map_err(to_sqlite)?;
    Ok(())
}

fn write_checkpoint(tx: &Transaction, cp: &Checkpoint) -> Result<(), StateStoreError> {
    let existing: Option<(String, i64)> = tx
        .prepare_cached(SELECT_CHECKPOINT_STAGE)
        .map_err(to_sqlite)?
        .query_row(params![cp.run_id, cp.item_id, cp.part_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(to_sqlite)?;

    if let Some((batch_id, stage)) = existing {
        let should_update = if batch_id == cp.batch_id {
            stage_code(cp.stage) >= stage
        } else {
            stage == stage_code(CheckpointStage::Committed)
        };

        if !should_update {
            return Ok(());
        }
    }

    let data = serde_json::to_string(cp).map_err(to_ser)?;
    tx.prepare_cached(UPSERT_CHECKPOINT)
        .map_err(to_sqlite)?
        .execute(params![
            cp.run_id,
            cp.item_id,
            cp.part_id,
            cp.batch_id,
            stage_code(cp.stage),
            to_i64(cp.rows_done)?,
            data
        ])
        .map_err(to_sqlite)?;

    Ok(())
}

fn stage_code(stage: CheckpointStage) -> i64 {
    stage as i64
}

fn to_i64(n: u64) -> Result<i64, StateStoreError> {
    i64::try_from(n).map_err(to_storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PipelineRunState, PipelineStatus, RunStatus};
    use model::core::value::{IntervalValue, Value};
    use model::pagination::cursor::{Cursor, QualCol};
    use tempfile::tempdir;

    fn mk_cp(stage: CheckpointStage, batch: &str, cursor: Cursor) -> Checkpoint {
        Checkpoint {
            run_id: "run".into(),
            item_id: "item".into(),
            part_id: "part".into(),
            stage,
            src_offset: cursor,
            pending_offset: None,
            batch_id: batch.to_string(),
            rows_done: 0,
            updated_at: chrono::Utc::now(),
        }
    }
    #[tokio::test]
    async fn keeps_uncommitted_write_over_future_read() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        store
            .save_checkpoint(&mk_cp(CheckpointStage::Write, "batch-1", Cursor::None))
            .await
            .unwrap();

        store
            .save_checkpoint(&mk_cp(
                CheckpointStage::Read,
                "batch-2",
                Cursor::Default { offset: 1 },
            ))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.stage, CheckpointStage::Write);
        assert_eq!(cp.batch_id, "batch-1");
    }

    #[tokio::test]
    async fn advances_after_commit() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        store
            .save_checkpoint(&mk_cp(CheckpointStage::Committed, "batch-1", Cursor::None))
            .await
            .unwrap();

        store
            .save_checkpoint(&mk_cp(
                CheckpointStage::Read,
                "batch-2",
                Cursor::Default { offset: 1 },
            ))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(cp.stage, CheckpointStage::Read);
        assert_eq!(cp.batch_id, "batch-2");
    }

    fn col(column: &str) -> QualCol {
        QualCol {
            table: "t".into(),
            column: column.into(),
        }
    }

    /// Checkpoints are stored as JSON now; every cursor shape must survive it,
    /// or a paused migration cannot resume.
    #[test]
    fn every_cursor_round_trips_through_json() {
        let ts = chrono::NaiveDate::from_ymd_opt(2026, 10, 11)
            .unwrap()
            .and_hms_micro_opt(6, 30, 0, 123_456)
            .unwrap();

        let cursors = vec![
            Cursor::None,
            Cursor::Default { offset: 42 },
            Cursor::Pk {
                pk_col: col("id"),
                id: u64::MAX,
            },
            Cursor::Numeric {
                col: col("n"),
                val: i128::MIN,
            },
            Cursor::Timestamp {
                col: col("ts"),
                ts: -1,
            },
            Cursor::CompositeNumPk {
                num_col: col("n"),
                pk_col: col("id"),
                val: i128::MAX,
                id: 7,
            },
            Cursor::CompositeTsPk {
                ts_col: col("ts"),
                pk_col: col("id"),
                ts: 1_791_590_400_000_000,
                id: 7,
            },
            Cursor::Keyset {
                keys: vec![col("a"), col("b"), col("c"), col("d"), col("e"), col("f")],
                values: vec![
                    Value::Int(-5),
                    Value::UInt(u64::MAX),
                    Value::Decimal("12345678901234567890.000001".parse().unwrap()),
                    Value::Float(1.5),
                    Value::String("caf\u{e9} \u{1f600}".into()),
                    Value::Binary(vec![0, 255, 7]),
                    Value::Timestamp {
                        value: ts,
                        offset_secs: Some(3600),
                    },
                    Value::Uuid("67e55044-10b1-426f-9247-bb680e5fe0c8".parse().unwrap()),
                    Value::Json(serde_json::json!({"z": 1, "a": [true, null]})),
                    Value::Interval(IntervalValue {
                        months: 1,
                        days: -2,
                        microseconds: 3,
                    }),
                    Value::Null,
                ],
            },
        ];

        for cursor in cursors {
            let mut cp = mk_cp(CheckpointStage::Committed, "b", cursor.clone());
            cp.pending_offset = Some(cursor.clone());

            let json = serde_json::to_string(&cp).unwrap();
            let back: Checkpoint = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("{cursor:?} did not round-trip: {e}\n{json}"));
            assert_eq!(back.src_offset, cursor);
            assert_eq!(back.pending_offset, Some(cursor));
        }
    }

    #[tokio::test]
    async fn commit_batch_writes_wal_and_checkpoint_together() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        store
            .save_checkpoint(&mk_cp(CheckpointStage::Write, "batch-1", Cursor::None))
            .await
            .unwrap();

        let mut committed = mk_cp(
            CheckpointStage::Committed,
            "batch-1",
            Cursor::Default { offset: 1000 },
        );
        committed.rows_done = 1000;
        let entry = WalEntry::BatchCommit {
            run_id: "run".into(),
            item_id: "item".into(),
            part_id: "part".into(),
            batch_id: "batch-1".into(),
            ts: chrono::Utc::now(),
        };
        store.commit_batch(&entry, &committed).await.unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.stage, CheckpointStage::Committed);
        assert_eq!(cp.src_offset, Cursor::Default { offset: 1000 });
        assert_eq!(store.total_rows_done("run", "item").await.unwrap(), 1000);
        assert!(matches!(
            store.iter_wal("run").await.unwrap().as_slice(),
            [WalEntry::BatchCommit { batch_id, .. }] if batch_id == "batch-1"
        ));
    }

    #[tokio::test]
    async fn total_rows_done_sums_every_part_of_an_item() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        for (part, rows) in [("p1", 100), ("p2", 250)] {
            let mut cp = mk_cp(CheckpointStage::Committed, "b", Cursor::None);
            cp.part_id = part.into();
            cp.rows_done = rows;
            store.save_checkpoint(&cp).await.unwrap();
        }
        let mut other = mk_cp(CheckpointStage::Committed, "b", Cursor::None);
        other.item_id = "other".into();
        other.rows_done = 9;
        store.save_checkpoint(&other).await.unwrap();

        assert_eq!(store.total_rows_done("run", "item").await.unwrap(), 350);
        assert_eq!(store.total_rows_done("run", "missing").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn wal_comes_back_in_append_order() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        for i in 0..50 {
            store
                .append_wal(&WalEntry::ItemStart {
                    run_id: "run".into(),
                    item_id: format!("item-{i}"),
                })
                .await
                .unwrap();
        }
        store
            .append_wal(&WalEntry::RunDone {
                run_id: "other".into(),
            })
            .await
            .unwrap();

        let items: Vec<String> = store
            .iter_wal("run")
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e {
                WalEntry::ItemStart { item_id, .. } => item_id,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        let expected: Vec<String> = (0..50).map(|i| format!("item-{i}")).collect();
        assert_eq!(items, expected);
    }

    fn run_state(run_id: &str, status: RunStatus) -> RunState {
        RunState {
            run_id: run_id.into(),
            config_path: "c.ppl".into(),
            config_hash: "h".into(),
            status,
            started_at: chrono::Utc::now(),
            total_pipelines: 1,
            pipelines: vec![PipelineRunState {
                name: "p".into(),
                item_id: "item".into(),
                status: PipelineStatus::Running,
                rows_done: 1,
                total_rows: Some(2),
            }],
        }
    }

    #[tokio::test]
    async fn delete_run_removes_only_that_run() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        for run in ["run", "keep"] {
            store
                .save_run_state(&run_state(run, RunStatus::Running))
                .await
                .unwrap();
            let mut cp = mk_cp(CheckpointStage::Committed, "b", Cursor::None);
            cp.run_id = run.into();
            store.save_checkpoint(&cp).await.unwrap();
            store
                .append_wal(&WalEntry::RunDone { run_id: run.into() })
                .await
                .unwrap();
        }

        store.delete_run("run").await.unwrap();

        assert!(store.load_run_state("run").await.unwrap().is_none());
        assert!(
            store
                .load_checkpoint("run", "item", "part")
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.iter_wal("run").await.unwrap().is_empty());

        let runs = store.list_runs().await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id, "keep");
        assert!(
            store
                .load_checkpoint("keep", "item", "part")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(store.iter_wal("keep").await.unwrap().len(), 1);
    }
}
