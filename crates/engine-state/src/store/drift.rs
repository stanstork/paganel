use crate::{
    StateStore,
    error::StateStoreError,
    store::{to_ser, to_sqlite},
};
use model::integrity::drift::{DriftRun, KEPT_RUNS};
use rusqlite::{TransactionBehavior, params};

/// `pag drift` run history.
impl StateStore {
    pub async fn save_drift_run(&self, run: &DriftRun) -> Result<(), StateStoreError> {
        let run_id = run.run_id.clone();
        let config = run.config.clone();
        let exit_code = run.exit_code();
        let value = serde_json::to_string(run).map_err(to_ser)?;

        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(to_sqlite)?;

            tx.prepare_cached(
                "INSERT INTO drift_runs (run_id, config, exit_code, run) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (run_id) DO UPDATE
                 SET config = excluded.config, exit_code = excluded.exit_code, run = excluded.run",
            )
            .map_err(to_sqlite)?
            .execute(params![run_id, config, exit_code, value])
            .map_err(to_sqlite)?;

            tx.prepare_cached(
                "DELETE FROM drift_runs WHERE config = ?1 AND run_id NOT IN
                 (SELECT run_id FROM drift_runs WHERE config = ?1 ORDER BY run_id DESC LIMIT ?2)",
            )
            .map_err(to_sqlite)?
            .execute(params![config, KEPT_RUNS as i64])
            .map_err(to_sqlite)?;

            tx.commit().map_err(to_sqlite)
        })
        .await
    }

    pub async fn last_drift_run(&self, config: &str) -> Result<Option<DriftRun>, StateStoreError> {
        Ok(self.list_drift_runs(config, 1).await?.pop())
    }

    pub async fn list_drift_runs(
        &self,
        config: &str,
        limit: usize,
    ) -> Result<Vec<DriftRun>, StateStoreError> {
        let config = config.to_string();

        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT run FROM drift_runs WHERE config = ?1 ORDER BY run_id DESC LIMIT ?2",
                )
                .map_err(to_sqlite)?;

            stmt.query_map(params![config, limit as i64], |row| row.get::<_, String>(0))
                .map_err(to_sqlite)?
                .map(|row| {
                    let json = row.map_err(to_sqlite)?;
                    serde_json::from_str(&json).map_err(to_ser)
                })
                .collect()
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn run(config: &str, n: usize) -> DriftRun {
        let now = chrono::Utc::now();
        DriftRun {
            run_id: format!("drift-{n:04}"),
            config: config.into(),
            started_at: now,
            finished_at: now,
            tables: Vec::new(),
            skipped: Vec::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn saving_keeps_the_newest_runs_of_each_config() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        for n in 0..KEPT_RUNS + 2 {
            store.save_drift_run(&run("/a.ppl", n)).await.unwrap();
        }
        store.save_drift_run(&run("/b.ppl", 0)).await.unwrap();

        let a = store.list_drift_runs("/a.ppl", 1000).await.unwrap();
        assert_eq!(a.len(), KEPT_RUNS);
        assert_eq!(a[0].run_id, format!("drift-{:04}", KEPT_RUNS + 1));
        assert_eq!(a[KEPT_RUNS - 1].run_id, "drift-0002");

        // Another config's history is neither pruned nor mixed in.
        let b = store.last_drift_run("/b.ppl").await.unwrap().unwrap();
        assert_eq!(b.run_id, "drift-0000");
        assert!(store.last_drift_run("/c.ppl").await.unwrap().is_none());
    }
}
