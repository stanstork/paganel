use chrono::DateTime;
use engine_state::{
    StateStore,
    error::StateStoreError,
    models::{Checkpoint, CheckpointStage, WalEntry},
};
use model::pagination::cursor::Cursor;
use serde::Serialize;
use std::{fmt, sync::Arc};

#[derive(Clone)]
pub struct ProgressService {
    pub store: Arc<StateStore>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ProgressStage {
    Idle,
    Snapshot,
    Running,
    Validating,
    Done,
    Failed,
}

impl ProgressStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProgressStage::Idle => "Idle",
            ProgressStage::Snapshot => "Snapshot",
            ProgressStage::Running => "Running",
            ProgressStage::Validating => "Validating",
            ProgressStage::Done => "Done",
            ProgressStage::Failed => "Failed",
        }
    }
}

impl fmt::Display for ProgressStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgressStatus {
    pub stage: ProgressStage,
    pub last_cursor: Cursor,
    pub rows_done: u64,
    pub last_heartbeat: Option<DateTime<chrono::Utc>>,
}

impl ProgressService {
    pub fn new(store: Arc<StateStore>) -> Self {
        ProgressService { store }
    }

    pub async fn item_status(
        &self,
        run_id: &str,
        item_id: &str,
    ) -> Result<ProgressStatus, StateStoreError> {
        let wal_entries = self.store.iter_wal(run_id).await?;

        let mut part_hint = None;
        let mut last_heartbeat = None;
        let mut item_started = false;
        let mut item_done = false;
        let mut failed = false;

        for entry in &wal_entries {
            match entry {
                WalEntry::ItemStart {
                    item_id: wal_item, ..
                } if wal_item == item_id => {
                    item_started = true;
                }
                WalEntry::BatchBegin {
                    item_id: wal_item,
                    part_id,
                    ..
                }
                | WalEntry::BatchCommit {
                    item_id: wal_item,
                    part_id,
                    ..
                } if wal_item == item_id => {
                    part_hint = Some(part_id.clone());
                    item_started = true;
                }
                WalEntry::Heartbeat {
                    item_id: wal_item,
                    part_id,
                    at,
                    ..
                } if wal_item == item_id => {
                    part_hint = Some(part_id.clone());
                    last_heartbeat = Some(*at);
                }
                WalEntry::CircuitBreakerOpen {
                    item_id: wal_item, ..
                } if wal_item == item_id => {
                    failed = true;
                }
                WalEntry::ItemDone {
                    item_id: wal_item, ..
                } if wal_item == item_id => {
                    item_done = true;
                }
                _ => {}
            }
        }

        let checkpoint = self
            .load_checkpoint(run_id, item_id, part_hint.as_deref())
            .await?;

        let (checkpoint_stage, last_cursor, rows_done) = if let Some(cp) = checkpoint {
            let cursor = cp
                .pending_offset
                .clone()
                .unwrap_or_else(|| cp.src_offset.clone());
            (Some(cp.stage), cursor, cp.rows_done)
        } else {
            (None, Cursor::None, 0)
        };

        let stage = if failed {
            ProgressStage::Failed
        } else if item_done {
            ProgressStage::Done
        } else if let Some(stage) = checkpoint_stage {
            stage_from_checkpoint(&stage)
        } else if item_started {
            ProgressStage::Snapshot
        } else {
            ProgressStage::Idle
        };

        Ok(ProgressStatus {
            stage,
            last_cursor,
            rows_done,
            last_heartbeat,
        })
    }

    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_hint: Option<&str>,
    ) -> Result<Option<Checkpoint>, StateStoreError> {
        if let Some(part_id) = part_hint
            && let Some(cp) = self.store.load_checkpoint(run_id, item_id, part_id).await?
        {
            return Ok(Some(cp));
        }

        if part_hint != Some("part-0")
            && let Some(cp) = self
                .store
                .load_checkpoint(run_id, item_id, "part-0")
                .await?
        {
            return Ok(Some(cp));
        }

        Ok(None)
    }
}

fn stage_from_checkpoint(stage: &CheckpointStage) -> ProgressStage {
    match stage {
        CheckpointStage::Read | CheckpointStage::Write => ProgressStage::Snapshot,
        CheckpointStage::Committed => ProgressStage::Running,
        CheckpointStage::Validated => ProgressStage::Validating,
        _ => ProgressStage::Idle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_state::StateStore;
    use model::pagination::cursor::Cursor;
    use tempfile::tempdir;

    const RUN_ID: &str = "test-run";
    const ITEM_ID: &str = "test-item";
    const PART_ID: &str = "part-0";

    fn checkpoint(
        stage: CheckpointStage,
        cursor: Cursor,
        pending: Option<Cursor>,
        rows: u64,
    ) -> Checkpoint {
        Checkpoint {
            run_id: RUN_ID.to_string(),
            item_id: ITEM_ID.to_string(),
            part_id: PART_ID.to_string(),
            stage,
            src_offset: cursor,
            pending_offset: pending,
            batch_id: "batch".into(),
            rows_done: rows,
            updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn reports_snapshot_stage_with_checkpoint() {
        let dir = tempdir().unwrap();
        let store: Arc<StateStore> =
            Arc::new(StateStore::open(dir.path()).expect("open state store"));
        let service = ProgressService::new(store.clone());

        store
            .append_wal(&WalEntry::ItemStart {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .unwrap();
        store
            .append_wal(&WalEntry::Heartbeat {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
                part_id: PART_ID.to_string(),
                at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        store
            .save_checkpoint(&checkpoint(
                CheckpointStage::Read,
                Cursor::Default { offset: 1 },
                Some(Cursor::Default { offset: 2 }),
                10,
            ))
            .await
            .unwrap();

        let status = service.item_status(RUN_ID, ITEM_ID).await.unwrap();
        assert_eq!(status.stage, ProgressStage::Snapshot);
        assert_eq!(status.rows_done, 10);
        assert_eq!(
            status.last_cursor,
            Cursor::Default { offset: 2 },
            "pending cursor is reported when available"
        );
        assert!(status.last_heartbeat.is_some());
    }

    #[tokio::test]
    async fn reports_done_stage_when_item_finished() {
        let dir = tempdir().unwrap();
        let store: Arc<StateStore> =
            Arc::new(StateStore::open(dir.path()).expect("open state store"));
        let service = ProgressService::new(store.clone());

        store
            .append_wal(&WalEntry::ItemDone {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .unwrap();

        let status = service.item_status(RUN_ID, ITEM_ID).await.unwrap();
        assert_eq!(status.stage, ProgressStage::Done);
        assert_eq!(status.rows_done, 0);
    }

    #[tokio::test]
    async fn reports_failed_stage_when_breaker_tripped() {
        let dir = tempdir().unwrap();
        let store: Arc<StateStore> =
            Arc::new(StateStore::open(dir.path()).expect("open state store"));
        let service = ProgressService::new(store.clone());

        store
            .append_wal(&WalEntry::CircuitBreakerOpen {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
                part_id: PART_ID.to_string(),
                stage: "write".to_string(),
                failures: 3,
                last_error: "boom".into(),
            })
            .await
            .unwrap();

        let status = service.item_status(RUN_ID, ITEM_ID).await.unwrap();
        assert_eq!(status.stage, ProgressStage::Failed);
    }
}
