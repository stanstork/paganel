use crate::item::ItemId;
use engine_core::error::StateStoreError;
use engine_state::models::CheckpointStage;
use engine_state::{
    StateStore,
    models::{Checkpoint, CheckpointSummary, WalEntry},
};
use model::pagination::cursor::Cursor;
use std::sync::Arc;
use tracing::{debug, warn};

/// Manages checkpoint and WAL operations.
pub struct StateManager {
    ids: ItemId,
    store: Arc<StateStore>,
}

impl StateManager {
    pub fn new(ids: ItemId, store: Arc<StateStore>) -> Self {
        Self { ids, store }
    }

    /// Commit a batch: its WAL entry and its `Committed` checkpoint (resume
    /// point `next`, running total `rows_done`), written together.
    pub async fn commit_batch(
        &self,
        batch_id: &str,
        next: &Cursor,
        rows_done: u64,
    ) -> Result<(), StateStoreError> {
        let now = chrono::Utc::now();
        let entry = WalEntry::BatchCommit {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            batch_id: batch_id.to_string(),
            ts: now,
        };
        let checkpoint = Checkpoint {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            stage: CheckpointStage::Committed,
            src_offset: next.clone(),
            pending_offset: None,
            batch_id: batch_id.to_string(),
            rows_done,
            updated_at: now,
        };

        self.store.commit_batch(&entry, &checkpoint).await
    }

    /// Save a checkpoint with specific stage and cursor information.
    pub async fn save_checkpoint(
        &self,
        stage: &CheckpointStage,
        src_offset: &Cursor,
        pending_offset: Option<&Cursor>,
        batch_id: &str,
        rows_done: u64,
    ) -> Result<(), StateStoreError> {
        self.store
            .save_checkpoint(&Checkpoint {
                run_id: self.ids.run_id(),
                item_id: self.ids.item_id(),
                part_id: self.ids.part_id(),
                stage: *stage,
                src_offset: src_offset.clone(),
                pending_offset: pending_offset.cloned(),
                batch_id: batch_id.to_string(),
                rows_done,
                updated_at: chrono::Utc::now(),
            })
            .await
    }

    /// Load the current checkpoint.
    pub async fn load_checkpoint(&self) -> Result<Option<Checkpoint>, StateStoreError> {
        self.store
            .load_checkpoint(&self.ids.run_id(), &self.ids.item_id(), &self.ids.part_id())
            .await
    }

    /// Resume from the last checkpoint, determining the correct cursor.
    pub async fn resume_cursor(&self) -> Result<Cursor, StateStoreError> {
        let summary = self
            .store
            .last_checkpoint(&self.ids.run_id(), &self.ids.item_id(), &self.ids.part_id())
            .await?;

        match summary {
            Some(s) => {
                debug!(
                    stage = %s.stage,
                    batch_id = %s.batch_id,
                    "resuming from checkpoint"
                );
                Ok(self.cursor_from_checkpoint(&s).await)
            }
            None => {
                debug!("no checkpoint found, starting from beginning");
                Ok(Cursor::None)
            }
        }
    }

    pub fn ids(&self) -> &ItemId {
        &self.ids
    }

    /// Reconstruct the correct resume cursor based on checkpoint and WAL.
    ///
    /// Rules:
    /// - If stage="committed": resume from `src_offset` (fully committed)
    /// - If stage="read"/"write":
    ///     - If WAL contains BatchCommit for this batch -> resume from `pending_offset`
    ///     - Otherwise -> resume from `src_offset`
    /// - Otherwise: fallback to `src_offset`
    async fn cursor_from_checkpoint(&self, summary: &CheckpointSummary) -> Cursor {
        match summary.stage {
            CheckpointStage::Committed => {
                // Batch was fully committed, safe to continue from src_offset
                debug!(
                    cursor = ?summary.src_offset,
                    "resuming from committed checkpoint"
                );
                summary.src_offset.clone()
            }

            CheckpointStage::Read | CheckpointStage::Write => {
                // Batch was in progress - need to check if it was actually written
                let wal_entries = match self.store.iter_wal(&self.ids.run_id()).await {
                    Ok(entries) => entries,
                    Err(err) => {
                        warn!(error = %err, "failed to read WAL entries, using safe cursor");
                        return summary.src_offset.clone();
                    }
                };

                if Self::wal_has_commit(&wal_entries, &self.ids, &summary.batch_id) {
                    // Commit found in WAL - batch was written successfully
                    let resume_cursor = summary
                        .pending_offset
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| summary.src_offset.clone());

                    debug!(
                        cursor = ?resume_cursor,
                        batch_id = %summary.batch_id,
                        "batch was committed, resuming from pending_offset"
                    );
                    resume_cursor
                } else {
                    // No commit in WAL - batch was lost, need to re-read
                    warn!(
                        cursor = ?summary.src_offset,
                        batch_id = %summary.batch_id,
                        "batch not committed, resuming from src_offset (will re-read)"
                    );
                    summary.src_offset.clone()
                }
            }

            stage => {
                // Unknown stage - use safe default
                warn!(
                    stage = %stage,
                    cursor = ?summary.src_offset,
                    "unknown checkpoint stage, using src_offset"
                );
                summary.src_offset.clone()
            }
        }
    }

    /// Check if WAL contains a commit entry for a specific batch.
    fn wal_has_commit(entries: &[WalEntry], ids: &ItemId, batch_id: &str) -> bool {
        entries.iter().rev().any(|entry| {
            matches!(entry,
                WalEntry::BatchCommit { item_id, part_id, batch_id: b_id, .. }
                if *item_id == ids.item_id() && *part_id == ids.part_id() && b_id == batch_id
            )
        })
    }
}
