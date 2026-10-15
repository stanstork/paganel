use crate::error::VerifyError;
use engine_state::{RowHashLog, StateStore, default_state_dir};
use std::{path::Path, sync::Arc};

/// The state a verify run reads: receipts in the state store, row hashes on disk.
#[derive(Clone)]
pub struct VerifyState {
    pub store: Arc<StateStore>,
    pub hash_log: Arc<RowHashLog>,
}

impl VerifyState {
    /// Open the store and row-hash log in `state_dir`.
    pub fn open(state_dir: impl AsRef<Path>) -> Result<Self, VerifyError> {
        let dir = state_dir.as_ref();
        // Keep the store's error typed, so a busy store reads as busy.
        let store = StateStore::open(dir)?;

        Ok(Self {
            store: Arc::new(store),
            hash_log: Arc::new(RowHashLog::in_state_dir(dir)),
        })
    }

    /// Open the default state directory (`~/.paganel/state`).
    pub fn open_default() -> Result<Self, VerifyError> {
        Self::open(default_state_dir()?)
    }
}
