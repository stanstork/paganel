use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateStoreError {
    #[error("Failed to save checkpoint: {0}")]
    SaveCheckpoint(String),

    #[error("Failed to load checkpoint: {0}")]
    LoadCheckpoint(String),

    #[error("Failed to append WAL entry: {0}")]
    AppendWAL(String),

    #[error("Failed to iterate WAL entries: {0}")]
    IterateWAL(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error(
        "A migration for this config is already running (run {run_id}).\n
        Wait for it to finish, or use `pag status` and `pag pause`."
    )]
    RunLocked { run_id: String },

    #[error("State store is busy: another process holds its lock")]
    Busy,

    #[error("Poisoned mutex: {0}")]
    PoisonedMutex(String),
}
