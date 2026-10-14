use crate::error::StateStoreError;
use rusqlite::ErrorCode;

mod db;
pub mod receipts;
pub mod runs;
mod schema;

pub use db::StateStore;

/// Storage-layer failure.
#[inline]
pub(super) fn to_storage(e: impl std::fmt::Display) -> StateStoreError {
    StateStoreError::Storage(e.to_string())
}

/// SQLite failure, keeping "another process holds the lock" distinct.
pub(super) fn to_sqlite(e: rusqlite::Error) -> StateStoreError {
    match e.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => StateStoreError::Busy,
        _ => to_storage(e),
    }
}

/// Encoding or decoding failure for a stored value.
#[inline]
pub(super) fn to_ser(e: impl std::fmt::Display) -> StateStoreError {
    StateStoreError::Serialization(e.to_string())
}
