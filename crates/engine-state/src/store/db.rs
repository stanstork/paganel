use crate::error::StateStoreError;
use crate::store::{schema, to_sqlite, to_storage};
use rusqlite::Connection;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    thread::JoinHandle,
    time::Duration,
};
use tracing::warn;

const DB_FILE: &str = "state.db";
const CACHE_CAPACITY_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const WAL_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Receipts, checkpoints, WAL, and run state.
pub struct StateStore {
    pub(super) conn: Arc<Mutex<Connection>>,
    pub(super) dir: PathBuf,
    _wal_flusher: WalFlusher,
}

impl StateStore {
    /// Open (creating if needed) the store in `path` and bring its schema up to date.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateStoreError> {
        let dir = path.as_ref();
        std::fs::create_dir_all(dir).map_err(to_storage)?;

        let db_path = dir.join(DB_FILE);
        let mut conn = Connection::open(&db_path).map_err(to_sqlite)?;
        configure(&conn)?;
        schema::migrate(&mut conn)?;

        // WAL flushing moves to a background thread, so no write pays for it.
        conn.pragma_update(None, "wal_autocheckpoint", 0)
            .map_err(to_sqlite)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            dir: dir.to_path_buf(),
            _wal_flusher: WalFlusher::start(db_path)?,
        })
    }

    /// Open the store, or return `None` when another process keeps the
    /// database locked for longer than the busy timeout.
    pub fn try_open(path: impl AsRef<Path>) -> Result<Option<Self>, StateStoreError> {
        match Self::open(path) {
            Ok(store) => Ok(Some(store)),
            Err(StateStoreError::Busy) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Directory backing this store.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Run blocking SQLite work off the async runtime.
    pub(super) async fn with_conn<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, StateStoreError> + Send + 'static,
    ) -> Result<T, StateStoreError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = conn
                .lock()
                .map_err(|e| StateStoreError::PoisonedMutex(e.to_string()))?;
            f(&mut conn)
        })
        .await
        .map_err(to_storage)?
    }
}

/// Flushes the WAL into the database file on its own connection.
struct WalFlusher {
    stop: Option<mpsc::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl WalFlusher {
    fn start(db_path: PathBuf) -> Result<Self, StateStoreError> {
        let conn = Connection::open(db_path).map_err(to_sqlite)?;
        conn.busy_timeout(BUSY_TIMEOUT).map_err(to_sqlite)?;

        let (stop, stopped) = mpsc::channel::<()>();
        let handle = std::thread::Builder::new()
            .name("state-wal-flush".into())
            .spawn(move || {
                loop {
                    let last = match stopped.recv_timeout(WAL_FLUSH_INTERVAL) {
                        Err(RecvTimeoutError::Timeout) => false,
                        // Sender dropped: the store is closing.
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => true,
                    };

                    if let Err(e) = flush_wal(&conn) {
                        warn!(error = %e, "state store WAL flush failed");
                    }

                    if last {
                        break;
                    }
                }
            })
            .map_err(to_storage)?;

        Ok(Self {
            stop: Some(stop),
            handle: Some(handle),
        })
    }
}

/// One passive WAL checkpoint. Returns (frames in the WAL, frames copied back).
fn flush_wal(conn: &Connection) -> rusqlite::Result<(i64, i64)> {
    conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
        Ok((row.get(1)?, row.get(2)?))
    })
}

impl Drop for WalFlusher {
    fn drop(&mut self) {
        // Closing the channel wakes the thread for one final flush.
        self.stop.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Connection settings applied on every open.
fn configure(conn: &Connection) -> Result<(), StateStoreError> {
    conn.busy_timeout(BUSY_TIMEOUT).map_err(to_sqlite)?;

    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(to_sqlite)?;

    if !mode.eq_ignore_ascii_case("wal") {
        warn!(
            mode = %mode,
            "state store could not enable WAL mode; status and verify will wait for a running migration"
        );
    }

    // Negative cache_size is in KiB.
    let cache_kib = -((CACHE_CAPACITY_BYTES / 1024) as i64);

    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(to_sqlite)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(to_sqlite)?;
    conn.pragma_update(None, "cache_size", cache_kib)
        .map_err(to_sqlite)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn user_version(store: &StateStore) -> i64 {
        let conn = store.conn.lock().unwrap();
        conn.query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wal_is_flushed_off_the_write_path() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        let autocheckpoint: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .unwrap();
        assert_eq!(autocheckpoint, 0, "commits must not flush the WAL inline");

        store
            .append_wal(&crate::models::WalEntry::RunDone {
                run_id: "run".into(),
            })
            .await
            .unwrap();

        // What the background thread runs each tick: it copies every frame back.
        let other = Connection::open(dir.path().join(DB_FILE)).unwrap();
        let (frames, copied) = flush_wal(&other).unwrap();
        assert!(frames > 0, "the write should be sitting in the WAL");
        assert_eq!(frames, copied);

        // Dropping the store stops the thread without hanging.
        drop(store);
    }

    #[test]
    fn open_creates_the_database_in_wal_mode() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();

        assert!(dir.path().join(DB_FILE).is_file());
        let mode: String = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        assert_eq!(user_version(&store) as usize, schema::latest_version());
    }

    #[test]
    fn reopening_does_not_reapply_migrations() {
        let dir = tempdir().unwrap();
        drop(StateStore::open(dir.path()).unwrap());

        // A second run of migration 1 would fail on CREATE TABLE.
        let store = StateStore::open(dir.path()).unwrap();
        assert_eq!(user_version(&store) as usize, schema::latest_version());
    }

    #[test]
    fn two_handles_on_one_file_both_open() {
        let dir = tempdir().unwrap();
        let _a = StateStore::open(dir.path()).unwrap();
        let b = StateStore::try_open(dir.path()).unwrap();
        assert!(
            b.is_some(),
            "a second process must be able to open the store"
        );
    }

    #[test]
    fn refuses_a_schema_newer_than_this_build() {
        let dir = tempdir().unwrap();
        {
            let store = StateStore::open(dir.path()).unwrap();
            let conn = store.conn.lock().unwrap();
            conn.pragma_update(None, "user_version", 999).unwrap();
        }

        let err = StateStore::open(dir.path()).err().unwrap();
        assert!(err.to_string().contains("newer"), "{err}");
    }
}
