use crate::error::StateStoreError;
use merge::merge_runs;
use model::integrity::row_key::KeyedRowHash;
use record::{RecordReader, encode};
use sort::write_sorted_runs;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod fold;
mod merge;
mod record;
mod sort;

pub use fold::fold_root;

/// Which side of a verification a set of row hashes belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowHashScope {
    /// The committed set a receipt was folded from, written by `apply
    /// --integrity` or `state import`: the expected side of every diff.
    Baseline,
    /// Scratch space where verify re-hashes the live table.
    Verify,
}

impl RowHashScope {
    /// Directory this scope's sets live under.
    pub fn dir_name(self) -> &'static str {
        match self {
            RowHashScope::Baseline => "baseline",
            RowHashScope::Verify => "verify",
        }
    }
}

/// Boxed sorted iterator over a table's stored row hashes.
pub type RowHashIter = Box<dyn Iterator<Item = Result<KeyedRowHash, StateStoreError>> + Send>;

const PENDING: &str = "pending.log";
const SORTED: &str = "sorted.log";
const SORTED_TMP: &str = "sorted.tmp";

/// Append-only row-hash storage rooted at one directory.
pub struct RowHashLog {
    root: PathBuf,
    seq: AtomicU64,
    writers: Mutex<HashMap<PathBuf, Arc<Mutex<BufWriter<File>>>>>,
}

impl RowHashLog {
    /// Rooted beside the key-value store inside the state directory.
    pub fn in_state_dir(state_dir: impl AsRef<Path>) -> Self {
        Self::new(state_dir.as_ref().join("rowhash"))
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            seq: AtomicU64::new(0),
            writers: Mutex::new(HashMap::new()),
        }
    }

    /// Append a batch. Lanes share one appender.
    pub fn append(
        &self,
        scope: RowHashScope,
        pipeline: &str,
        table: &str,
        entries: &[KeyedRowHash],
    ) -> Result<(), StateStoreError> {
        if entries.is_empty() {
            return Ok(());
        }

        let base = self.seq.fetch_add(entries.len() as u64, Ordering::Relaxed);
        let mut buf = Vec::with_capacity(entries.len() * 64);

        for (i, entry) in entries.iter().enumerate() {
            encode(entry, base.wrapping_add(i as u64), &mut buf)?;
        }

        let writer = self.appender(scope, pipeline, table)?;
        let mut guard = writer.lock().map_err(|_| poisoned())?;

        guard.write_all(&buf).map_err(storage)
    }

    /// Sort, deduplicate, and seal the set so it can be streamed in key order.
    pub fn seal(
        &self,
        scope: RowHashScope,
        pipeline: &str,
        table: &str,
    ) -> Result<(), StateStoreError> {
        let dir = self.dir(scope, pipeline, table);
        self.close_appender(&dir)?;

        let pending = dir.join(PENDING);
        if !pending.exists() {
            return Ok(());
        }

        let sorted = dir.join(SORTED);
        let mut inputs: Vec<(PathBuf, u32)> = Vec::with_capacity(2);

        if sorted.exists() {
            inputs.push((sorted.clone(), 0));
        }
        inputs.push((pending.clone(), 1));

        let tmp = dir.join(SORTED_TMP);
        let runs = write_sorted_runs(&inputs, &dir)?;

        match runs.as_slice() {
            [] => {
                File::create(&tmp).map_err(storage)?;
            }
            [single] => {
                fs::rename(single, &tmp).map_err(storage)?;
            }
            _ => merge_runs(&runs, &tmp)?,
        }

        fs::rename(&tmp, &sorted).map_err(storage)?;
        fs::remove_file(&pending).map_err(storage)?;

        for run in &runs {
            let _ = fs::remove_file(run);
        }

        Ok(())
    }

    /// Stream the sealed set in ascending key order. Empty if nothing was sealed.
    pub fn stream(
        &self,
        scope: RowHashScope,
        pipeline: &str,
        table: &str,
    ) -> Result<RowHashIter, StateStoreError> {
        let path = self.dir(scope, pipeline, table).join(SORTED);

        if !path.exists() {
            return Ok(Box::new(std::iter::empty()));
        }

        Ok(Box::new(RecordReader::open(&path)?))
    }

    /// Delete a set outright - pending writes, runs, and sealed output alike.
    pub fn clear(
        &self,
        scope: RowHashScope,
        pipeline: &str,
        table: &str,
    ) -> Result<(), StateStoreError> {
        let dir = self.dir(scope, pipeline, table);
        self.close_appender(&dir)?;

        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(storage(e)),
        }
    }

    /// Delete every stored set for `pipeline`, across both scopes and all its tables.
    pub fn clear_pipeline(&self, pipeline: &str) -> Result<(), StateStoreError> {
        let sanitized = sanitize(pipeline);

        for scope in [RowHashScope::Baseline, RowHashScope::Verify] {
            let dir = self.root.join(scope.dir_name()).join(&sanitized);
            self.close_appenders_under(&dir)?;

            match fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(storage(e)),
            }
        }

        Ok(())
    }

    fn dir(&self, scope: RowHashScope, pipeline: &str, table: &str) -> PathBuf {
        self.root
            .join(scope.dir_name())
            .join(sanitize(pipeline))
            .join(sanitize(table))
    }

    fn appender(
        &self,
        scope: RowHashScope,
        pipeline: &str,
        table: &str,
    ) -> Result<Arc<Mutex<BufWriter<File>>>, StateStoreError> {
        let dir = self.dir(scope, pipeline, table);
        let mut writers = self.writers.lock().map_err(|_| poisoned())?;

        if let Some(existing) = writers.get(&dir) {
            return Ok(existing.clone());
        }

        fs::create_dir_all(&dir).map_err(storage)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(PENDING))
            .map_err(storage)?;

        let writer = Arc::new(Mutex::new(BufWriter::with_capacity(256 * 1024, file)));
        writers.insert(dir, writer.clone());

        Ok(writer)
    }

    /// Flush and drop the appender for `dir`, so the file on disk is complete.
    fn close_appender(&self, dir: &Path) -> Result<(), StateStoreError> {
        let writer = self.writers.lock().map_err(|_| poisoned())?.remove(dir);

        if let Some(writer) = writer {
            let mut guard = writer.lock().map_err(|_| poisoned())?;
            guard.flush().map_err(storage)?;
        }

        Ok(())
    }

    /// Flush and drop every appender whose directory sits under `prefix`, so a
    /// whole pipeline subtree can be removed safely.
    fn close_appenders_under(&self, prefix: &Path) -> Result<(), StateStoreError> {
        let mut writers = self.writers.lock().map_err(|_| poisoned())?;

        let matching: Vec<PathBuf> = writers
            .keys()
            .filter(|dir| dir.starts_with(prefix))
            .cloned()
            .collect();

        for dir in matching {
            if let Some(writer) = writers.remove(&dir) {
                writer
                    .lock()
                    .map_err(|_| poisoned())?
                    .flush()
                    .map_err(storage)?;
            }
        }

        Ok(())
    }
}

/// Keep pipeline and table names usable as directory names.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '@') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn storage(e: io::Error) -> StateStoreError {
    StateStoreError::Storage(e.to_string())
}

fn poisoned() -> StateStoreError {
    StateStoreError::Storage("row hash log lock poisoned".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::row_key::KeyedRowHash;
    use tempfile::tempdir;

    fn log(dir: &std::path::Path) -> RowHashLog {
        RowHashLog::new(dir.join("rowhash"))
    }

    fn entry(key: &[u8], hash: u8) -> KeyedRowHash {
        KeyedRowHash {
            key: key.to_vec(),
            hash: [hash; 32],
        }
    }

    fn sealed(log: &RowHashLog, scope: RowHashScope) -> Vec<KeyedRowHash> {
        log.seal(scope, "p", "t").unwrap();
        log.stream(scope, "p", "t")
            .unwrap()
            .map(|e| e.unwrap())
            .collect()
    }

    /// The whole order-independence argument rests on a sealed set coming back
    /// in key order no matter what order it went in.
    #[test]
    fn row_hashes_come_back_sorted_by_key() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(
            RowHashScope::Baseline,
            "p",
            "t",
            &[entry(b"c", 3), entry(b"a", 1)],
        )
        .unwrap();
        log.append(RowHashScope::Baseline, "p", "t", &[entry(b"b", 2)])
            .unwrap();

        let keys: Vec<Vec<u8>> = sealed(&log, RowHashScope::Baseline)
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    /// Re-emitted rows - batch retries, cascade fan-in, overlapping lanes - must
    /// collapse onto one entry, keeping the value written last.
    #[test]
    fn same_key_keeps_the_last_write() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(RowHashScope::Baseline, "p", "t", &[entry(b"k", 1)])
            .unwrap();
        log.append(RowHashScope::Baseline, "p", "t", &[entry(b"k", 9)])
            .unwrap();

        let all = sealed(&log, RowHashScope::Baseline);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].hash, [9u8; 32]);
    }

    /// Sealing twice must not lose the first seal's rows: an interrupted run
    /// leaves a sealed set that the resumed run extends.
    #[test]
    fn sealing_again_merges_with_the_previous_seal() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(
            RowHashScope::Baseline,
            "p",
            "t",
            &[entry(b"a", 1), entry(b"c", 3)],
        )
        .unwrap();
        assert_eq!(sealed(&log, RowHashScope::Baseline).len(), 2);

        // A later run adds a row and rewrites an existing one.
        log.append(
            RowHashScope::Baseline,
            "p",
            "t",
            &[entry(b"b", 2), entry(b"a", 7)],
        )
        .unwrap();

        let all = sealed(&log, RowHashScope::Baseline);
        let keys: Vec<Vec<u8>> = all.iter().map(|e| e.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(all[0].hash, [7u8; 32], "the later write wins across seals");
    }

    /// Verify stages the destination beside the migration's set; the two must
    /// not see each other.
    #[test]
    fn scopes_are_isolated_and_clearable() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(RowHashScope::Baseline, "p", "t", &[entry(b"k", 1)])
            .unwrap();
        log.append(RowHashScope::Verify, "p", "t", &[entry(b"k", 2)])
            .unwrap();

        assert_eq!(sealed(&log, RowHashScope::Baseline)[0].hash, [1u8; 32]);
        assert_eq!(sealed(&log, RowHashScope::Verify)[0].hash, [2u8; 32]);

        log.clear(RowHashScope::Verify, "p", "t").unwrap();
        assert!(sealed(&log, RowHashScope::Verify).is_empty());
        assert_eq!(
            sealed(&log, RowHashScope::Baseline).len(),
            1,
            "baseline scope survives"
        );
    }

    /// `reset` clears a pipeline's whole subtree - every table, both scopes -
    /// while leaving other pipelines' sets intact.
    #[test]
    fn clear_pipeline_removes_every_scope_and_table() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(RowHashScope::Baseline, "p", "t1", &[entry(b"a", 1)])
            .unwrap();
        log.append(RowHashScope::Baseline, "p", "t2", &[entry(b"b", 2)])
            .unwrap();
        log.append(RowHashScope::Verify, "p", "t1", &[entry(b"a", 1)])
            .unwrap();
        log.append(RowHashScope::Baseline, "other", "t1", &[entry(b"c", 3)])
            .unwrap();

        log.clear_pipeline("p").unwrap();

        for (scope, table) in [
            (RowHashScope::Baseline, "t1"),
            (RowHashScope::Baseline, "t2"),
            (RowHashScope::Verify, "t1"),
        ] {
            log.seal(scope, "p", table).unwrap();
            assert_eq!(
                log.stream(scope, "p", table).unwrap().count(),
                0,
                "{scope:?}/{table} should be gone"
            );
        }

        log.seal(RowHashScope::Baseline, "other", "t1").unwrap();
        assert_eq!(
            log.stream(RowHashScope::Baseline, "other", "t1")
                .unwrap()
                .count(),
            1,
            "an unrelated pipeline must survive"
        );
    }

    /// Tables inside one pipeline must not bleed together.
    #[test]
    fn tables_are_isolated() {
        let dir = tempdir().unwrap();
        let log = log(dir.path());

        log.append(RowHashScope::Baseline, "p", "t", &[entry(b"k", 1)])
            .unwrap();
        log.append(RowHashScope::Baseline, "p", "other", &[entry(b"k", 2)])
            .unwrap();

        assert_eq!(sealed(&log, RowHashScope::Baseline).len(), 1);
        log.seal(RowHashScope::Baseline, "p", "other").unwrap();
        assert_eq!(
            log.stream(RowHashScope::Baseline, "p", "other")
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn reserved_names_get_their_own_directory() {
        assert_eq!(sanitize("@import/orders"), "@import_orders");
        assert_ne!(sanitize("@import/orders"), sanitize("_import_orders"));
    }
}
