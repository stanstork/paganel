use crate::{error::StateStoreError, store::to_storage};
use std::{
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
};

/// Exclusive claim on one run, held for as long as this value lives.
#[derive(Debug)]
pub struct RunLock {
    _file: File,
    path: PathBuf,
}

impl RunLock {
    /// Claim `run_id`, failing with `RunLocked` if another process holds it.
    pub fn acquire(state_dir: impl AsRef<Path>, run_id: &str) -> Result<Self, StateStoreError> {
        let dir = state_dir.as_ref().join("locks");
        std::fs::create_dir_all(&dir).map_err(to_storage)?;

        let path = dir.join(format!("{run_id}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(to_storage)?;

        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(StateStoreError::RunLocked {
                run_id: run_id.to_string(),
            }),
            Err(TryLockError::Error(e)) => Err(to_storage(e)),
        }
    }

    /// The lock file backing this claim.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn a_held_run_cannot_be_claimed_again() {
        let dir = tempdir().unwrap();
        let _held = RunLock::acquire(dir.path(), "run-a").unwrap();

        let err = RunLock::acquire(dir.path(), "run-a").unwrap_err();
        assert!(
            matches!(err, StateStoreError::RunLocked { ref run_id } if run_id == "run-a"),
            "{err}"
        );
    }

    #[test]
    fn other_runs_are_not_blocked() {
        let dir = tempdir().unwrap();
        let _a = RunLock::acquire(dir.path(), "run-a").unwrap();
        assert!(RunLock::acquire(dir.path(), "run-b").is_ok());
    }

    #[test]
    fn dropping_the_lock_releases_the_run() {
        let dir = tempdir().unwrap();
        let held = RunLock::acquire(dir.path(), "run-a").unwrap();
        let path = held.path().to_path_buf();
        drop(held);

        assert!(RunLock::acquire(dir.path(), "run-a").is_ok());
        assert!(path.exists(), "the lock file stays on disk");
    }
}
