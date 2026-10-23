use crate::error::StateStoreError;
use crate::store::{to_sqlite, to_storage};
use rusqlite::{Connection, TransactionBehavior};
use std::cmp::Ordering;

const MIGRATIONS: &[&str] = &[
    include_str!("migrations/0001_init.sql"),
    include_str!("migrations/0002_imports.sql"),
    include_str!("migrations/0003_drift_runs.sql"),
];

/// Bring the schema up to date.
pub(super) fn migrate(conn: &mut Connection) -> Result<(), StateStoreError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(to_sqlite)?;

    let applied: usize = tx
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(to_storage)?
        .try_into()
        .map_err(to_storage)?;

    let target_version = MIGRATIONS.len();

    match applied.cmp(&target_version) {
        Ordering::Greater => {
            return Err(StateStoreError::Storage(format!(
                "state store schema version {applied} is newer than this version of Paganel supports ({target_version}); upgrade Paganel"
            )));
        }
        Ordering::Equal => {
            // Already up to date, short-circuit and drop the transaction
            return Ok(());
        }
        Ordering::Less => {
            // Apply all pending migrations
            for sql in &MIGRATIONS[applied..] {
                tx.execute_batch(sql).map_err(to_sqlite)?;
            }

            tx.pragma_update(None, "user_version", target_version as i64)
                .map_err(to_storage)?;

            tx.commit().map_err(to_sqlite)?;
        }
    }

    Ok(())
}

/// Number of migrations this build knows about.
#[cfg(test)]
pub(super) fn latest_version() -> usize {
    MIGRATIONS.len()
}
