use crate::{error::ProducerError, profile};
use engine_infra::event_bus::bus::EventBus;
use engine_state::{RowHashLog, RowHashScope, StateStore, error::StateStoreError, fold_root};
use model::{
    events::migration::MigrationEvent,
    integrity::{
        config::IntegrityConfig, hasher::RowHasher, receipt::VerificationReceipt,
        row_key::KeyedRowHash,
    },
    records::Record,
};
use std::sync::Arc;
use std::{collections::HashMap, time::Instant};
use tracing::{info, warn};

/// Hashes migrated rows and streams the `(row key, row hash)` pairs to the
/// row-hash store as each batch is produced.
pub struct IntegrityState {
    /// One hasher per destination table, keyed by table name.
    hashers: HashMap<String, RowHasher>,
    hash_log: Arc<RowHashLog>,
    pipeline: String,
    config: IntegrityConfig,
    /// Fast-path index lookups to avoid String allocations per batch.
    table_names: Vec<String>,
    table_indices: HashMap<String, usize>,
    /// Latch so untracked rows are reported once, not once per batch.
    warned_untracked: bool,
}

impl IntegrityState {
    pub fn new(
        config: IntegrityConfig,
        hash_log: Arc<RowHashLog>,
        pipeline: impl Into<String>,
    ) -> Self {
        let capacity = config.tables.len();
        let mut hashers = HashMap::with_capacity(capacity);
        let mut table_names = Vec::with_capacity(capacity);
        let mut table_indices = HashMap::with_capacity(capacity);

        for (i, (table, cols)) in config.tables.iter().enumerate() {
            hashers.insert(
                table.clone(),
                RowHasher::new(cols.clone(), config.algorithm),
            );
            table_names.push(table.clone());
            table_indices.insert(table.clone(), i);
        }

        Self {
            hashers,
            hash_log,
            pipeline: pipeline.into(),
            config,
            table_names,
            table_indices,
            warned_untracked: false,
        }
    }

    /// Hash every row in `rows` and persist the results, grouped by destination table.
    pub fn hash_batch(&mut self, rows: &[Record]) -> Result<(), StateStoreError> {
        if rows.is_empty() {
            return Ok(());
        }

        let t_hash = Instant::now();
        let hashed = tokio::task::block_in_place(|| self.hash_grouped(rows));
        profile::record_stage("integrity: hash+key", t_hash.elapsed());

        let t_store = Instant::now();
        for (table_idx, entries) in hashed {
            self.hash_log.append(
                RowHashScope::Baseline,
                &self.pipeline,
                &self.table_names[table_idx],
                &entries,
            )?;
        }
        profile::record_stage("integrity: append", t_store.elapsed());

        Ok(())
    }

    /// Group the batch by destination table and hash each group.
    fn hash_grouped(&mut self, rows: &[Record]) -> Vec<(usize, Vec<KeyedRowHash>)> {
        let mut groups = vec![Vec::new(); self.table_names.len()];
        let single_table = self.table_names.len() == 1;
        let mut untracked = 0usize;

        for row in rows {
            let idx = self
                .table_indices
                .get(row.table())
                .copied()
                .or_else(|| self.match_table_ci(row.table()));

            match idx {
                Some(idx) => groups[idx].push(row),
                None if single_table => groups[0].push(row),
                None => untracked += 1,
            }
        }

        if untracked > 0 && !self.warned_untracked {
            self.warned_untracked = true;
            warn!(
                rows = untracked,
                "integrity: rows belong to a table with no destination metadata; \
                 they are excluded from the verification receipt"
            );
        }

        let mut out = Vec::with_capacity(groups.len());

        for (idx, table_rows) in groups.into_iter().enumerate() {
            if table_rows.is_empty() {
                continue;
            }

            let table = &self.table_names[idx];
            let hasher = self.hashers.get_mut(table).expect("hasher pre-populated");

            let entries = hasher.hash_rows(
                &table_rows,
                self.config.types(table),
                self.config.keys(table),
            );

            out.push((idx, entries));
        }

        out
    }

    /// Case-insensitive fallback lookup for a destination table.
    fn match_table_ci(&self, table: &str) -> Option<usize> {
        self.table_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(table))
    }
}

/// Drop any row hashes left over from a previous run of this pipeline.
pub fn reset_row_hashes(
    config: &IntegrityConfig,
    hash_log: &RowHashLog,
    pipeline: &str,
) -> Result<(), StateStoreError> {
    for table in config.tables.keys() {
        hash_log.clear(RowHashScope::Baseline, pipeline, table)?;
    }
    Ok(())
}

/// Fold each table's stored row hashes into a Merkle root and write its receipt.
#[allow(clippy::too_many_arguments)]
pub async fn finalize_receipts(
    config: &IntegrityConfig,
    hash_log: &RowHashLog,
    receipts: &Arc<StateStore>,
    event_bus: &EventBus,
    pipeline_name: &str,
    item_id: &str,
    primary_table: &str,
    run_id: &str,
    skipped_rows: u64,
) -> Result<(), ProducerError> {
    event_bus
        .publish(MigrationEvent::IntegrityStarted {
            run_id: run_id.to_string(),
            item_id: item_id.to_string(),
            tables: config.tables.len(),
            timestamp: chrono::Utc::now(),
        })
        .await;

    let mut receipts_written = 0usize;

    for table in config.tables.keys() {
        let algorithm = config.algorithm;

        // Announce the seal before the heavy sort/merge/fold.
        event_bus
            .publish(MigrationEvent::IntegritySealing {
                run_id: run_id.to_string(),
                item_id: item_id.to_string(),
                table: table.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;

        let (total_rows, table_root) = tokio::task::block_in_place(|| {
            let t_seal = Instant::now();
            hash_log.seal(RowHashScope::Baseline, pipeline_name, table)?;
            profile::record_stage("integrity: seal (sort)", t_seal.elapsed());

            let t_fold = Instant::now();
            let iter = hash_log.stream(RowHashScope::Baseline, pipeline_name, table)?;
            let folded = fold_root(iter, algorithm)?;
            profile::record_stage("integrity: merkle fold", t_fold.elapsed());

            Ok::<_, StateStoreError>(folded)
        })?;

        // A table with no rows produced no receipt before.
        if total_rows == 0 {
            continue;
        }

        // `skipped_rows` is a pipeline-wide DLQ count, it belongs to the primary destination table.
        let skipped_rows = if table.eq_ignore_ascii_case(primary_table) {
            skipped_rows
        } else {
            0
        };

        let receipt = VerificationReceipt {
            run_id: run_id.to_string(),
            pipeline_name: pipeline_name.to_string(),
            table_name: table.clone(),
            table_root,
            column_order: config.columns(table).to_vec(),
            key_columns: config.keys(table).to_vec(),
            total_rows,
            skipped_rows,
            algorithm,
            created_at: chrono::Utc::now(),
        };

        info!(
            table,
            rows = total_rows,
            root = %receipt.root_hex(),
            "integrity receipt written"
        );
        receipts.save_receipt(&receipt).await?;

        receipts_written += 1;
        event_bus
            .publish(MigrationEvent::IntegrityReceipt {
                run_id: run_id.to_string(),
                item_id: item_id.to_string(),
                table: table.clone(),
                rows: total_rows,
                root: receipt.root_hex(),
                timestamp: chrono::Utc::now(),
            })
            .await;
    }

    event_bus
        .publish(MigrationEvent::IntegrityCompleted {
            run_id: run_id.to_string(),
            item_id: item_id.to_string(),
            receipts: receipts_written,
            timestamp: chrono::Utc::now(),
        })
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::core::value::{FieldValue, Value};
    use model::integrity::algorithm::HashAlgorithm;
    use model::records::OpType;

    fn row(table: &str, id: i64) -> Record {
        Record::from_fields(
            table,
            vec![FieldValue {
                name: "id".to_string(),
                value: Some(Value::Int(id)),
                data_type: Value::Int(id).data_type(),
            }],
            OpType::Insert,
        )
    }

    /// A cascade row whose table name differs only in case from the tracked
    /// destination must still land in that table's group - not get dropped as
    /// untracked, which would surface as a false `extra` at verify time.
    #[test]
    fn rows_route_to_a_case_insensitively_matching_table() {
        let mut tables = HashMap::new();
        tables.insert("Actor".to_string(), vec!["id".to_string()]);
        tables.insert("Film".to_string(), vec!["id".to_string()]);
        let config = IntegrityConfig::new(HashAlgorithm::Sha256, tables);

        // hash_grouped performs no IO, so the log root is never touched.
        let hash_log = Arc::new(RowHashLog::new("unused"));
        let mut state = IntegrityState::new(config, hash_log, "p");

        let grouped = state.hash_grouped(&[row("actor", 1)]);

        assert_eq!(grouped.len(), 1, "the row must be grouped, not dropped");
        let (idx, entries) = &grouped[0];
        assert_eq!(entries.len(), 1);
        assert!(
            state.table_names[*idx].eq_ignore_ascii_case("actor"),
            "routed to the case-insensitively matching table"
        );
        assert!(
            !state.warned_untracked,
            "a case-only difference is not an untracked table"
        );
    }

    /// A row for a table that is genuinely not tracked is still dropped and
    /// flagged, in a multi-table (cascade) config.
    #[test]
    fn genuinely_unknown_table_is_reported_untracked() {
        let mut tables = HashMap::new();
        tables.insert("actor".to_string(), vec!["id".to_string()]);
        tables.insert("film".to_string(), vec!["id".to_string()]);
        let config = IntegrityConfig::new(HashAlgorithm::Sha256, tables);

        let hash_log = Arc::new(RowHashLog::new("unused"));
        let mut state = IntegrityState::new(config, hash_log, "p");

        let grouped = state.hash_grouped(&[row("category", 1)]);

        assert!(grouped.is_empty(), "no group for an unknown table");
        assert!(state.warned_untracked);
    }
}
