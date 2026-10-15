use crate::{diff::percent_of, error::VerifyError, reader::TableReader};
use connectors::{
    sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector,
    traits::reader::DataReader,
};
use engine_core::{dispatch_driver, drivers::DriverRef};
use engine_state::{PROGRESS_INTERVAL, RowHashLog, RowHashScope, Ticker};
use model::{
    integrity::{algorithm::HashAlgorithm, hasher::RowHasher, receipt::VerificationReceipt},
    pagination::cursor::Cursor,
    records::Record,
};
use query_builder::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tracing::info;

/// Rows fetched per read while hashing.
const READ_BATCH_ROWS: usize = 1000;

/// How rows are hashed. Both sides of a comparison must use the same spec, or
/// equal rows hash differently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashSpec {
    pub column_order: Vec<String>,
    pub key_columns: Vec<String>,
    pub algorithm: HashAlgorithm,
}

impl HashSpec {
    /// The spec a receipt was written with, to re-hash its table the same way.
    pub fn from_receipt(receipt: &VerificationReceipt) -> Self {
        Self {
            column_order: receipt.column_order.clone(),
            key_columns: receipt.key_columns.clone(),
            algorithm: receipt.algorithm,
        }
    }
}

/// Where hashed rows go.
#[derive(Clone, Copy)]
pub struct HashSink<'a> {
    pub log: &'a RowHashLog,
    pub scope: RowHashScope,
    pub pipeline: &'a str,
    pub table: &'a str,
}

pub async fn spec_from_table(
    driver: &DriverRef,
    table: &str,
    key_override: Option<&[String]>,
    ignore: &[String],
) -> Result<(HashSpec, TableMetadata), VerifyError> {
    let meta = fetch_table_metadata(driver, table).await?;
    let spec = build_spec(&meta, key_override, ignore)?;
    Ok((spec, meta))
}

fn build_spec(
    meta: &TableMetadata,
    key_override: Option<&[String]>,
    ignore: &[String],
) -> Result<HashSpec, VerifyError> {
    let has_column = |name: &str| meta.columns.keys().any(|c| c.eq_ignore_ascii_case(name));
    let unknown = |names: &[String]| -> Vec<String> {
        names.iter().filter(|n| !has_column(n)).cloned().collect()
    };

    let missing = unknown(ignore);
    if !missing.is_empty() {
        return Err(VerifyError::InitializationError(format!(
            "table '{}' has no column(s) {} to ignore",
            meta.name,
            missing.join(", ")
        )));
    }

    let key_columns = match key_override {
        Some(keys) => {
            let missing = unknown(keys);
            if !missing.is_empty() {
                return Err(VerifyError::InitializationError(format!(
                    "table '{}' has no key column(s) {}",
                    meta.name,
                    missing.join(", ")
                )));
            }
            keys.to_vec()
        }
        None => meta.primary_keys.clone(),
    };

    let ignored = |c: &str| ignore.iter().any(|i| i.eq_ignore_ascii_case(c));

    if let Some(key) = key_columns.iter().find(|k| ignored(k)) {
        return Err(VerifyError::InitializationError(format!(
            "key column '{key}' of table '{}' cannot also be ignored",
            meta.name
        )));
    }

    let mut column_order: Vec<String> = meta
        .columns
        .keys()
        .filter(|c| !ignored(c))
        .cloned()
        .collect();
    column_order.sort();

    Ok(HashSpec {
        column_order,
        key_columns,
        algorithm: HashAlgorithm::default(),
    })
}

pub async fn hash_table(
    driver: &DriverRef,
    meta: TableMetadata,
    spec: &HashSpec,
    sink: &HashSink<'_>,
    expected_rows: Option<u64>,
) -> Result<u64, VerifyError> {
    let table = meta.name.clone();
    let col_types: HashMap<String, String> = meta
        .columns
        .values()
        .map(|c| (c.name.clone(), c.data_type.clone()))
        .collect();

    // Page over the primary key when there is one, so reads stay index-ordered;
    // otherwise over the spec's key.
    let page_keys = if meta.primary_keys.is_empty() {
        spec.key_columns.clone()
    } else {
        meta.primary_keys.clone()
    };

    let offset_strategy = OffsetStrategyFactory::keyset_over_pk(
        OffsetStrategyFactory::default_strategy(),
        &table,
        &page_keys,
    );
    let reader = create_table_reader(driver, meta, offset_strategy)?;

    let mut hasher = RowHasher::new(spec.column_order.clone(), spec.algorithm);
    let mut cursor = Some(Cursor::None);
    let mut rows_read = 0u64;

    let started = Instant::now();
    let mut ticker = Ticker::new(PROGRESS_INTERVAL);

    while let Some(c) = cursor {
        let (rows, next_cursor) = reader.next_batch(c, READ_BATCH_ROWS).await?;
        rows_read += rows.len() as u64;

        if ticker.report(rows_read) {
            let percent_str = expected_rows
                .map(|e| percent_of(rows_read, e))
                .unwrap_or_default();

            info!(
                table = %table,
                rows = rows_read,
                expected = expected_rows,
                "reading{percent_str}",
            );
        }

        if !rows.is_empty() {
            let refs: Vec<&Record> = rows.iter().collect();
            let entries = hasher.hash_rows(&refs, &col_types, &spec.key_columns);
            sink.log
                .append(sink.scope, sink.pipeline, sink.table, &entries)?;
        }

        cursor = next_cursor;
    }

    if started.elapsed() >= PROGRESS_INTERVAL {
        info!(table = %table, rows = rows_read, "table read; sorting");
    }

    Ok(rows_read)
}

pub(crate) async fn fetch_table_metadata(
    driver: &DriverRef,
    table: &str,
) -> Result<TableMetadata, VerifyError> {
    dispatch_driver!(driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        introspector.table_metadata(table).await
    })
    .map_err(Into::into)
}

fn create_table_reader(
    driver: &DriverRef,
    meta: TableMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<TableReader, VerifyError> {
    dispatch_driver!(driver, |d| {
        let data_reader: Arc<dyn DataReader> = d.clone() as _;
        Ok(TableReader::new(
            data_reader,
            meta.clone(),
            offset_strategy.clone(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectors::sql::metadata::column::ColumnMetadata;

    fn meta(columns: &[&str], pk: &[&str]) -> TableMetadata {
        TableMetadata {
            name: "orders".into(),
            schema: None,
            columns: columns
                .iter()
                .map(|c| {
                    let col = ColumnMetadata {
                        name: c.to_string(),
                        ..Default::default()
                    };
                    (c.to_string(), col)
                })
                .collect(),
            primary_keys: pk.iter().map(|k| k.to_string()).collect(),
            foreign_keys: Vec::new(),
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Same shape `apply` hashes its destination with: sorted columns, the PK, SHA-256.
    #[test]
    fn spec_matches_what_apply_builds() {
        let m = meta(&["note", "id", "amount"], &["id"]);
        let spec = build_spec(&m, None, &[]).unwrap();

        assert_eq!(spec.column_order, strings(&["amount", "id", "note"]));
        assert_eq!(spec.key_columns, strings(&["id"]));
        assert_eq!(spec.algorithm, HashAlgorithm::Sha256);
    }

    #[test]
    fn ignored_columns_are_left_out_case_insensitively() {
        let m = meta(&["id", "amount", "_fivetran_synced"], &["id"]);
        let spec = build_spec(&m, None, &strings(&["_FIVETRAN_SYNCED"])).unwrap();

        assert_eq!(spec.column_order, strings(&["amount", "id"]));
    }

    #[test]
    fn key_override_replaces_a_missing_primary_key() {
        let m = meta(&["order_no", "amount"], &[]);
        let spec = build_spec(&m, Some(&strings(&["order_no"])), &[]).unwrap();

        assert_eq!(spec.key_columns, strings(&["order_no"]));
    }

    #[test]
    fn a_table_without_a_key_is_hashed_unkeyed() {
        let spec = build_spec(&meta(&["a", "b"], &[]), None, &[]).unwrap();
        assert!(spec.key_columns.is_empty());
    }

    #[test]
    fn unknown_names_and_ignored_keys_are_refused() {
        let m = meta(&["id", "amount"], &["id"]);

        let err = build_spec(&m, None, &strings(&["nope"])).unwrap_err();
        assert!(err.to_string().contains("no column(s) nope"), "{err}");

        let err = build_spec(&m, Some(&strings(&["nope"])), &[]).unwrap_err();
        assert!(err.to_string().contains("no key column(s) nope"), "{err}");

        let err = build_spec(&m, None, &strings(&["id"])).unwrap_err();
        assert!(err.to_string().contains("cannot also be ignored"), "{err}");
    }

    #[test]
    fn receipt_spec_reuses_the_receipts_own_order_and_key() {
        let receipt = VerificationReceipt {
            run_id: "run".into(),
            pipeline_name: "p".into(),
            table_name: "orders".into(),
            table_root: [0; 32],
            column_order: strings(&["amount", "id"]),
            key_columns: strings(&["id"]),
            total_rows: 1,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Blake3,
            created_at: chrono::Utc::now(),
        };
        let spec = HashSpec::from_receipt(&receipt);

        assert_eq!(spec.column_order, receipt.column_order);
        assert_eq!(spec.key_columns, receipt.key_columns);
        assert_eq!(spec.algorithm, HashAlgorithm::Blake3);
    }
}
