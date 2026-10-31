use crate::integrity::algorithm::HashAlgorithm;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Written to the state store when a pipeline completes. Loaded by engine-verify.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReceipt {
    pub run_id: String,
    pub pipeline_name: String,
    pub table_name: String,
    /// Merkle root over every row leaf, taken in ascending row-key order.
    pub table_root: [u8; 32],
    /// Lexicographically sorted destination column names.
    pub column_order: Vec<String>,
    /// Destination key (primary key) columns, in table order. Empty means the
    /// table has no primary key and each row hash served as its own key.
    pub key_columns: Vec<String>,
    /// Distinct row keys committed to `table_root` - the tree's leaf count.
    pub total_rows: u64,
    /// Rows sent to DLQ - not present in destination.
    /// Allows verify to distinguish expected absences from data loss.
    pub skipped_rows: u64,
    pub algorithm: HashAlgorithm,
    pub created_at: DateTime<Utc>,
}

impl VerificationReceipt {
    /// The full table root.
    pub fn root_hex(&self) -> String {
        root_hex(&self.table_root)
    }
}

/// Render a 32-byte root as 64 lowercase hex characters.
pub fn root_hex(root: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(64);
    for byte in root {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
