use crate::integrity::receipt::VerificationReceipt;
use serde::{Deserialize, Serialize};

/// Maximum number of individual divergences carried in a result.
pub const MAX_REPORTED_DIVERGENCES: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationResult {
    Match {
        receipt: VerificationReceipt,
        duration_ms: u64,
        #[serde(default)]
        extra_ignored: u64,
    },
    Mismatch {
        receipt: VerificationReceipt,
        /// Root recomputed from the destination's current contents.
        actual_root: [u8; 32],
        summary: DivergenceSummary,
        divergences: Vec<Divergence>,
        duration_ms: u64,
    },
    /// No receipt for this table - the pipeline ran without `--integrity`.
    NoPriorRun { pipeline: String, table: String },
    /// A receipt exists, but the row-hash log it must be diffed against is
    /// missing or truncated.
    LogUnavailable {
        pipeline: String,
        table: String,
        /// Rows the receipt says were committed.
        expected_rows: u64,
        /// Rows actually found in the row-hash log.
        found_rows: u64,
    },
}

/// Complete counts for a mismatch, independent of the truncated detail list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DivergenceSummary {
    /// Rows in the receipt with no matching key in the destination - data loss.
    pub missing: u64,
    /// Rows in the destination with no matching key in the receipt.
    pub extra: u64,
    /// Keys present on both sides whose row contents differ.
    pub changed: u64,
    /// Row keys committed by the receipt.
    pub expected_rows: u64,
    /// Distinct row keys currently in the destination.
    pub actual_rows: u64,
}

impl DivergenceSummary {
    pub fn is_clean(&self) -> bool {
        self.missing == 0 && self.extra == 0 && self.changed == 0
    }
}

/// A single row whose state differs between the receipt and the destination,
/// identified by its primary key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Divergence {
    /// Human-readable row key, e.g. `actor_id=42`.
    pub key: String,
    pub kind: DivergenceKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DivergenceKind {
    /// Migrated, but absent from the destination now.
    Missing { expected_hash: [u8; 32] },
    /// Present in the destination, but never migrated.
    Extra { actual_hash: [u8; 32] },
    /// Present on both sides with different contents.
    Changed {
        expected_hash: [u8; 32],
        actual_hash: [u8; 32],
    },
}
