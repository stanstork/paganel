#![deny(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod diff;
pub mod error;
pub mod progress;
pub mod reader;
pub mod state;
pub mod table;
pub mod verifier;

pub use diff::{DiffInput, DiffOutcome, diff_keyed_sets};
pub use progress::{NoopProgress, VerifyProgress};
pub use state::VerifyState;
pub use table::{HashSink, HashSpec, hash_table, spec_from_table};
pub use verifier::{VerifyOptions, verify, verify_plan, verify_with_progress};
