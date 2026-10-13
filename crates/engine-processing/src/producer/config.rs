use model::integrity::config::IntegrityConfig;
use std::{num::NonZeroUsize, time::Duration};

pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Configuration for producer behavior.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Number of rows to fetch per batch
    pub batch_size: usize,

    /// Concurrent transform operations
    pub transform_concurrency: NonZeroUsize,

    /// How long to wait when idle before polling again
    pub idle_poll_interval: Duration,

    /// Number of rows to sample for validation
    pub sample_size: usize,

    /// Maximum retry attempts for transient failures
    pub max_retries: usize,

    /// Delay between retries
    pub retry_delay: Duration,

    /// When `Some`, the producer hashes each batch and streams the keyed row
    /// hashes to the row-hash store.
    pub integrity: Option<IntegrityConfig>,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            transform_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
            idle_poll_interval: Duration::from_millis(500),
            sample_size: 10,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            integrity: None,
        }
    }
}

impl ProducerConfig {
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.transform_concurrency = concurrency;
        self
    }

    pub fn with_integrity(mut self, config: IntegrityConfig) -> Self {
        self.integrity = Some(config);
        self
    }
}
