use crate::page::payload_bytes;
use model::records::Record;
use std::{sync::OnceLock, time::Duration};
use tracing::{info, warn};

/// Rows per keyset page when `PAGANEL_PAGE_ROWS` is not set.
const DEFAULT_PAGE_ROWS: usize = 1000;
const BYTES_PER_MIB: f64 = 1_048_576.0;

/// True when `PAGANEL_PROFILE` is set.
pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("PAGANEL_PROFILE").is_some())
}

/// Rows per keyset page from `PAGANEL_PAGE_ROWS`, if set to a valid positive number.
pub(crate) fn page_rows_override() -> Option<usize> {
    static PAGE_ROWS: OnceLock<Option<usize>> = OnceLock::new();
    *PAGE_ROWS.get_or_init(|| {
        let raw = std::env::var("PAGANEL_PAGE_ROWS").ok()?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        match trimmed.parse::<usize>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                warn!(
                    value = %raw,
                    "PAGANEL_PAGE_ROWS is not a positive number; ignoring override"
                );
                None
            }
        }
    })
}

/// Rows per keyset page: `PAGANEL_PAGE_ROWS`, or the default.
pub(crate) fn page_rows() -> usize {
    page_rows_override().unwrap_or(DEFAULT_PAGE_ROWS)
}

/// Where one table read spent its time, page by page.
#[derive(Debug, Default)]
pub(crate) struct ReadProfile {
    pages: u64,
    rows: u64,
    bytes: u64,
    wait: Duration,
    slowest_wait: Duration,
    hash: Duration,
    write: Duration,
}

impl ReadProfile {
    pub(crate) fn page(&mut self, wait: Duration, rows: &[Record]) {
        if !enabled() {
            return;
        }
        self.pages += 1;
        self.rows += rows.len() as u64;
        self.bytes += payload_bytes(rows) as u64;
        self.wait += wait;
        self.slowest_wait = self.slowest_wait.max(wait);
    }

    pub(crate) fn hashed(&mut self, d: Duration) {
        self.hash += d;
    }

    pub(crate) fn written(&mut self, d: Duration) {
        self.write += d;
    }

    /// Log the profile, when profiling is on.
    pub(crate) fn log(&self, table: &str, total: Duration) {
        if !enabled() {
            return;
        }

        let wait_per_page_ms = if self.pages > 0 {
            self.wait.as_secs_f64() * 1_000.0 / self.pages as f64
        } else {
            0.0
        };

        info!(
            table,
            page_rows = page_rows(),
            pages = self.pages,
            rows = self.rows,
            payload_mb = %format_args!("{:.1}", self.bytes as f64 / BYTES_PER_MIB),
            total_ms = ms(total),
            wait_ms = ms(self.wait),
            wait_per_page_ms = %format_args!("{wait_per_page_ms:.2}"),
            slowest_page_ms = ms(self.slowest_wait),
            hash_ms = ms(self.hash),
            write_ms = ms(self.write),
            "read profile",
        );
    }
}

/// Log a pair's phase times, when profiling is on.
pub(crate) fn log_pair(pair: &str, read: Duration, sort: Duration, diff: Duration) {
    if !enabled() {
        return;
    }

    info!(
        pair,
        read_ms = ms(read),
        sort_ms = ms(sort),
        diff_ms = ms(diff),
        "pair profile",
    );
}

/// Log the re-check's time, split into the grace wait and the re-read.
pub(crate) fn log_recheck(pair: &str, keys: usize, grace: Duration, reread: Duration) {
    if !enabled() {
        return;
    }

    info!(
        pair,
        keys,
        grace_ms = ms(grace),
        reread_ms = ms(reread),
        "recheck profile",
    );
}

fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
}
