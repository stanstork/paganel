use model::records::Record;

/// Decoded bytes a page aims for.
const TARGET_PAGE_BYTES: usize = 4 * 1024 * 1024; // 4 MB
/// The first page, before any row size is known.
const FIRST_PAGE_ROWS: usize = 1_000;
const MIN_PAGE_ROWS: usize = 1_000;
const MAX_PAGE_ROWS: usize = 100_000;

/// Rows to request for the next page, either fixed or learned from prior pages.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PageSizer {
    Fixed(usize),
    Adaptive(usize),
}

impl PageSizer {
    pub(crate) fn new(fixed: Option<usize>) -> Self {
        match fixed {
            Some(rows) => Self::Fixed(rows),
            None => Self::Adaptive(FIRST_PAGE_ROWS),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        match *self {
            Self::Fixed(rows) | Self::Adaptive(rows) => rows,
        }
    }

    /// Size the next page from the last one: `rows` rows that decoded to
    /// `bytes` bytes.
    pub(crate) fn observe(&mut self, rows: usize, bytes: usize) {
        let Self::Adaptive(current) = self else {
            return;
        };

        if rows == 0 {
            return;
        }

        let estimated = if bytes == 0 {
            MAX_PAGE_ROWS
        } else {
            let target = (TARGET_PAGE_BYTES as u128 * rows as u128) / bytes as u128;
            usize::try_from(target).unwrap_or(MAX_PAGE_ROWS)
        };

        *current = estimated.clamp(MIN_PAGE_ROWS, MAX_PAGE_ROWS);
    }
}

pub(crate) fn payload_bytes(rows: &[Record]) -> usize {
    rows.iter()
        .flat_map(|row| (0..row.len()).filter_map(move |i| row.value_at(i)))
        .map(|v| v.size_bytes())
        .sum()
}
