use super::KeyRange;
use crate::error::VerifyError;
use connectors::sql::checksum::SegmentHash;
use std::collections::{BTreeMap, BTreeSet};

/// Segments level 0 aims for.
const LEVEL0_SEGMENTS: i128 = 1_000;
/// Parts a differing segment is split into.
const SPLIT: i128 = 32;

/// A segment that differs, with the larger of its two row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Segment {
    pub(super) range: KeyRange,
    pub(super) rows: u64,
}

/// Level 0's segment numbering.
#[derive(Debug, Clone, Copy)]
pub(super) struct Grid {
    pub(super) lo: i64,
    pub(super) width: i64,
}

impl Grid {
    pub(super) fn new(lo: i64, hi: i64) -> Self {
        let span = i128::from(hi) - i128::from(lo) + 1;
        let width = ((span + LEVEL0_SEGMENTS - 1) / LEVEL0_SEGMENTS).max(1);
        Self {
            lo,
            width: i64::try_from(width).unwrap_or(i64::MAX),
        }
    }

    /// The keys of segment `seg`; -1 is everything below `lo`.
    pub(super) fn range(&self, seg: i64) -> Option<KeyRange> {
        if seg == -1 {
            return Some((i64::MIN, self.lo.checked_sub(1)?));
        }
        let start = i128::from(self.lo) + i128::from(seg) * i128::from(self.width);
        let end = start + i128::from(self.width) - 1;
        Some((
            i64::try_from(start).ok()?,
            i64::try_from(end.min(i128::from(i64::MAX))).ok()?,
        ))
    }
}

/// The segments whose count or fingerprint differ, or that one side lacks.
pub(super) fn differing(
    source: &BTreeMap<i64, SegmentHash>,
    target: &BTreeMap<i64, SegmentHash>,
    range_of: impl Fn(i64) -> Option<KeyRange>,
) -> Result<Vec<Segment>, VerifyError> {
    let ids: BTreeSet<i64> = source.keys().chain(target.keys()).copied().collect();
    let mut out = Vec::new();

    for id in ids {
        let (s, t) = (source.get(&id), target.get(&id));
        let same =
            matches!((s, t), (Some(a), Some(b)) if (a.rows, a.h1, a.h2) == (b.rows, b.h1, b.h2));
        if same {
            continue;
        }
        let range = range_of(id).ok_or_else(|| {
            VerifyError::InitializationError(format!("block mode: segment {id} has no key range"))
        })?;
        let rows = s.map_or(0, |h| h.rows).max(t.map_or(0, |h| h.rows));
        out.push(Segment { range, rows });
    }
    Ok(out)
}

/// Sort and join touching ranges, so the leaf read has fewer of them.
pub(super) fn merge(mut ranges: Vec<KeyRange>) -> Vec<KeyRange> {
    ranges.sort_unstable();
    let mut out: Vec<KeyRange> = Vec::with_capacity(ranges.len());
    for (lo, hi) in ranges {
        match out.last_mut() {
            Some(last) if i128::from(lo) <= i128::from(last.1) + 1 => last.1 = last.1.max(hi),
            _ => out.push((lo, hi)),
        }
    }
    out
}

/// Split a range into up to `SPLIT` contiguous parts.
pub(super) fn split(range: KeyRange) -> Vec<KeyRange> {
    let (lo, hi) = (i128::from(range.0), i128::from(range.1));
    let width = ((hi - lo + 1 + SPLIT - 1) / SPLIT).max(1);
    let mut parts = Vec::new();
    let mut start = lo;
    while start <= hi {
        let end = (start + width - 1).min(hi);
        // Both ends lie within `range`, so they fit in i64.
        parts.push((start as i64, end as i64));
        start = end + 1;
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_zero_grid_covers_the_range_in_about_a_thousand_segments() {
        let grid = Grid::new(1, 10_000_000);
        assert_eq!(grid.width, 10_000);
        assert_eq!(grid.range(0), Some((1, 10_000)));
        assert_eq!(grid.range(999), Some((9_990_001, 10_000_000)));
        assert_eq!(grid.range(-1), Some((i64::MIN, 0)));

        let small = Grid::new(5, 7);
        assert_eq!(small.width, 1);

        // The whole i64 range: widths and ends must not overflow.
        let all = Grid::new(i64::MIN, i64::MAX);
        assert_eq!(all.range(-1), None);
        let last = (u64::MAX / all.width as u64) as i64;
        assert_eq!(all.range(last).map(|r| r.1), Some(i64::MAX));
    }

    #[test]
    fn split_covers_the_range_without_gaps() {
        for range in [(1, 10_000), (0, 31), (5, 5), (i64::MAX - 100, i64::MAX)] {
            let parts = split(range);
            assert!(parts.len() <= SPLIT as usize);
            assert_eq!(parts.first().map(|p| p.0), Some(range.0));
            assert_eq!(parts.last().map(|p| p.1), Some(range.1));
            for pair in parts.windows(2) {
                assert_eq!(pair[0].1 + 1, pair[1].0, "{range:?}");
            }
        }
    }

    #[test]
    fn merge_joins_touching_ranges() {
        assert_eq!(
            merge(vec![(11, 20), (1, 10), (30, 40), (35, 50)]),
            vec![(1, 20), (30, 50)]
        );
        assert_eq!(
            merge(vec![(i64::MAX - 1, i64::MAX)]),
            vec![(i64::MAX - 1, i64::MAX)]
        );
    }

    #[test]
    fn segments_differ_on_count_hash_or_absence() {
        let h = |seg, rows, h1| SegmentHash {
            seg,
            rows,
            h1,
            h2: 0,
        };
        let source = BTreeMap::from([(0, h(0, 10, 1)), (1, h(1, 10, 2)), (2, h(2, 10, 3))]);
        let target = BTreeMap::from([(0, h(0, 10, 1)), (1, h(1, 9, 2)), (3, h(3, 4, 4))]);
        let grid = Grid { lo: 0, width: 10 };

        let found = differing(&source, &target, |s| grid.range(s)).unwrap();
        assert_eq!(
            found,
            vec![
                Segment {
                    range: (10, 19),
                    rows: 10
                },
                Segment {
                    range: (20, 29),
                    rows: 10
                },
                Segment {
                    range: (30, 39),
                    rows: 4
                },
            ]
        );
    }
}
