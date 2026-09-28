use model::execution::row_count::RowCount;

/// Width of the magnitude bar, in cells.
pub(super) const BAR_W: usize = 5;

pub(super) fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Group a u64 with thousands separators: `16044` -> `16,044`.
pub(super) fn commas(n: u64) -> String {
    let digits = n.to_string();
    let bytes = digits.as_bytes();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

/// Count can't be known at plan time (e.g. a WASM/plugin source that streams or
/// fetches rows at runtime)
pub(super) fn is_unknown_count(rc: &RowCount) -> bool {
    rc.value == 0 && rc.is_estimated && rc.confidence.is_none()
}

pub(super) fn fmt_rows(rc: &RowCount) -> String {
    if is_unknown_count(rc) {
        return "unknown".to_string();
    }

    let n = commas(rc.value);
    if rc.is_estimated { format!("~{n}") } else { n }
}

/// A 0..=`BAR_W` cell magnitude bar using eighth-blocks, scaled to `max`.
pub(super) fn bar(rows: u64, max: u64) -> String {
    if max == 0 || rows == 0 {
        return String::new();
    }
    let eighths = ((rows as f64 / max as f64) * (BAR_W as f64 * 8.0))
        .round()
        .max(1.0) as usize;
    let full = eighths / 8;
    let rem = eighths % 8;
    let parts = ['▏', '▎', '▍', '▌', '▋', '▊', '▉'];
    let mut b = "█".repeat(full);
    if rem > 0 {
        b.push(parts[rem - 1]);
    }
    b
}

pub(super) fn truncate_list(items: &[&str], max: usize) -> String {
    if items.len() <= max {
        items.join(", ")
    } else {
        format!(
            "{}, … and {} more",
            items[..max].join(", "),
            items.len() - max
        )
    }
}

pub(super) fn fmt_seconds(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

pub(super) fn pad(v: &str, width: usize) -> String {
    let len = v.chars().count();
    if len >= width {
        v.to_string()
    } else {
        format!("{v}{}", " ".repeat(width - len))
    }
}

pub(super) fn trunc(v: &str, max: usize) -> String {
    if v.chars().count() <= max {
        v.to_string()
    } else {
        let keep = max.saturating_sub(1);
        format!("{}…", v.chars().take(keep).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_seconds_formats_ranges() {
        assert_eq!(fmt_seconds(5), "5s");
        assert_eq!(fmt_seconds(90), "1m 30s");
        assert_eq!(fmt_seconds(3661), "1h 1m");
    }

    #[test]
    fn commas_groups_thousands() {
        assert_eq!(commas(599), "599");
        assert_eq!(commas(1000), "1,000");
        assert_eq!(commas(16044), "16,044");
        assert_eq!(commas(17643210), "17,643,210");
    }

    #[test]
    fn bar_scales_to_max() {
        assert_eq!(bar(0, 100), "");
        assert_eq!(bar(100, 100), "█████"); // full
        assert!(!bar(1, 100).is_empty()); // tiny but non-empty
    }

    #[test]
    fn fmt_rows_distinguishes_unknown_from_zero_and_estimates() {
        // The unknown() sentinel (plugin source) reads as "unknown", not "~0".
        assert_eq!(fmt_rows(&RowCount::unknown()), "unknown");
        assert!(is_unknown_count(&RowCount::unknown()));

        // A real empty table is an exact 0.
        assert_eq!(fmt_rows(&RowCount::exact(0)), "0");
        assert!(!is_unknown_count(&RowCount::exact(0)));

        // A genuine estimate of ~0 keeps its confidence and stays "~0".
        assert_eq!(fmt_rows(&RowCount::estimated(0, 0.5)), "~0");
        assert!(!is_unknown_count(&RowCount::estimated(0, 0.5)));

        // Normal counts are unaffected.
        assert_eq!(fmt_rows(&RowCount::exact(1000)), "1,000");
        assert_eq!(fmt_rows(&RowCount::estimated(1000, 0.9)), "~1,000");
    }
}
