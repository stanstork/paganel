/// Timestamps in human-readable output, e.g. `2026-10-20 14:20:15 UTC`.
pub(crate) const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S UTC";

/// Thousands-separated integer, e.g. 1234567 -> "1,234,567".
pub(crate) fn commas(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);

    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commas_group_thousands() {
        assert_eq!(commas(0), "0");
        assert_eq!(commas(599), "599");
        assert_eq!(commas(1000), "1,000");
        assert_eq!(commas(16044), "16,044");
        assert_eq!(commas(17_643_210), "17,643,210");
        assert_eq!(commas(u64::MAX), "18,446,744,073,709,551,615");
    }
}
