use super::SegmentHash;
use model::{core::value::Value, records::Record};

/// Read one row of [`fingerprint_query`]'s result.
pub fn parse_segment_hash(record: &Record) -> Option<SegmentHash> {
    Some(SegmentHash {
        seg: i64::try_from(integer(record.value("seg")?)?).ok()?,
        rows: u64::try_from(integer(record.value("n")?)?).ok()?,
        h1: u64::try_from(integer(record.value("h1")?)?).ok()?,
        h2: u64::try_from(integer(record.value("h2")?)?).ok()?,
    })
}

/// Drivers return counts and sums as integers, decimals or text, by dialect.
fn integer(value: &Value) -> Option<i128> {
    match value {
        Value::Int(i) => Some(i128::from(*i)),
        Value::UInt(u) => Some(i128::from(*u)),
        Value::Decimal(d) => d.to_plain_string().parse().ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}
