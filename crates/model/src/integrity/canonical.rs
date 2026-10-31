use crate::core::value::Value;
use chrono::{NaiveDate, Timelike};

pub const TAG_NULL: u8 = 0x00;
pub const TAG_INT: u8 = 0x01;
pub const TAG_UINT: u8 = 0x02;
pub const TAG_BOOL: u8 = 0x03;
pub const TAG_STRING: u8 = 0x10;
pub const TAG_DECIMAL: u8 = 0x11;
pub const TAG_FLOAT: u8 = 0x12;
pub const TAG_DATE: u8 = 0x20;
pub const TAG_TIMESTAMP: u8 = 0x21;
pub const TAG_TIME: u8 = 0x22;
pub const TAG_INTERVAL: u8 = 0x23;
pub const TAG_YEAR: u8 = 0x24;
pub const TAG_UUID: u8 = 0x30;
pub const TAG_BINARY: u8 = 0x40;
pub const TAG_GEOMETRY: u8 = 0x41;
pub const TAG_BITS: u8 = 0x42;
pub const TAG_JSON: u8 = 0x50;
pub const TAG_ARRAY: u8 = 0x60;
pub const TAG_SET: u8 = 0x61;
pub const TAG_ENUM: u8 = 0x70;
pub const TAG_INET: u8 = 0x80;
pub const TAG_CIDR: u8 = 0x81;
pub const TAG_MACADDR: u8 = 0x82;
pub const TAG_COMPOSITE: u8 = 0x90;

/// Encoding form for a decimal, so the compact and textual forms can never be
/// read as one another.
const DECIMAL_COMPACT: u8 = 0x00;
const DECIMAL_TEXT: u8 = 0x01;

pub fn serialize_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(TAG_NULL),

        Value::Int(i) => {
            buf.push(TAG_INT);
            buf.extend(&i.to_le_bytes());
        }

        Value::UInt(u) => {
            // PostgreSQL has no unsigned integer types - any UInt that fits in i64 will
            // be read back as Int after a round-trip through PG. Normalise here so that
            // Value::UInt(1) and Value::Int(1) produce identical canonical bytes.
            // Values that exceed i64::MAX cannot be stored in any PG integer column and
            // retain the distinct TAG_UINT encoding to flag potential data loss.
            if *u <= i64::MAX as u64 {
                buf.push(TAG_INT);
                buf.extend(&(*u as i64).to_le_bytes());
            } else {
                buf.push(TAG_UINT);
                buf.extend(&u.to_le_bytes());
            }
        }

        Value::Boolean(b) => {
            // Normalize to TAG_INT for cross-database compatibility.
            // MySQL TINYINT(1) is read as Value::Int(0/1) while PostgreSQL BOOLEAN
            // is read as Value::Boolean(false/true). Both represent the same logical
            // value; encoding both as Int ensures hashes match across a migration.
            buf.push(TAG_INT);
            buf.extend(&(*b as i64).to_le_bytes());
        }

        Value::String(s) => {
            buf.push(TAG_STRING);
            write_bytes(s.as_bytes(), buf);
        }

        Value::Decimal(d) => {
            buf.push(TAG_DECIMAL);
            write_decimal(d, buf);
        }

        Value::Float(f) => {
            if f.is_nan() {
                // NaN has no defined equality - treat as NULL
                buf.push(TAG_NULL);
            } else {
                buf.push(TAG_FLOAT);
                // Normalize -0.0 -> +0.0
                let f = if *f == 0.0_f64 { 0.0_f64 } else { *f };
                buf.extend(&f.to_be_bytes());
            }
        }

        Value::Date(d) => {
            buf.push(TAG_DATE);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
            let days = d.signed_duration_since(epoch).num_days() as i32;
            buf.extend(&days.to_le_bytes());
        }

        Value::Timestamp { value, offset_secs } => {
            buf.push(TAG_TIMESTAMP);

            let offset = chrono::Duration::seconds(offset_secs.unwrap_or(0) as i64);
            let utc = value.checked_sub_signed(offset).unwrap_or(*value);
            let micros = utc
                .signed_duration_since(chrono::NaiveDateTime::UNIX_EPOCH)
                .num_microseconds()
                .unwrap_or(0);
            buf.extend(&micros.to_le_bytes());
        }

        Value::Time { value, .. } => {
            // offset_secs on a NaiveTime is ambiguous - encode the raw time only
            buf.push(TAG_TIME);
            let micros = value.num_seconds_from_midnight() as i64 * 1_000_000
                + value.nanosecond() as i64 / 1_000;
            buf.extend(&micros.to_le_bytes());
        }

        Value::Interval(iv) => {
            buf.push(TAG_INTERVAL);
            buf.extend(&iv.months.to_le_bytes());
            buf.extend(&iv.days.to_le_bytes());
            buf.extend(&iv.microseconds.to_le_bytes());
        }

        Value::Year(y) => {
            // Normalize to TAG_INT for cross-database compatibility.
            // PostgreSQL has no YEAR type; MySQL YEAR columns are stored as SMALLINT
            // in PostgreSQL and read back as Value::Int. Encoding both as Int ensures
            // hashes match across a migration.
            buf.push(TAG_INT);
            buf.extend(&(*y as i64).to_le_bytes());
        }

        Value::Uuid(u) => {
            buf.push(TAG_UUID);
            buf.extend(u.as_bytes());
        }

        Value::Binary(b) => {
            // Normalize valid-UTF-8 binary to String encoding for cross-database
            // compatibility. MySQL reads VARCHAR columns with binary charset/collation
            // as Value::Binary while PostgreSQL reads the same column as Value::String.
            // Only true binary content (invalid UTF-8 bytes, e.g. image data) keeps
            // the TAG_BINARY encoding.
            if let Ok(s) = std::str::from_utf8(b) {
                buf.push(TAG_STRING);
                write_bytes(s.as_bytes(), buf);
            } else {
                buf.push(TAG_BINARY);
                write_bytes(b, buf);
            }
        }

        Value::Geometry(g) => {
            // Raw WKB bytes - same treatment as binary
            buf.push(TAG_GEOMETRY);
            write_bytes(g, buf);
        }

        Value::Bits(bits) => {
            buf.push(TAG_BITS);
            buf.extend(&(bits.len() as u32).to_le_bytes());
            // Pack bits MSB-first into bytes
            for chunk in bits.chunks(8) {
                let mut byte = 0u8;
                for (i, &bit) in chunk.iter().enumerate() {
                    if bit {
                        byte |= 1 << (7 - i); // set bit from left to right
                    }
                }
                buf.push(byte);
            }
        }

        Value::Json(j) => {
            buf.push(TAG_JSON);
            write_bytes(canonical_json(j).as_bytes(), buf);
        }

        Value::Array(arr) => {
            buf.push(TAG_ARRAY);
            buf.extend(&(arr.len() as u32).to_le_bytes());
            for elem in arr {
                serialize_value(elem, buf);
            }
        }

        Value::Set(set) => {
            // Normalize to TAG_ARRAY of String elements for cross-database compatibility.
            // MySQL SET -> PostgreSQL TEXT[] during migration; PG reads back as Value::Array.
            // MySQL returns SET elements in schema-definition order; PG preserves insertion
            // order (= schema order), so element ordering matches without sorting.
            buf.push(TAG_ARRAY);
            buf.extend(&(set.len() as u32).to_le_bytes());
            for s in set {
                buf.push(TAG_STRING);
                write_bytes(s.as_bytes(), buf);
            }
        }

        Value::Enum { value, .. } => {
            // Normalize to TAG_STRING for cross-database compatibility.
            // MySQL ENUM -> PostgreSQL VARCHAR during migration; PG reads back as Value::String.
            // Encode only the string value (type_name is schema metadata, not row data).
            buf.push(TAG_STRING);
            write_bytes(value.as_bytes(), buf);
        }

        Value::IpAddr(addr) => {
            buf.push(TAG_INET);
            match addr {
                std::net::IpAddr::V4(v4) => {
                    buf.push(4);
                    buf.extend(&v4.octets());
                }
                std::net::IpAddr::V6(v6) => {
                    buf.push(6);
                    buf.extend(&v6.octets());
                }
            }
        }

        Value::Cidr { addr, prefix } => {
            buf.push(TAG_CIDR);
            match addr {
                std::net::IpAddr::V4(v4) => {
                    buf.push(4);
                    buf.extend(&v4.octets());
                }
                std::net::IpAddr::V6(v6) => {
                    buf.push(6);
                    buf.extend(&v6.octets());
                }
            }
            buf.push(*prefix);
        }

        Value::MacAddr(m) => {
            buf.push(TAG_MACADDR);
            buf.extend(m);
        }

        Value::Composite { fields, .. } => {
            // Sort by field name for determinism - composite field order is schema-defined
            // but driver result order may vary
            buf.push(TAG_COMPOSITE);
            let mut sorted: Vec<_> = fields.iter().collect();
            sorted.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            buf.extend(&(sorted.len() as u32).to_le_bytes());
            for (name, val) in sorted {
                write_bytes(name.as_bytes(), buf);
                serialize_value(val, buf);
            }
        }
    }
}

/// Render canonically-encoded key bytes back into `col=value` text.
pub fn describe_key(bytes: &[u8], col_names: &[String]) -> String {
    let mut cursor = 0usize;
    let mut parts = Vec::new();
    let mut i = 0;

    while cursor < bytes.len() {
        let Some(text) = read_value(bytes, &mut cursor) else {
            // Undecodable tail: fall back to hex of the whole key rather than
            // reporting a half-decoded value that could mislead.
            return format!("0x{}", hex(bytes));
        };

        match col_names.get(i) {
            Some(name) => parts.push(format!("{name}={text}")),
            None => parts.push(text),
        }
        i += 1;
    }

    if parts.is_empty() {
        format!("0x{}", hex(bytes))
    } else {
        parts.join(",")
    }
}

/// Decode canonically-encoded key bytes back into values a query can bind.
pub fn decode_key(bytes: &[u8]) -> Option<Vec<Value>> {
    let mut cursor = 0usize;
    let mut values = Vec::new();

    while cursor < bytes.len() {
        let tag = *bytes.get(cursor)?;
        cursor += 1;

        let value = match tag {
            TAG_INT => Value::Int(i64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?)),
            TAG_UINT => Value::UInt(u64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?)),
            TAG_STRING => {
                let raw = take_prefixed(bytes, &mut cursor)?;
                Value::String(String::from_utf8(raw.to_vec()).ok()?)
            }
            TAG_UUID => Value::Uuid(uuid::Uuid::from_bytes(take_array::<16>(
                bytes,
                &mut cursor,
            )?)),
            _ => return None,
        };
        values.push(value);
    }

    Some(values)
}

/// Decode one canonical value, advancing `cursor`. `None` on malformed input.
fn read_value(b: &[u8], cursor: &mut usize) -> Option<String> {
    let tag = *b.get(*cursor)?;
    *cursor += 1;

    match tag {
        TAG_NULL => Some("NULL".to_string()),
        TAG_INT => Some(i64::from_le_bytes(take_array::<8>(b, cursor)?).to_string()),
        TAG_UINT => Some(u64::from_le_bytes(take_array::<8>(b, cursor)?).to_string()),
        TAG_BOOL => Some((*take(b, cursor, 1)?.first()? != 0).to_string()),
        TAG_FLOAT => Some(f64::from_be_bytes(take_array::<8>(b, cursor)?).to_string()),

        TAG_STRING | TAG_DECIMAL | TAG_JSON => {
            let raw = take_prefixed(b, cursor)?;
            Some(String::from_utf8_lossy(raw).into_owned())
        }

        TAG_BINARY | TAG_GEOMETRY => Some(format!("0x{}", hex(take_prefixed(b, cursor)?))),

        TAG_DATE => {
            let days = i32::from_le_bytes(take_array::<4>(b, cursor)?);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let date = epoch.checked_add_signed(chrono::Duration::days(days as i64))?;
            Some(date.to_string())
        }

        TAG_TIMESTAMP => {
            let micros = i64::from_le_bytes(take_array::<8>(b, cursor)?);
            chrono::DateTime::from_timestamp_micros(micros)
                .map(|dt| dt.naive_utc().to_string())
                .or_else(|| Some(format!("{micros}us")))
        }

        TAG_TIME => {
            let micros = i64::from_le_bytes(take_array::<8>(b, cursor)?);
            Some(format!("{micros}us"))
        }

        TAG_INTERVAL => {
            let months = i32::from_le_bytes(take_array::<4>(b, cursor)?);
            let days = i32::from_le_bytes(take_array::<4>(b, cursor)?);
            let micros = i64::from_le_bytes(take_array::<8>(b, cursor)?);
            Some(format!("{months}mon {days}d {micros}us"))
        }

        TAG_UUID => {
            let raw = take_array::<16>(b, cursor)?;
            Some(uuid::Uuid::from_bytes(raw).to_string())
        }

        TAG_BITS => {
            let bit_count = u32::from_le_bytes(take_array(b, cursor)?) as usize;
            let bytes = take(b, cursor, bit_count.div_ceil(8))?;
            Some(format!("0x{}", hex(bytes)))
        }

        TAG_ARRAY | TAG_SET => {
            let count = u32::from_le_bytes(take_array(b, cursor)?) as usize;
            let items: Vec<String> = (0..count)
                .map(|_| read_value(b, cursor))
                .collect::<Option<_>>()?;
            Some(format!("[{}]", items.join(",")))
        }

        TAG_INET | TAG_CIDR => {
            let version = take_array::<1>(b, cursor)?[0];
            let octets = if version == 4 { 4 } else { 16 };
            let addr = hex(take(b, cursor, octets)?);

            if tag == TAG_CIDR {
                let prefix = take_array::<1>(b, cursor)?[0];
                Some(format!("0x{addr}/{prefix}"))
            } else {
                Some(format!("0x{addr}"))
            }
        }

        TAG_MACADDR => Some(format!("0x{}", hex(take(b, cursor, 6)?))),

        TAG_COMPOSITE => {
            let count = u32::from_le_bytes(take_array(b, cursor)?) as usize;
            let fields: Vec<String> = (0..count)
                .map(|_| {
                    let name = String::from_utf8_lossy(take_prefixed(b, cursor)?).into_owned();
                    let val = read_value(b, cursor)?;
                    Some(format!("{name}:{val}"))
                })
                .collect::<Option<_>>()?;
            Some(format!("({})", fields.join(",")))
        }
        _ => None,
    }
}

#[inline]
fn take<'a>(b: &'a [u8], cursor: &mut usize, len: usize) -> Option<&'a [u8]> {
    let slice = b.get(*cursor..cursor.checked_add(len)?)?;
    *cursor += len;
    Some(slice)
}

#[inline]
fn take_array<const N: usize>(b: &[u8], cursor: &mut usize) -> Option<[u8; N]> {
    take(b, cursor, N)?.try_into().ok()
}

#[inline]
fn take_prefixed<'a>(b: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let len = u32::from_le_bytes(take_array(b, cursor)?) as usize;
    take(b, cursor, len)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Canonically encode a decimal.
fn write_decimal(d: &bigdecimal::BigDecimal, buf: &mut Vec<u8>) {
    let (mantissa, exponent) = d.as_bigint_and_exponent();

    let Ok(mut mantissa) = i128::try_from(&mantissa) else {
        buf.push(DECIMAL_TEXT);
        write_bytes(d.normalized().to_string().as_bytes(), buf);
        return;
    };

    let mut exponent = exponent;
    while mantissa != 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        exponent -= 1;
    }

    // Zero has no significant digits, so its exponent carries no meaning:
    // pin it, or 0.00 and 0 would encode differently.
    if mantissa == 0 {
        exponent = 0;
    }

    buf.push(DECIMAL_COMPACT);
    buf.extend_from_slice(&mantissa.to_le_bytes());
    buf.extend_from_slice(&exponent.to_le_bytes());
}

/// Write a length-prefixed byte slice: 4-byte LE length + raw bytes.
#[inline]
fn write_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.extend(&(bytes.len() as u32).to_le_bytes());
    buf.extend(bytes);
}

/// Canonical JSON: compact, object keys sorted alphabetically, no whitespace.
fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let pairs: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k)
                            .expect("serializing a JSON string key is infallible"),
                        canonical_json(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", pairs.join(","))
        }
        serde_json::Value::Array(arr) => {
            let elems: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", elems.join(","))
        }
        other => serde_json::to_string(other).expect("serializing a JSON primitive is infallible"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(val: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_value(val, &mut buf);
        buf
    }

    #[test]
    fn null_is_single_byte() {
        assert_eq!(encode(&Value::Null), vec![TAG_NULL]);
    }

    #[test]
    fn nan_float_encodes_as_null() {
        assert_eq!(encode(&Value::Float(f64::NAN)), vec![TAG_NULL]);
    }

    #[test]
    fn negative_zero_equals_positive_zero() {
        assert_eq!(encode(&Value::Float(-0.0)), encode(&Value::Float(0.0)));
    }

    #[test]
    fn decimal_trailing_zeros_stripped() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;
        let a = encode(&Value::Decimal(BigDecimal::from_str("1.50").unwrap()));
        let b = encode(&Value::Decimal(BigDecimal::from_str("1.5").unwrap()));
        assert_eq!(a, b);
    }

    /// Equal numeric values must encode identically however they are written,
    /// including across the compact/textual boundary conditions.
    #[test]
    fn decimal_equal_values_encode_identically() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let cases = [
            ("0", "0.00"),
            ("0.0", "-0.0"),
            ("100", "1.00E2"),
            ("-2.500", "-2.5"),
            ("12345.67", "12345.6700"),
        ];
        for (a, b) in cases {
            assert_eq!(
                encode(&Value::Decimal(BigDecimal::from_str(a).expect("a"))),
                encode(&Value::Decimal(BigDecimal::from_str(b).expect("b"))),
                "{a} and {b} are the same number"
            );
        }
    }

    #[test]
    fn decimal_distinct_values_encode_differently() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let distinct = ["0", "1", "-1", "0.1", "10", "1.01", "123456789.123456789"];
        for (i, a) in distinct.iter().enumerate() {
            for b in &distinct[i + 1..] {
                assert_ne!(
                    encode(&Value::Decimal(BigDecimal::from_str(a).expect("a"))),
                    encode(&Value::Decimal(BigDecimal::from_str(b).expect("b"))),
                    "{a} and {b} are different numbers"
                );
            }
        }
    }

    /// A mantissa too large for the compact form falls back to text, and must
    /// still normalize and stay distinct from its neighbours.
    #[test]
    fn decimal_oversized_mantissa_falls_back_to_text() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let huge = "1".repeat(45);
        let with_zeros = format!("{huge}.500");
        let without = format!("{huge}.5");

        assert_eq!(
            encode(&Value::Decimal(
                BigDecimal::from_str(&with_zeros).expect("a")
            )),
            encode(&Value::Decimal(BigDecimal::from_str(&without).expect("b"))),
            "the textual path normalizes too"
        );
        assert_ne!(
            encode(&Value::Decimal(BigDecimal::from_str(&without).expect("a"))),
            encode(&Value::Decimal(BigDecimal::from_str("1.5").expect("b"))),
        );
    }

    #[test]
    fn enum_normalizes_to_string() {
        // Enum with different type_names but same value -> same bytes (type_name is schema metadata).
        // Also: Value::Enum and Value::String with same value -> same bytes (cross-DB compat).
        let a = encode(&Value::Enum {
            type_name: "rating".to_string(),
            value: "PG".to_string(),
        });
        let b = encode(&Value::Enum {
            type_name: "mpaa_rating".to_string(),
            value: "PG".to_string(),
        });
        assert_eq!(a, b);
        assert_eq!(a, encode(&Value::String("PG".to_string())));
    }

    #[test]
    fn set_normalizes_to_array_of_strings() {
        // Value::Set and Value::Array of String values produce the same canonical bytes
        // (cross-DB compat: MySQL SET -> PostgreSQL TEXT[]).
        let set = encode(&Value::Set(vec!["a".to_string(), "b".to_string()]));
        let arr = encode(&Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]));
        assert_eq!(set, arr);
    }

    #[test]
    fn year_normalizes_to_int() {
        // Value::Year and Value::Int with same value -> same bytes (cross-DB compat:
        // MySQL YEAR -> PostgreSQL SMALLINT which reads back as Int).
        assert_eq!(encode(&Value::Year(2006)), encode(&Value::Int(2006)));
    }

    #[test]
    fn timestamp_utc_normalization() {
        use chrono::NaiveDate;
        // 10:00 +05:00 == 05:00 UTC
        let plus5 = Value::Timestamp {
            value: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            offset_secs: Some(5 * 3600),
        };
        let utc = Value::Timestamp {
            value: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(5, 0, 0)
                .unwrap(),
            offset_secs: None,
        };
        assert_eq!(encode(&plus5), encode(&utc));
    }

    #[test]
    fn json_keys_sorted() {
        let j: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let encoded = encode(&Value::Json(j));
        // extract the string after the 5-byte header (tag + 4-byte len)
        let s = std::str::from_utf8(&encoded[5..]).unwrap();
        assert_eq!(s, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn decode_key_round_trips_bindable_keys() {
        let id = uuid::Uuid::from_u128(42);
        let keys = [
            vec![Value::Int(-7)],
            vec![Value::String("ACME-1 é".into())],
            vec![Value::Uuid(id)],
            vec![Value::Int(3), Value::String("eu".into())],
        ];

        for key in keys {
            let bytes: Vec<u8> = key.iter().flat_map(encode).collect();
            assert_eq!(decode_key(&bytes), Some(key));
        }

        // UInt in i64 range is stored as Int, which binds the same.
        assert_eq!(
            decode_key(&encode(&Value::UInt(5))),
            Some(vec![Value::Int(5)])
        );
    }

    #[test]
    fn decode_key_refuses_lossy_types() {
        let date = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(decode_key(&encode(&date)), None);
        assert_eq!(decode_key(&encode(&Value::Null)), None);
        assert_eq!(decode_key(&[TAG_INT, 1, 2]), None);
    }
}
