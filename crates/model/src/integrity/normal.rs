use crate::core::value::Value;
use chrono::{Duration, NaiveDateTime};
use std::fmt::Write;

/// How a column's values are written as normal text. Chosen from both sides'
/// column types together; a pair of types without a family is compared in
/// full mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    /// Integers of any width or sign, booleans, MySQL `YEAR`: decimal digits.
    Int,
    /// Exact decimals: digits with trailing fractional zeros removed.
    Decimal,
    /// Strings and enum labels: the UTF-8 text itself.
    Text,
    /// `YYYY-MM-DD`.
    Date,
    /// UTC `YYYY-MM-DD HH:MM:SS.ffffff`, always six fractional digits.
    Timestamp,
    /// Lowercase hyphenated UUID.
    Uuid,
    /// Lowercase hex of the bytes.
    Binary,
}

/// A value that does not belong to the family chosen for its column.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{value:?} is not a {family:?} value")]
pub struct NotInFamily {
    pub family: Family,
    pub value: Value,
}

/// The normal text of `value`, or `None` for NULL.
pub fn normal_text(family: Family, value: &Value) -> Result<Option<String>, NotInFamily> {
    let text = match (family, value) {
        (_, Value::Null) => return Ok(None),

        // TAG_INT for all of these; a UInt above i64::MAX keeps TAG_UINT, and
        // its digits are longer than any i64's, so it equals nothing else.
        (Family::Int, Value::Int(i)) => i.to_string(),
        (Family::Int, Value::UInt(u)) => u.to_string(),
        (Family::Int, Value::Boolean(b)) => u8::from(*b).to_string(),
        (Family::Int, Value::Year(y)) => y.to_string(),

        // `write_decimal` compares numeric value: trailing zeros and the scale
        // of zero carry no meaning. Plain digits, never exponent notation.
        (Family::Decimal, Value::Decimal(d)) => d.normalized().to_plain_string(),

        // TAG_STRING compares the exact bytes; an enum encodes as its label,
        // valid-UTF-8 binary as a string.
        (Family::Text, Value::String(s)) => s.clone(),
        (Family::Text, Value::Enum { value, .. }) => value.clone(),
        (Family::Text, Value::Binary(b)) => match std::str::from_utf8(b) {
            Ok(s) => s.to_string(),
            Err(_) => return Err(not_in(family, value)),
        },

        (Family::Date, Value::Date(d)) => d.format("%Y-%m-%d").to_string(),

        // Microseconds since the epoch in UTC, truncated like the encoder.
        (
            Family::Timestamp,
            Value::Timestamp {
                value: ts,
                offset_secs,
            },
        ) => {
            let offset = Duration::seconds(i64::from(offset_secs.unwrap_or(0)));
            let utc: NaiveDateTime = ts.checked_sub_signed(offset).unwrap_or(*ts);
            utc.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        }

        (Family::Uuid, Value::Uuid(u)) => u.hyphenated().to_string(),

        (Family::Binary, Value::Binary(b)) => hex(b),

        _ => return Err(not_in(family, value)),
    };
    Ok(Some(text))
}

fn not_in(family: Family, value: &Value) -> NotInFamily {
    NotInFamily {
        family,
        value: value.clone(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integrity::canonical::serialize_value;
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveTime};
    use std::str::FromStr;

    fn canonical(v: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_value(v, &mut buf);
        buf
    }

    /// A small deterministic generator, so a failure reproduces exactly.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            // splitmix64
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn dec(s: &str) -> Value {
        Value::Decimal(BigDecimal::from_str(s).expect("decimal"))
    }

    fn ts(date: NaiveDate, micros_of_day: u64, offset_secs: Option<i32>) -> Value {
        let secs = (micros_of_day / 1_000_000) as u32;
        let micros = (micros_of_day % 1_000_000) as u32;
        let time =
            NaiveTime::from_num_seconds_from_midnight_opt(secs, micros * 1_000).expect("time");
        Value::Timestamp {
            value: date.and_time(time),
            offset_secs,
        }
    }

    /// Values a family's columns can hold on either side: edge values, every
    /// variant the readers produce for it, and random ones. Equal values are
    /// generated in several forms on purpose.
    fn values(family: Family, rng: &mut Rng) -> Vec<Value> {
        let mut out = vec![Value::Null];
        match family {
            Family::Int => {
                for i in [0, 1, -1, 2, i64::MIN, i64::MAX, 1999, 2006] {
                    out.push(Value::Int(i));
                }
                for u in [0, 1, 2, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
                    out.push(Value::UInt(u));
                }
                out.extend([Value::Boolean(false), Value::Boolean(true)]);
                out.extend([Value::Year(1999), Value::Year(2006)]);
                for _ in 0..60 {
                    let small = rng.below(2_000) as i64 - 1_000;
                    out.push(Value::Int(small));
                    out.push(Value::UInt(small.unsigned_abs()));
                    out.push(Value::Int(rng.next() as i64));
                }
            }
            Family::Decimal => {
                let big = "9".repeat(40);
                for s in [
                    "0",
                    "0.00",
                    "-0.00",
                    "1",
                    "1.0",
                    "1.50",
                    "1.5",
                    "-1.5",
                    "100",
                    "1E2",
                    "10.010",
                    "0.000001",
                    "12345.6700",
                ] {
                    out.push(dec(s));
                }
                // Mantissas past i128 once trailing zeros are counted, and the
                // same numbers written without them.
                out.push(dec(&format!("1.{}", "0".repeat(40))));
                out.push(dec(&format!("{big}.{}", "0".repeat(10))));
                out.push(dec(&big));
                out.push(dec(&format!("{big}.5")));
                for _ in 0..80 {
                    let units = rng.below(100_000) as i64 - 50_000;
                    let scale = rng.below(5);
                    let zeros = "0".repeat(rng.below(3) as usize);
                    let n = BigDecimal::new(units.into(), scale as i64);
                    out.push(dec(&format!("{}{zeros}", n.to_plain_string())));
                    out.push(Value::Decimal(n));
                }
            }
            Family::Text => {
                for s in [
                    "", " ", "a", "a ", "A", "é", "e\u{301}", "~", "1:x", "US", "USD",
                ] {
                    out.push(Value::String(s.to_string()));
                    out.push(Value::Binary(s.as_bytes().to_vec()));
                }
                out.push(Value::Enum {
                    type_name: "status".into(),
                    value: "paid".into(),
                });
                out.push(Value::String("paid".into()));
                for _ in 0..60 {
                    let len = rng.below(4) as usize;
                    let s: String = (0..len)
                        .map(|_| ['a', 'b', ' ', 'é', '0'][rng.below(5) as usize])
                        .collect();
                    out.push(Value::String(s));
                }
            }
            Family::Date => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
                for _ in 0..100 {
                    let days = rng.below(80_000) as i64 - 40_000;
                    out.push(Value::Date(epoch + Duration::days(days)));
                }
                out.push(Value::Date(NaiveDate::from_ymd_opt(1000, 1, 1).expect("d")));
                out.push(Value::Date(
                    NaiveDate::from_ymd_opt(9999, 12, 31).expect("d"),
                ));
            }
            Family::Timestamp => {
                let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).expect("date");
                // The same instant with and without an offset.
                out.push(ts(day(2024, 1, 15), 10 * 3_600_000_000, Some(5 * 3600)));
                out.push(ts(day(2024, 1, 15), 5 * 3_600_000_000, None));
                out.push(ts(day(2024, 1, 15), 5 * 3_600_000_000 + 1, None));
                out.push(ts(day(1969, 12, 31), 86_399_999_999, None));
                out.push(ts(day(1970, 1, 1), 0, None));
                for _ in 0..80 {
                    let date = day(1960 + rng.below(80) as i32, 1 + rng.below(12) as u32, 1);
                    let micros = rng.below(3) * 500_000 + rng.below(2) * 3_600_000_000;
                    let offset = [None, Some(0), Some(3600), Some(-19_800)][rng.below(4) as usize];
                    out.push(ts(date, micros, offset));
                }
            }
            Family::Uuid => {
                for _ in 0..50 {
                    let u = uuid::Uuid::from_u128(u128::from(rng.below(30)) << 64 | 7);
                    out.push(Value::Uuid(u));
                }
            }
            Family::Binary => {
                for b in [&b""[..], b"\x00", b"\xff", b"\xff\x00", b"abc", b"\xc3\x28"] {
                    out.push(Value::Binary(b.to_vec()));
                }
                for _ in 0..60 {
                    let len = rng.below(3) as usize;
                    let b: Vec<u8> = (0..len)
                        .map(|_| [0x00, 0x61, 0xff][rng.below(3) as usize])
                        .collect();
                    out.push(Value::Binary(b));
                }
            }
        }
        out
    }

    const FAMILIES: [Family; 7] = [
        Family::Int,
        Family::Decimal,
        Family::Text,
        Family::Date,
        Family::Timestamp,
        Family::Uuid,
        Family::Binary,
    ];

    /// Every pair of values in a family, with whether their normal texts and
    /// their canonical bytes are equal.
    fn pairs(mut check: impl FnMut(Family, &Value, &Value, bool, bool)) {
        let mut rng = Rng(0x5EED);
        for family in FAMILIES {
            let vals = values(family, &mut rng);
            let normal: Vec<_> = vals
                .iter()
                .map(|v| normal_text(family, v).expect("in family"))
                .collect();
            let bytes: Vec<_> = vals.iter().map(canonical).collect();
            for i in 0..vals.len() {
                for j in i..vals.len() {
                    let same_text = normal[i] == normal[j];
                    let same_bytes = bytes[i] == bytes[j];
                    check(family, &vals[i], &vals[j], same_text, same_bytes);
                }
            }
        }
    }

    /// The safety property: block mode never calls two values equal that a
    /// full check would report as different.
    #[test]
    fn equal_normal_text_means_equal_canonical_bytes() {
        pairs(|family, a, b, same_text, same_bytes| {
            assert!(
                !same_text || same_bytes,
                "{family:?}: {a:?} and {b:?} share normal text but not canonical bytes"
            );
        });
    }

    /// The speed property: values a full check calls equal never make a
    /// segment differ, which would cost a leaf read on every run.
    #[test]
    fn equal_canonical_bytes_means_equal_normal_text() {
        pairs(|family, a, b, same_text, same_bytes| {
            assert!(
                !same_bytes || same_text,
                "{family:?}: {a:?} and {b:?} share canonical bytes but not normal text"
            );
        });
    }

    #[test]
    fn normal_text_examples() {
        let n = |f, v: Value| normal_text(f, &v).expect("in family");
        assert_eq!(n(Family::Int, Value::Boolean(true)), Some("1".into()));
        assert_eq!(
            n(Family::Int, Value::UInt(u64::MAX)),
            Some(u64::MAX.to_string())
        );
        assert_eq!(n(Family::Decimal, dec("100.00")), Some("100".into()));
        assert_eq!(n(Family::Decimal, dec("-0.50")), Some("-0.5".into()));
        assert_eq!(n(Family::Decimal, dec("0.000")), Some("0".into()));
        assert_eq!(
            n(
                Family::Timestamp,
                ts(
                    NaiveDate::from_ymd_opt(2024, 1, 15).expect("d"),
                    36_000_000_000,
                    Some(3600)
                )
            ),
            Some("2024-01-15 09:00:00.000000".into())
        );
        assert_eq!(
            n(Family::Binary, Value::Binary(vec![0, 0xab])),
            Some("00ab".into())
        );
        assert_eq!(n(Family::Text, Value::Null), None);
    }

    #[test]
    fn a_value_outside_its_family_is_refused() {
        assert!(normal_text(Family::Int, &dec("1")).is_err());
        assert!(normal_text(Family::Text, &Value::Binary(vec![0xff])).is_err());
        assert!(normal_text(Family::Uuid, &Value::String("x".into())).is_err());
    }
}
