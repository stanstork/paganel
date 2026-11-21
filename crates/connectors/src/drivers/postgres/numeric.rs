use bigdecimal::{
    BigDecimal,
    num_bigint::{BigInt, Sign},
};
use bytes::{BufMut, BytesMut};
use std::error::Error;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

const SIGN_POSITIVE: u16 = 0x0000;
const SIGN_NEGATIVE: u16 = 0x4000;
const NBASE: u32 = 10_000;
const HEADER_LEN: usize = 8;

/// A `numeric` value of any precision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgNumeric(pub BigDecimal);

type BoxError = Box<dyn Error + Sync + Send>;

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        if raw.len() < HEADER_LEN {
            return Err("numeric: truncated value".into());
        }

        let ndigits = usize::from(u16::from_be_bytes([raw[0], raw[1]]));
        let weight = i64::from(i16::from_be_bytes([raw[2], raw[3]]));
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = i64::from(u16::from_be_bytes([raw[6], raw[7]]));

        let negative = match sign {
            SIGN_POSITIVE => false,
            SIGN_NEGATIVE => true,
            _ => return Err("numeric: NaN and infinity have no decimal value".into()),
        };

        let digit_bytes = raw
            .get(HEADER_LEN..HEADER_LEN + 2 * ndigits)
            .ok_or("numeric: truncated value")?;

        let mut mantissa = BigInt::default();
        for chunk in digit_bytes.chunks_exact(2) {
            let digit = u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
            if digit >= NBASE {
                return Err(format!("numeric: digit {digit} out of range").into());
            }
            mantissa = mantissa * NBASE + digit;
        }
        if negative {
            mantissa = -mantissa;
        }

        // The last digit sits at 10000^(weight - ndigits + 1). Shown with
        // `dscale` fractional digits: base-10000 digits can carry up to three
        // zeros past it (1.10 arrives as [1, 1000]), never anything else.
        let scale = -4 * (weight - ndigits as i64 + 1);
        Ok(PgNumeric(
            BigDecimal::new(mantissa, scale).with_scale(dscale),
        ))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

impl ToSql for PgNumeric {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        const POW10: [u16; 4] = [1, 10, 100, 1_000];

        let (mantissa, scale) = self.0.as_bigint_and_exponent();
        let dscale = u16::try_from(scale.max(0)).map_err(|_| "numeric: scale too large")?;

        if mantissa.sign() == Sign::NoSign {
            out.reserve(HEADER_LEN);
            out.put_u16(0);
            out.put_i16(0);
            out.put_u16(SIGN_POSITIVE);
            out.put_u16(dscale);
            return Ok(IsNull::No);
        }

        let digits = mantissa.magnitude().to_string();
        let sig = digits.trim_end_matches('0');

        // Decimal place (power of 10) of the first and last non-zero digits.
        let top_p = (digits.len() as i128 - 1) - i128::from(scale);
        let bot_p = top_p - (sig.len() as i128 - 1);
        let first_group = top_p.div_euclid(4);
        let last_group = bot_p.div_euclid(4);

        let ndigits =
            u16::try_from(first_group - last_group + 1).map_err(|_| "numeric: too many digits")?;
        let weight = i16::try_from(first_group).map_err(|_| "numeric: value out of range")?;

        let mut groups = vec![0u16; usize::from(ndigits)];
        for (i, &b) in sig.as_bytes().iter().enumerate() {
            let p = top_p - i as i128;
            let idx = (first_group - p.div_euclid(4)) as usize;
            groups[idx] += u16::from(b - b'0') * POW10[p.rem_euclid(4) as usize];
        }

        let sign = if mantissa.sign() == Sign::Minus {
            SIGN_NEGATIVE
        } else {
            SIGN_POSITIVE
        };

        out.reserve(HEADER_LEN + 2 * groups.len());
        out.put_u16(ndigits);
        out.put_i16(weight);
        out.put_u16(sign);
        out.put_u16(dscale);
        for g in groups {
            out.put_u16(g);
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn round_trip(s: &str) -> BigDecimal {
        let mut buf = BytesMut::new();
        PgNumeric(BigDecimal::from_str(s).unwrap())
            .to_sql(&Type::NUMERIC, &mut buf)
            .unwrap();
        PgNumeric::from_sql(&Type::NUMERIC, &buf).unwrap().0
    }

    #[test]
    fn wide_values_survive_a_round_trip() {
        for s in [
            "0",
            "0.00",
            "1.10",
            "-0.50",
            "100",
            "10000",
            "0.0001",
            "123456789.000000001",
            "1.000000000000000000000000000001",
            "-0.000000000000000000000000000001",
            "12345678901234567890123456789012345.5",
            "99999999999999999999999999999999999999999999999999",
        ] {
            let back = round_trip(s);
            assert_eq!(back, BigDecimal::from_str(s).unwrap(), "{s}");
            assert_eq!(
                back.to_string(),
                BigDecimal::from_str(s).unwrap().to_string(),
                "{s} keeps its scale"
            );
        }
    }

    /// The bytes Postgres sends for `-1234.5600`: 2 digits, weight 0,
    /// negative, dscale 4, digits [1234, 5600].
    #[test]
    fn decodes_the_wire_format() {
        let raw = [0, 2, 0, 0, 0x40, 0, 0, 4, 0x04, 0xD2, 0x15, 0xE0];
        let value = PgNumeric::from_sql(&Type::NUMERIC, &raw).unwrap().0;
        assert_eq!(value.to_string(), "-1234.5600");

        let mut buf = BytesMut::new();
        PgNumeric(value).to_sql(&Type::NUMERIC, &mut buf).unwrap();
        assert_eq!(&buf[..], &raw[..]);
    }

    #[test]
    fn nan_is_an_error() {
        let raw = [0, 0, 0, 0, 0xC0, 0, 0, 0];
        assert!(PgNumeric::from_sql(&Type::NUMERIC, &raw).is_err());
    }
}
