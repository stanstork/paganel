use super::numeric::PgNumeric;
use bigdecimal::BigDecimal;
use bytes::BytesMut;
use chrono::{DateTime, TimeZone, Utc};
use model::core::value::Value;
use rust_decimal::Decimal as RustDecimal;
use std::str::FromStr;
use tokio_postgres::types::{IsNull, Json as PgJson, ToSql, Type, to_sql_checked};

/// Numeric parameter that adapts to whatever numeric column it is compared against.
#[derive(Debug)]
struct FlexInt(i64);

impl ToSql for FlexInt {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::INT2 => (self.0 as i16).to_sql(ty, out),
            Type::INT4 => (self.0 as i32).to_sql(ty, out),
            Type::FLOAT4 => (self.0 as f32).to_sql(ty, out),
            Type::FLOAT8 => (self.0 as f64).to_sql(ty, out),
            Type::NUMERIC => RustDecimal::from(self.0).to_sql(ty, out),
            _ => self.0.to_sql(ty, out),
        }
    }
    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC
        )
    }
    to_sql_checked!();
}

/// Floating-point parameter that accepts any PG numeric column type.
#[derive(Debug)]
struct FlexFloat(f64);

impl ToSql for FlexFloat {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::FLOAT4 => (self.0 as f32).to_sql(ty, out),
            Type::INT2 => (self.0 as i16).to_sql(ty, out),
            Type::INT4 => (self.0 as i32).to_sql(ty, out),
            Type::INT8 => (self.0 as i64).to_sql(ty, out),
            Type::NUMERIC => {
                // Route through the decimal's text form: `f64 -> Decimal` has no
                // direct lossless constructor.
                let decimal = RustDecimal::from_str(&self.0.to_string())
                    .map_err(|e| format!("cannot bind {} as NUMERIC: {e}", self.0))?;
                decimal.to_sql(ty, out)
            }
            _ => self.0.to_sql(ty, out),
        }
    }
    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC | Type::INT2 | Type::INT4 | Type::INT8
        )
    }
    to_sql_checked!();
}

/// Text parameter that adapts to the column type PostgreSQL infers.
#[derive(Debug)]
struct FlexLiteral(String);

impl ToSql for FlexLiteral {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let raw = self.0.trim();
        match *ty {
            Type::BOOL => match raw.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "on" => true.to_sql(ty, out),
                "false" | "f" | "0" | "no" | "off" => false.to_sql(ty, out),
                other => Err(format!("cannot bind {other:?} as BOOL").into()),
            },
            Type::INT2 => raw.parse::<i16>()?.to_sql(ty, out),
            Type::INT4 => raw.parse::<i32>()?.to_sql(ty, out),
            Type::INT8 => raw.parse::<i64>()?.to_sql(ty, out),
            Type::FLOAT4 => raw.parse::<f32>()?.to_sql(ty, out),
            Type::FLOAT8 => raw.parse::<f64>()?.to_sql(ty, out),
            Type::NUMERIC => PgNumeric(BigDecimal::from_str(raw)?).to_sql(ty, out),
            _ => self.0.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <String as ToSql>::accepts(ty)
            || matches!(
                *ty,
                Type::BOOL
                    | Type::INT2
                    | Type::INT4
                    | Type::INT8
                    | Type::FLOAT4
                    | Type::FLOAT8
                    | Type::NUMERIC
            )
    }
    to_sql_checked!();
}

pub struct PgParam(Box<dyn ToSql + Sync + Send>);

impl PgParam {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => PgParam(Box::new(Option::<String>::None)),

            // Numeric types
            Value::Int(i) => PgParam(Box::new(FlexInt(*i))),
            Value::UInt(u) => PgParam(Box::new(FlexInt(*u as i64))),
            Value::Float(f) => PgParam(Box::new(FlexFloat(*f))),
            Value::Decimal(d) => PgParam(Box::new(PgNumeric(d.clone()))),

            // String. Bound flexibly: filter literals arrive as text regardless of
            // the column they are compared against.
            Value::String(s) => PgParam(Box::new(FlexLiteral(s.clone()))),

            // Binary
            Value::Binary(b) => PgParam(Box::new(b.clone())),

            // Boolean
            Value::Boolean(b) => PgParam(Box::new(*b)),

            // Temporal types
            Value::Date(d) => PgParam(Box::new(*d)),
            Value::Time { value: t, .. } => PgParam(Box::new(*t)),
            Value::Timestamp {
                value: ts,
                offset_secs: Some(offset),
            } => {
                // TIMESTAMPTZ column: send as DateTime<Utc>
                let dt: DateTime<Utc> =
                    Utc.from_utc_datetime(ts) + chrono::Duration::seconds(*offset as i64);
                PgParam(Box::new(dt))
            }
            Value::Timestamp { value: ts, .. } => PgParam(Box::new(*ts)),
            Value::Year(y) => PgParam(Box::new(*y)),

            // JSON
            Value::Json(j) => PgParam(Box::new(PgJson(j.clone()))),

            // UUID
            Value::Uuid(u) => PgParam(Box::new(*u)),

            // Enum and Set
            Value::Enum { value: v, .. } => PgParam(Box::new(v.clone())),
            Value::Set(values) => PgParam(Box::new(values.clone())),

            // Array
            Value::Array(arr) => {
                // Convert to string array for simplicity
                let strings: Vec<String> = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        Value::Int(i) => i.to_string(),
                        Value::UInt(u) => u.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        _ => String::new(),
                    })
                    .collect();
                PgParam(Box::new(strings))
            }

            // Bits - convert to bytes
            Value::Bits(bits) => {
                let mut bytes = Vec::new();
                for chunk in bits.chunks(8) {
                    let mut byte = 0u8;
                    for (i, &bit) in chunk.iter().enumerate() {
                        if bit {
                            byte |= 1 << (7 - i);
                        }
                    }
                    bytes.push(byte);
                }
                PgParam(Box::new(bytes))
            }

            // Interval - as string (PG has native interval but complex to convert)
            Value::Interval(iv) => {
                let interval_str = format!(
                    "{} months {} days {} microseconds",
                    iv.months, iv.days, iv.microseconds
                );
                PgParam(Box::new(interval_str))
            }

            // Network types
            Value::IpAddr(addr) => PgParam(Box::new(*addr)),
            Value::Cidr { addr, prefix } => PgParam(Box::new(format!("{}/{}", addr, prefix))),
            Value::MacAddr(mac) => PgParam(Box::new(format!(
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
            ))),

            // Geometry - as binary (WKB)
            Value::Geometry(g) => PgParam(Box::new(g.clone())),

            // Composite - as JSON
            Value::Composite { fields, .. } => {
                let json_obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| {
                        let json_val = match v {
                            Value::String(s) => serde_json::Value::String(s.clone()),
                            Value::Int(i) => serde_json::json!(i),
                            Value::UInt(u) => serde_json::json!(u),
                            Value::Float(f) => serde_json::json!(f),
                            Value::Boolean(b) => serde_json::json!(b),
                            Value::Null => serde_json::Value::Null,
                            _ => serde_json::Value::Null,
                        };
                        (k.clone(), json_val)
                    })
                    .collect();
                PgParam(Box::new(PgJson(serde_json::Value::Object(json_obj))))
            }
        }
    }
}

impl AsRef<dyn ToSql + Sync> for PgParam {
    fn as_ref(&self) -> &(dyn ToSql + Sync + 'static) {
        &*self.0
    }
}

pub struct PgParamStore {
    pub params: Vec<PgParam>,
}

impl PgParamStore {
    pub fn from_values(values: &[Value]) -> Self {
        let params = values.iter().map(PgParam::from_value).collect();
        PgParamStore { params }
    }

    pub fn as_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params.iter().map(|param| param.as_ref()).collect()
    }
}
