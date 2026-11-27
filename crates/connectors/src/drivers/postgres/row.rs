use super::numeric::PgNumeric;
use crate::traits::row_decoder::RowDecoder;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use model::{
    core::{types::Type, value::Value},
    records::{OpType, Record, RecordSchema, SchemaColumn},
};
use std::net::IpAddr;
use std::sync::Arc;
use tokio_postgres::Row as PgRow;
use tokio_postgres::types::{FromSql, Kind, Type as PgType};
use uuid::Uuid;

/// Reads any PostgreSQL enum value as its label text.
struct PgEnumText(String);

impl<'a> FromSql<'a> for PgEnumText {
    fn from_sql(
        _ty: &PgType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // Enum values are transmitted as their label text.
        Ok(PgEnumText(std::str::from_utf8(raw)?.to_owned()))
    }

    fn accepts(ty: &PgType) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

/// Wrapper for PostgreSQL Row to implement RowDecoder
pub struct PgRowDecoder<'a>(pub &'a PgRow);

impl<'a> RowDecoder for PgRowDecoder<'a> {
    fn schema(&self, table: &str) -> Arc<RecordSchema> {
        let columns = self
            .0
            .columns()
            .iter()
            .map(|col| SchemaColumn::new(col.name(), pg_type_to_canonical(col.type_())))
            .collect();
        RecordSchema::new(table, columns)
    }

    fn decode_with_schema(&self, schema: &Arc<RecordSchema>) -> Record {
        let values = self
            .0
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| extract_value(self.0, idx, col.type_()))
            .collect();

        Record::new(Arc::clone(schema), values, OpType::default())
    }

    fn columns(&self) -> Vec<String> {
        self.0
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    }

    fn get_string(&self, column: &str) -> Option<String> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        self.0.try_get::<_, String>(idx).ok()
    }

    fn get_i32(&self, column: &str) -> Option<i32> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        self.0.try_get::<_, i32>(idx).ok()
    }

    fn get_u32(&self, column: &str) -> Option<u32> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        self.0.try_get::<_, i32>(idx).ok().map(|v| v as u32)
    }

    fn get_i64(&self, column: &str) -> Option<i64> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        self.0.try_get::<_, i64>(idx).ok()
    }

    fn get_bool(&self, column: &str) -> Option<bool> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        self.0.try_get::<_, bool>(idx).ok()
    }

    fn get_value(&self, column: &str) -> Option<Value> {
        let idx = self.0.columns().iter().position(|c| c.name() == column)?;
        let pg_type = self.0.columns()[idx].type_();
        extract_value(self.0, idx, pg_type)
    }
}

/// Convert PostgreSQL type to canonical Type
fn pg_type_to_canonical(pg_type: &PgType) -> Type {
    use model::core::types::{FloatSize, IntSize};

    match *pg_type {
        // Integer types
        PgType::INT2 => Type::Int {
            bits: IntSize::I16,
            unsigned: false,
            auto_increment: false,
        },
        PgType::INT4 => Type::Int {
            bits: IntSize::I32,
            unsigned: false,
            auto_increment: false,
        },
        PgType::INT8 => Type::Int {
            bits: IntSize::I64,
            unsigned: false,
            auto_increment: false,
        },

        // Floating point
        PgType::FLOAT4 => Type::Float {
            bits: FloatSize::F32,
        },
        PgType::FLOAT8 => Type::Float {
            bits: FloatSize::F64,
        },

        // Decimal
        PgType::NUMERIC => Type::Decimal {
            precision: None,
            scale: None,
        },

        // String types
        PgType::VARCHAR => Type::Varchar {
            length: None,
            charset: None,
        },
        PgType::CHAR | PgType::BPCHAR => Type::Char {
            length: None,
            charset: None,
        },
        PgType::TEXT => Type::Text { charset: None },

        // Binary
        PgType::BYTEA => Type::Blob { max_bytes: None },

        // Temporal
        PgType::DATE => Type::Date,
        PgType::TIME => Type::Time {
            precision: None,
            with_tz: false,
        },
        PgType::TIMETZ => Type::Time {
            precision: None,
            with_tz: true,
        },
        PgType::TIMESTAMP => Type::Timestamp {
            precision: None,
            with_tz: false,
        },
        PgType::TIMESTAMPTZ => Type::Timestamp {
            precision: None,
            with_tz: true,
        },

        // Boolean
        PgType::BOOL => Type::Boolean,

        // JSON
        PgType::JSON => Type::Json { binary: false },
        PgType::JSONB => Type::Json { binary: true },

        // UUID
        PgType::UUID => Type::Uuid,

        // Network types
        PgType::INET => Type::Inet,
        PgType::CIDR => Type::Cidr,
        PgType::MACADDR => Type::MacAddr,

        // Unknown
        _ => Type::Unknown {
            source_name: pg_type.name().to_string(),
            fallback_ddl: "TEXT".to_string(),
        },
    }
}

/// Extract a value from a PostgreSQL row at the given index
fn extract_value(row: &PgRow, idx: usize, pg_type: &PgType) -> Option<Value> {
    match *pg_type {
        // Integer types
        PgType::INT2 => row
            .try_get::<_, i16>(idx)
            .ok()
            .map(|v| Value::Int(v as i64)),
        PgType::INT4 => row
            .try_get::<_, i32>(idx)
            .ok()
            .map(|v| Value::Int(v as i64)),
        PgType::INT8 => row.try_get::<_, i64>(idx).ok().map(Value::Int),

        // Floating point
        PgType::FLOAT4 => row
            .try_get::<_, f32>(idx)
            .ok()
            .map(|v| Value::Float(v as f64)),
        PgType::FLOAT8 => row.try_get::<_, f64>(idx).ok().map(Value::Float),

        // Decimal
        PgType::NUMERIC => row
            .try_get::<_, PgNumeric>(idx)
            .ok()
            .map(|d| Value::Decimal(d.0)),

        // String types.
        // BPCHAR (CHAR(n)) is blank-padded to the declared length on storage and
        // returned with trailing spaces by the wire protocol. Trim them so that the
        // logical value matches what MySQL (and every other engine) returns for
        // fixed-length CHAR columns. This keeps canonical hashes consistent across
        // source and destination.
        PgType::BPCHAR => row
            .try_get::<_, String>(idx)
            .ok()
            .map(|s| Value::String(s.trim_end_matches(' ').to_string())),
        PgType::VARCHAR | PgType::CHAR | PgType::TEXT | PgType::NAME => {
            row.try_get::<_, String>(idx).ok().map(Value::String)
        }

        // Binary
        PgType::BYTEA => row.try_get::<_, Vec<u8>>(idx).ok().map(Value::Binary),

        // Temporal
        PgType::DATE => row.try_get::<_, NaiveDate>(idx).ok().map(Value::Date),
        PgType::TIME => row.try_get::<_, NaiveTime>(idx).ok().map(|v| Value::Time {
            value: v,
            offset_secs: None,
        }),
        PgType::TIMETZ => row.try_get::<_, NaiveTime>(idx).ok().map(|v| Value::Time {
            value: v,
            offset_secs: Some(0),
        }),
        PgType::TIMESTAMP => row
            .try_get::<_, NaiveDateTime>(idx)
            .ok()
            .map(|v| Value::Timestamp {
                value: v,
                offset_secs: None,
            }),
        PgType::TIMESTAMPTZ => {
            row.try_get::<_, DateTime<Utc>>(idx)
                .ok()
                .map(|v| Value::Timestamp {
                    value: v.naive_utc(),
                    offset_secs: Some(0),
                })
        }

        // Boolean
        PgType::BOOL => row.try_get::<_, bool>(idx).ok().map(Value::Boolean),

        // JSON/JSONB
        PgType::JSON | PgType::JSONB => row
            .try_get::<_, serde_json::Value>(idx)
            .ok()
            .map(Value::Json),

        // UUID
        PgType::UUID => row.try_get::<_, Uuid>(idx).ok().map(Value::Uuid),

        // Network types
        PgType::INET => row.try_get::<_, IpAddr>(idx).ok().map(Value::IpAddr),

        // Enum types: decode the label as text.
        _ if matches!(pg_type.kind(), Kind::Enum(_)) => row
            .try_get::<_, PgEnumText>(idx)
            .ok()
            .map(|e| Value::String(e.0)),

        // Arrays - handle text arrays as common case
        _ if pg_type.name().starts_with('_') => {
            // Array type
            if let Ok(arr) = row.try_get::<_, Vec<String>>(idx) {
                Some(Value::Array(arr.into_iter().map(Value::String).collect()))
            } else if let Ok(arr) = row.try_get::<_, Vec<i64>>(idx) {
                Some(Value::Array(arr.into_iter().map(Value::Int).collect()))
            } else {
                None
            }
        }

        // Fallback - try string
        _ => row.try_get::<_, String>(idx).ok().map(Value::String),
    }
}
