mod expr;
mod mysql;
mod postgres;
mod query;
mod result;

pub use mysql::MySqlNormal;
pub use postgres::PostgresNormal;
pub use query::{filter_expr, fingerprint_query, key_range_query, normal_select};
pub use result::parse_segment_hash;

use crate::sql::metadata::column::ColumnMetadata;
use model::integrity::normal::Family;
use query_builder::{ast::expr::Expr, dialect::QueryDialect};

/// What one side's column is, as far as normal text cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Int,
    Bool,
    /// `scale` is the declared number of fractional digits.
    Decimal {
        scale: u32,
    },
    Text,
    Enum,
    Date,
    /// No time zone: MySQL `DATETIME`, Postgres `timestamp`.
    Timestamp,
    /// A UTC instant: MySQL `TIMESTAMP` (read in the session's UTC),
    /// Postgres `timestamptz`.
    TimestampTz,
    Uuid,
    Binary,
}

impl ColumnKind {
    fn family(self) -> Family {
        match self {
            ColumnKind::Int | ColumnKind::Bool => Family::Int,
            ColumnKind::Decimal { .. } => Family::Decimal,
            ColumnKind::Text | ColumnKind::Enum => Family::Text,
            ColumnKind::Date => Family::Date,
            ColumnKind::Timestamp | ColumnKind::TimestampTz => Family::Timestamp,
            ColumnKind::Uuid => Family::Uuid,
            ColumnKind::Binary => Family::Binary,
        }
    }
}

/// One compared column on one side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalColumn {
    pub name: String,
    pub kind: ColumnKind,
}

/// Where a fingerprint query's segment numbers come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segments {
    /// Level 0: segment `(key - lo) / width`.
    Grid {
        /// Smallest key on either side.
        lo: i64,
        /// Keys per segment.
        width: i64,
    },
    /// Narrowing: `(seg, lo, hi)`, keys in `[lo, hi]` are segment `seg`.
    Ranges(Vec<(i64, i64, i64)>),
}

/// One segment's result on one side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHash {
    pub seg: i64,
    pub rows: u64,
    pub h1: u64,
    pub h2: u64,
}

/// A dialect's half of block mode.
pub trait NormalSql: Send + Sync {
    fn dialect(&self) -> &'static dyn QueryDialect;
    fn kind(&self, column: &ColumnMetadata) -> Option<ColumnKind>;

    /// The column's normal text: a text expression, NULL for NULL.
    fn normal(&self, kind: ColumnKind, column: Expr) -> Expr;

    /// `SUM` over the rows of a segment of a 16-hex-digit slice read as an
    /// unsigned 64-bit integer, modulo 2^64.
    fn fold(&self, hex16: Expr) -> Expr;

    /// SQL type for a bound integer key inside a `VALUES` list.
    fn int_key_type(&self) -> &'static str;

    /// Length in bytes of a text expression.
    fn byte_length(&self, text: Expr) -> Expr {
        expr::func("OCTET_LENGTH", vec![text])
    }

    /// Concatenation of text expressions that are never NULL.
    fn concat(&self, parts: Vec<Expr>) -> Expr {
        expr::concat_all(parts)
    }

    /// The MD5 of a text expression, as 32 lowercase hex digits.
    fn md5_hex(&self, text: Expr) -> Expr {
        expr::func("MD5", vec![text])
    }
}

/// The family two columns are compared in, or `None` when block mode can't
/// compare them.
pub fn family(source: ColumnKind, target: ColumnKind) -> Option<Family> {
    let (a, b) = (source.family(), target.family());
    (a == b).then_some(a)
}
