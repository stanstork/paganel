use super::{MAX_RANGES_PER_QUERY, plan::normal_sql};
use crate::{HashSpec, error::VerifyError};
use connectors::{
    sql::{
        checksum::{self, NormalColumn, NormalSql, SegmentHash, Segments},
        filter::SqlFilter,
        metadata::table::TableMetadata,
    },
    traits::executor::QueryExecutor,
};
use engine_core::{dispatch_driver, drivers::DriverRef, plan::pair::PairSide};
use model::{core::value::Value, records::Record};
use query_builder::{
    ast::{expr::Expr, select::Select},
    renderer::{Render, Renderer},
};
use std::{collections::BTreeMap, time::Duration};

/// The client waits this much longer than the statement timeout, so the
/// server's own limit fires first, with its clearer error.
pub(super) const TIMEOUT_MARGIN: Duration = Duration::from_secs(30);
/// The client-side wait when no statement timeout is set.
pub(super) const DEFAULT_QUERY_WAIT: Duration = Duration::from_secs(3600);

/// One side of a block comparison.
pub(super) struct Side<'a> {
    pub(super) name: &'static str,
    pub(super) driver: &'a DriverRef,
    pub(super) sql: &'static dyn NormalSql,
    pub(super) table: &'a str,
    pub(super) meta: &'a TableMetadata,
    pub(super) spec: &'a HashSpec,
    pub(super) filter: Option<&'a SqlFilter>,
    pub(super) filter_expr: Option<Expr>,
    pub(super) columns: Vec<NormalColumn>,
    pub(super) wait: Duration,
}

/// What a side's key range probe found.
pub(super) enum Probe {
    Empty,
    Range(i64, i64),
    Unsupported(String),
}

impl<'a> Side<'a> {
    pub(super) fn new(
        name: &'static str,
        driver: &'a DriverRef,
        side: &'a PairSide,
        meta: &'a TableMetadata,
        spec: &'a HashSpec,
        columns: Vec<NormalColumn>,
        wait: Duration,
    ) -> Result<Self, VerifyError> {
        let filter_expr = checksum::filter_expr(side.filter.as_ref())
            .map_err(|e| VerifyError::InitializationError(format!("{name} filter: {e}")))?;
        Ok(Self {
            name,
            driver,
            sql: normal_sql(driver),
            table: &side.table,
            meta,
            spec,
            filter: side.filter.as_ref(),
            filter_expr,
            columns,
            wait,
        })
    }

    pub(super) fn key(&self) -> &str {
        &self.spec.key_columns[0]
    }

    pub(super) async fn query(&self, select: &Select) -> Result<Vec<Record>, VerifyError> {
        let mut renderer = Renderer::new(self.sql.dialect());
        select.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let run = async { dispatch_driver!(self.driver, |d| d.query_params(&sql, &params).await) };
        let rows = tokio::time::timeout(self.wait, run).await.map_err(|_| {
            VerifyError::InitializationError(format!(
                "{} side: a block-mode query ran longer than {:?}",
                self.name, self.wait
            ))
        })?;
        Ok(rows?)
    }

    pub(super) async fn key_range(&self) -> Result<Probe, VerifyError> {
        let select = checksum::key_range_query(self.table, self.key(), self.filter_expr.as_ref());
        let rows = self.query(&select).await?;
        let Some(row) = rows.first() else {
            return Ok(Probe::Empty);
        };

        Ok(
            match (parse_i64(row.value("lo")), parse_i64(row.value("hi"))) {
                (Ok(Some(lo)), Ok(Some(hi))) => Probe::Range(lo, hi),
                (Err(v), _) | (_, Err(v)) => Probe::Unsupported(format!(
                    "{} key {} reaches {v}, outside the signed 64-bit range block mode segments",
                    self.name,
                    self.key()
                )),
                _ => Probe::Empty,
            },
        )
    }

    /// Count and fingerprint per segment.
    pub(super) async fn fingerprints(
        &self,
        segments: &Segments,
    ) -> Result<BTreeMap<i64, SegmentHash>, VerifyError> {
        let parts: Vec<Segments> = match segments {
            Segments::Grid { .. } => vec![segments.clone()],
            Segments::Ranges(ranges) => ranges
                .chunks(MAX_RANGES_PER_QUERY)
                .map(|c| Segments::Ranges(c.to_vec()))
                .collect(),
        };

        let mut out = BTreeMap::new();
        for part in parts {
            let select = checksum::fingerprint_query(
                self.sql,
                self.table,
                self.key(),
                &self.columns,
                &part,
                self.filter_expr.as_ref(),
            );
            for row in self.query(&select).await? {
                let hash = checksum::parse_segment_hash(&row).ok_or_else(|| {
                    VerifyError::InitializationError(format!(
                        "{} side: unreadable fingerprint row {row:?}",
                        self.name
                    ))
                })?;
                out.insert(hash.seg, hash);
            }
        }
        Ok(out)
    }
}

fn parse_i64(value: Option<&Value>) -> Result<Option<i64>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Int(i)) => Ok(Some(*i)),
        Some(Value::UInt(u)) => i64::try_from(*u).map(Some).map_err(|_| u.to_string()),
        Some(other) => Err(format!("{other:?}")),
    }
}
