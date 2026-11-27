use super::sides::{Sides, both};
use crate::{HashSpec, error::VerifyError, table::hash_rows_where};
use connectors::sql::{filter::SqlFilter, metadata::table::TableMetadata};
use engine_core::drivers::DriverRef;
use model::{
    core::value::Value,
    integrity::{
        canonical::decode_key,
        report::{DivergenceReport, Recheck, TableReport},
        result::{Divergence, DivergenceKind, MAX_REPORTED_DIVERGENCES},
        row_key::describe,
    },
};
use std::collections::HashMap;
use tracing::{Instrument, info_span};

/// Values per `IN (...)` when re-reading differing keys.
const RECHECK_CHUNK: usize = 500;

/// The keys that still differ after `grace`.
#[derive(Default)]
pub(super) struct Rechecked {
    missing: u64,
    changed: u64,
    extra: u64,
    settled: u64,
    divergences: Vec<Divergence>,
}

impl Sides<'_> {
    /// Re-read `keys` from both sides and diff them again.
    pub(super) async fn recheck(&self, keys: &[Vec<u8>]) -> Result<Option<Rechecked>, VerifyError> {
        let firsts = keys
            .iter()
            .filter_map(|key| decode_key(key).and_then(|v| v.into_iter().next()))
            .fold(Vec::new(), |mut acc, v| {
                if !acc.contains(&v) {
                    acc.push(v);
                }
                acc
            });

        if firsts.is_empty() && !keys.is_empty() {
            return Ok(None);
        }

        let (now_source, now_target) = both(
            self.shared,
            read_keys(
                self.source,
                self.source_meta,
                &self.specs.source,
                self.spec.source.filter.as_ref(),
                &firsts,
            )
            .instrument(info_span!("pair", side = %"source")),
            read_keys(
                self.target,
                self.target_meta,
                &self.specs.target,
                self.spec.target.filter.as_ref(),
                &firsts,
            )
            .instrument(info_span!("pair", side = %"target")),
        )
        .await?;

        let mut rechecked = Rechecked::default();
        for key in keys {
            let kind = match (now_source.get(key), now_target.get(key)) {
                (Some(s), Some(t)) if s == t => None,
                (None, None) => None,
                (Some(s), None) => Some(DivergenceKind::Missing { expected_hash: *s }),
                (None, Some(t)) => Some(DivergenceKind::Extra { actual_hash: *t }),
                (Some(s), Some(t)) => Some(DivergenceKind::Changed {
                    expected_hash: *s,
                    actual_hash: *t,
                }),
            };

            let Some(kind) = kind else {
                rechecked.settled += 1;
                continue;
            };

            match &kind {
                DivergenceKind::Missing { .. } => rechecked.missing += 1,
                DivergenceKind::Extra { .. } => rechecked.extra += 1,
                DivergenceKind::Changed { .. } => rechecked.changed += 1,
            }

            if rechecked.divergences.len() < MAX_REPORTED_DIVERGENCES {
                rechecked.divergences.push(Divergence {
                    key: describe(key, &self.specs.source.key_columns),
                    kind,
                });
            }
        }

        Ok(Some(rechecked))
    }
}

impl Rechecked {
    pub(super) fn apply(self, report: &mut TableReport) -> Recheck {
        report.missing = self.missing;
        report.changed = self.changed;
        report.extra = self.extra;
        report.divergences = self
            .divergences
            .iter()
            .map(DivergenceReport::from)
            .collect();
        Recheck::Done {
            settled: self.settled,
        }
    }
}

async fn read_keys(
    driver: &DriverRef,
    meta: &TableMetadata,
    spec: &HashSpec,
    filter: Option<&SqlFilter>,
    values: &[Value],
) -> Result<HashMap<Vec<u8>, [u8; 32]>, VerifyError> {
    let column = &spec.key_columns[0];
    let mut hashes = HashMap::new();

    for chunk in values.chunks(RECHECK_CHUNK) {
        hashes.extend(hash_rows_where(driver, meta, spec, filter, column, chunk.to_vec()).await?);
    }

    Ok(hashes)
}
