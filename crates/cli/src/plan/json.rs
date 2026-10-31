use super::fmt::filter_preview;
use engine_core::plan::pair::{PairSide, PairSpec};
use engine_planner::plan::execution::migration_report::MigrationReport;
use serde::Serialize;
use std::collections::BTreeMap;

/// `plan --json`: the planner's report, with the config's `pair` blocks next to it.
#[derive(Serialize)]
pub struct PlanJson<'a> {
    /// `None` for a config with only pairs: there is nothing for apply to plan.
    #[serde(flatten)]
    pub report: Option<&'a MigrationReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pairs: Vec<PairJson>,
}

#[derive(Serialize)]
pub struct PairJson {
    pub name: String,
    pub source: PairSideJson,
    pub target: PairSideJson,
    /// Source column -> target column, for columns named differently.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub map: BTreeMap<String, String>,
    /// Key columns by source name; absent when a primary key is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<String>>,
    pub grace_secs: u64,
}

#[derive(Serialize)]
pub struct PairSideJson {
    pub connection: String,
    pub table: String,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ignore: Vec<String>,
}

impl<'a> PlanJson<'a> {
    pub fn new(report: Option<&'a MigrationReport>, pairs: &[PairSpec]) -> Self {
        Self {
            report,
            pairs: pairs.iter().map(PairJson::from).collect(),
        }
    }
}

impl From<&PairSpec> for PairJson {
    fn from(p: &PairSpec) -> Self {
        Self {
            name: p.name.clone(),
            source: PairSideJson::from(&p.source),
            target: PairSideJson::from(&p.target),
            map: p.columns.clone(),
            key: p.key.clone(),
            grace_secs: p.grace.as_secs(),
        }
    }
}

impl From<&PairSide> for PairSideJson {
    fn from(side: &PairSide) -> Self {
        Self {
            connection: side.connection.name.clone(),
            table: side.table.clone(),
            filter: side
                .filter
                .as_ref()
                .and_then(|f| f.expr.as_ref())
                .map(|e| filter_preview(e, true)),
            ignore: side.ignore.clone(),
        }
    }
}
