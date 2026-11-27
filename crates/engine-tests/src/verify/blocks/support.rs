use crate::harness::ppl::feature_ppl;
use connectors::session::SessionSettings;
use engine_core::plan::execution::ExecutionPlan;
use engine_processing::EnvContext;
use ppl_syntax::builder::parse;
use std::{sync::Arc, time::Duration};

/// `src` is the Sakila MySQL fixture, `dst` the scratch Postgres.
pub(super) fn plan() -> ExecutionPlan {
    let doc = parse(&feature_ppl("")).expect("parse");
    ExecutionPlan::build(&doc, Arc::new(EnvContext::empty())).expect("plan")
}

pub(super) fn settings() -> SessionSettings {
    SessionSettings {
        statement_timeout: Some(Duration::from_secs(600)),
    }
}
