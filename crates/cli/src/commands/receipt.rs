use super::open_state_store;
use crate::{
    config,
    error::CliError,
    format::{DATE_FORMAT, commas},
};
use engine_processing::EnvContext;
use model::integrity::receipt::VerificationReceipt;
use serde::Serialize;
use std::{collections::HashSet, sync::Arc};

/// Print the integrity receipts stored by `apply --integrity`.
pub async fn execute(
    config_path: Option<String>,
    json: bool,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let state = open_state_store()?;
    let mut receipts = state
        .list_receipts()
        .await
        .map_err(|e| CliError::Unknown(format!("State store error: {e}")))?;

    if let Some(path) = config_path {
        let resolved = config::resolve_path(Some(path))?;
        let plan = config::load_plan(&resolved, false, env).await?;

        let pipelines = plan
            .pipelines
            .iter()
            .map(|p| p.name.as_str())
            .collect::<HashSet<_>>();
        receipts.retain(|r| pipelines.contains(r.pipeline_name.as_str()));
    }

    receipts.sort_unstable_by(|a, b| {
        a.pipeline_name
            .cmp(&b.pipeline_name)
            .then_with(|| a.table_name.cmp(&b.table_name))
    });

    if json {
        let entries = receipts.iter().map(ReceiptJson::from).collect::<Vec<_>>();
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(());
    }

    if receipts.is_empty() {
        println!("No integrity receipts found (run `apply --integrity` first).");
        return Ok(());
    }

    for (i, r) in receipts.iter().enumerate() {
        if i > 0 {
            println!();
        }
        print!("{}", ReceiptDisplay(r));
    }

    Ok(())
}

struct ReceiptDisplay<'a>(&'a VerificationReceipt);

impl<'a> std::fmt::Display for ReceiptDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = self.0;

        writeln!(f, "{}/{}", r.pipeline_name, r.table_name)?;
        writeln!(f, "  root:       {}", r.root_hex())?;
        writeln!(f, "  rows:       {}", commas(r.total_rows))?;
        writeln!(f, "  skipped:    {}", commas(r.skipped_rows))?;

        if r.key_columns.is_empty() {
            writeln!(f, "  key:        (none - row hash used as key)")?;
        } else {
            writeln!(f, "  key:        {}", r.key_columns.join(", "))?;
        }

        writeln!(f, "  columns:    {}", r.column_order.join(", "))?;
        writeln!(f, "  algorithm:  {:?}", r.algorithm)?;
        writeln!(f, "  run:        {}", r.run_id)?;
        writeln!(f, "  created:    {}", r.created_at.format(DATE_FORMAT))?;

        Ok(())
    }
}

/// The `--json` shape: the stored receipt with the root as hex instead of a
/// byte array, so it can be diffed and pasted as text.
#[derive(Serialize)]
struct ReceiptJson<'a> {
    pipeline: &'a str,
    table: &'a str,
    root: String,
    rows: u64,
    skipped_rows: u64,
    key_columns: &'a [String],
    columns: &'a [String],
    algorithm: String,
    run_id: &'a str,
    created_at: String,
}

impl<'a> From<&'a VerificationReceipt> for ReceiptJson<'a> {
    fn from(r: &'a VerificationReceipt) -> Self {
        Self {
            pipeline: &r.pipeline_name,
            table: &r.table_name,
            root: r.root_hex(),
            rows: r.total_rows,
            skipped_rows: r.skipped_rows,
            key_columns: &r.key_columns,
            columns: &r.column_order,
            algorithm: format!("{:?}", r.algorithm),
            run_id: &r.run_id,
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::algorithm::HashAlgorithm;

    fn receipt() -> VerificationReceipt {
        VerificationReceipt {
            run_id: "run-1".into(),
            pipeline_name: "migrate_actor".into(),
            table_name: "actor".into(),
            table_root: [0xab; 32],
            column_order: vec!["actor_id".into(), "first_name".into()],
            key_columns: vec!["actor_id".into()],
            total_rows: 16_044,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn text_block_prints_full_root() {
        let out = ReceiptDisplay(&receipt()).to_string();

        println!("\n----- receipt -----\n{out}-------------------\n");
        assert!(out.starts_with("migrate_actor/actor\n"));
        assert!(out.contains(&format!("root:       {}", "ab".repeat(32))));
        assert!(out.contains("rows:       16,044"));
        assert!(out.contains("key:        actor_id"));
    }

    #[test]
    fn json_shape_uses_hex_root() {
        let r = receipt();
        let json = serde_json::to_value(ReceiptJson::from(&r)).expect("serialize");
        assert_eq!(json["pipeline"], "migrate_actor");
        assert_eq!(json["table"], "actor");
        assert_eq!(json["root"], "ab".repeat(32));
        assert_eq!(json["rows"], 16_044);
        assert_eq!(json["key_columns"][0], "actor_id");
    }
}
