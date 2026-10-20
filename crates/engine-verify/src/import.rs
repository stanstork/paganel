use crate::{
    HashSink, error::VerifyError, hash_table, progress::VerifyProgress, state::VerifyState,
    table::spec_from_table,
};
use chrono::Utc;
use engine_core::drivers::DriverRef;
use engine_state::{RowHashScope, fold_root};
use model::integrity::{
    import::{ImportRecord, import_pipeline, validate_import_name},
    receipt::VerificationReceipt,
};

/// What to import, and under which name.
#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub name: String,
    /// Connection block name from the config; stored, never the URL.
    pub connection: String,
    /// `target` of the connection's URL.
    pub target: Option<String>,
    pub table: String,
    /// Key columns for a table without a primary key, or to override it.
    pub key_override: Option<Vec<String>>,
}

/// Hash `req.table` and store it as the baseline named `req.name`, replacing
/// any earlier import of that name.
pub async fn import_table(
    driver: &DriverRef,
    req: &ImportRequest,
    state: &VerifyState,
    progress: &mut dyn VerifyProgress,
) -> Result<(VerificationReceipt, ImportRecord), VerifyError> {
    validate_import_name(&req.name).map_err(VerifyError::InitializationError)?;

    let pipeline = import_pipeline(&req.name);
    let (spec, meta) =
        spec_from_table(driver, &req.table, req.key_override.as_deref(), &[]).await?;

    // Without a key, a changed row reads as one missing and one extra row.
    if spec.key_columns.is_empty() {
        return Err(VerifyError::NoKey(req.table.clone()));
    }

    progress.table_started(&pipeline, &req.table);

    // A re-import replaces the baseline, possibly of another table.
    state.hash_log.clear_pipeline(&pipeline)?;

    progress.table_phase("reading table");
    let sink = HashSink {
        log: &state.hash_log,
        scope: RowHashScope::Baseline,
        pipeline: &pipeline,
        table: &req.table,
    };
    hash_table(driver, meta, &spec, &sink, None).await?;

    progress.table_phase("sorting row hashes");
    let hash_log = state.hash_log.clone();
    let pipeline_clone = pipeline.clone();
    let table = req.table.clone();
    let algorithm = spec.algorithm;

    let (rows, root) = tokio::task::spawn_blocking(move || {
        hash_log.seal(RowHashScope::Baseline, &pipeline_clone, &table)?;
        fold_root(
            hash_log.stream(RowHashScope::Baseline, &pipeline_clone, &table)?,
            algorithm,
        )
    })
    .await
    .map_err(|e| VerifyError::InitializationError(format!("import task failed: {e}")))??;

    let now = Utc::now();
    let receipt = VerificationReceipt {
        run_id: format!("import-{}", now.format("%Y%m%dT%H%M%SZ")),
        pipeline_name: pipeline,
        table_name: req.table.clone(),
        table_root: root,
        column_order: spec.column_order,
        key_columns: spec.key_columns.clone(),
        total_rows: rows,
        skipped_rows: 0,
        algorithm: spec.algorithm,
        created_at: now,
    };

    let record = ImportRecord {
        name: req.name.clone(),
        connection: req.connection.clone(),
        target: req.target.clone(),
        table: req.table.clone(),
        key_columns: spec.key_columns,
        imported_at: now,
        rows,
    };

    state.store.save_import(&record, &receipt).await?;

    Ok((receipt, record))
}

/// Where a connection URL points, without credentials: scheme, host, port and database.
pub fn connection_target(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;

    let scheme = match parsed.scheme() {
        "postgresql" => "postgres",
        other => other,
    };

    let port = parsed.port().map_or(String::new(), |p| format!(":{p}"));

    Some(format!("{scheme}://{host}{port}{}", parsed.path()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_drops_credentials_and_query() {
        let target = connection_target("postgres://u:secret@db.internal:5432/app?sslmode=require");
        assert_eq!(target.as_deref(), Some("postgres://db.internal:5432/app"));
        assert!(!target.unwrap_or_default().contains("secret"));
    }

    #[test]
    fn target_spells_postgres_one_way() {
        assert_eq!(
            connection_target("postgresql://h/app"),
            connection_target("postgres://h/app")
        );
        assert_eq!(
            connection_target("mysql://u:p@h:3306/shop").as_deref(),
            Some("mysql://h:3306/shop")
        );
    }

    #[test]
    fn unparseable_urls_have_no_target() {
        assert_eq!(connection_target("not a url"), None);
        assert_eq!(connection_target("file:///tmp/data.csv"), None);
    }
}
