use crate::{
    error::VerifyError, progress::VerifyProgress, state::VerifyState, table::spec_from_table,
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

    progress.table_phase("sorting row hashes");
    let hash_log = state.hash_log.clone();
    let pipeline_clone = pipeline.clone();
    let table = req.table.clone();
    let algorithm = spec.algorithm;

    let (rows, root) = tokio::task::spawn_blocking(move || {
        hash_log.seal(RowHashScope::Apply, &pipeline_clone, &table)?;
        fold_root(
            hash_log.stream(RowHashScope::Apply, &pipeline_clone, &table)?,
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
        table: req.table.clone(),
        key_columns: spec.key_columns,
        imported_at: now,
        rows,
    };

    state.store.save_import(&record, &receipt).await?;

    Ok((receipt, record))
}
