use super::{open_state_store, state_dir};
use crate::{config, error::CliError};
use engine_processing::EnvContext;
use engine_state::{
    RowHashLog,
    models::{PipelineStatus, RunStatus},
};
use std::{
    io::{self, Write},
    sync::Arc,
};
use tracing::warn;

/// Clears all state for a migration - checkpoints, WAL, run state, PID file.
pub async fn execute(
    config_path: Option<String>,
    force: bool,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;
    let plan = config::load_plan(&config_path, false, env).await?;
    let run_id = plan.run_id();

    let state = open_state_store()?;

    let Some(run) = state
        .load_run_state(&run_id)
        .await
        .map_err(|e| CliError::Unknown(format!("Failed to load run state: {e}")))?
    else {
        return Err(CliError::UserMessage(format!(
            "No state found for '{config_path}' (run_id: {run_id})"
        )));
    };

    // Show what will be deleted
    print_run_summary(&run);

    // Prompt for confirmation unless --force is specified
    if !force && !prompt_confirmation()? {
        println!("Aborted.");
        return Ok(());
    }

    // Delete state
    state
        .delete_run(&run_id)
        .await
        .map_err(|e| CliError::Unknown(format!("Failed to delete run state: {}", e)))?;

    // Clear integrity artifacts too.
    if let Ok(dir) = state_dir() {
        let hash_log = RowHashLog::in_state_dir(&dir);
        for pipeline in &plan.pipelines {
            if let Err(e) = hash_log.clear_pipeline(&pipeline.name) {
                warn!(pipeline = %pipeline.name, error = %e, "failed to clear row-hash log");
            }
            if let Err(e) = state.delete_receipts(&pipeline.name).await {
                warn!(pipeline = %pipeline.name, error = %e, "failed to delete integrity receipts");
            }
        }

        // Remove pause sentinel file if present
        let _ = std::fs::remove_file(dir.join(format!("{run_id}.pause")));
    }

    println!("State cleared for '{config_path}' (run_id: {run_id})");
    Ok(())
}

/// Prompts the user to confirm the deletion.
fn prompt_confirmation() -> Result<bool, CliError> {
    println!("\nThis will permanently delete all state for this migration.");
    println!("The next 'apply' will start from scratch.");
    print!("Continue? [y/N] ");

    io::stdout()
        .flush()
        .map_err(|e| CliError::Unknown(format!("Failed to flush stdout: {e}")))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| CliError::Unknown(format!("Failed to read input: {e}")))?;

    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}

fn print_run_summary(run: &engine_state::models::RunState) {
    let status = match &run.status {
        RunStatus::Running => "Running".to_string(),
        RunStatus::Paused { .. } => "Paused".to_string(),
        RunStatus::Completed { .. } => "Completed".to_string(),
        RunStatus::Failed { error, .. } => format!("Failed: {error}"),
    };

    let total_rows: u64 = run.pipelines.iter().map(|p| p.rows_done).sum();
    let completed = run
        .pipelines
        .iter()
        .filter(|p| matches!(p.status, PipelineStatus::Completed))
        .count();

    println!("Run:        {}", run.run_id);
    println!("Config:     {}", run.config_path);
    println!("Status:     {status}");
    println!("Pipelines:  {completed}/{} completed", run.total_pipelines);
    println!("Rows:       {total_rows}");
}
