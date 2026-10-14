use super::open_state_store;
use crate::{commands::apply, config, error::CliError};
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::EnvContext;
use engine_state::models::RunStatus;
use model::execution::flags::IntegrityMode;
use std::sync::Arc;

/// Resumes a previously paused migration.
/// Validates that a paused run exists before delegating to apply.
pub async fn execute(
    config_path: Option<String>,
    tui: bool,
    pretty: bool,
    integrity: IntegrityMode,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;
    let plan = config::load_plan(&config_path, false, env.clone()).await?;
    let run_id = plan.run_id();

    // Verify a paused run exists for this config
    match open_state_store() {
        Ok(state) => match state.load_run_state(&run_id).await {
            Ok(Some(run)) => match &run.status {
                RunStatus::Paused { .. } => {
                    println!("Resuming paused migration (run_id: {run_id})");
                }
                RunStatus::Completed { .. } => {
                    return Err(CliError::UserMessage(format!(
                        "Migration '{config_path}' has already completed (run_id: {run_id})"
                    )));
                }
                RunStatus::Failed { error, .. } => {
                    return Err(CliError::UserMessage(format!(
                        "Migration '{config_path}' is in failed state: {error}\nUse 'pag apply' to retry from last checkpoint."
                    )));
                }
                RunStatus::Running => {
                    return Err(CliError::UserMessage(format!(
                        "Migration '{config_path}' appears to still be running (run_id: {run_id})"
                    )));
                }
            },
            Ok(None) => {
                return Err(CliError::UserMessage(format!(
                    "No previous run found for '{config_path}' (run_id: {run_id})\nUse 'pag apply' to start a new migration."
                )));
            }
            Err(_) => {
                // State store error - fall through to apply
            }
        },
        Err(_) => {
            return Err(CliError::UserMessage(format!(
                "No previous run found for '{config_path}'\nUse 'pag apply' to start a new migration."
            )));
        }
    }

    // Delegate to apply - it handles checkpoint-based resume
    apply::execute(
        Some(config_path),
        tui,
        pretty,
        false,
        integrity,
        shutdown,
        env,
    )
    .await
}
