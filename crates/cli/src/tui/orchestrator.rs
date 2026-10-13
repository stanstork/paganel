use crate::{
    commands::{open_state_store, run_completed},
    error::CliError,
    tui::{
        app::{
            command::MigrationCommand,
            core::App,
            handlers::events::{TerminalEvent, spawn_terminal_events},
        },
        pipeline::{PipelineState, PipelineStatus},
        plan::build_plan_context,
        tasks::{spawn_command_handler, spawn_event_forwarder, spawn_executor},
        terminal::TerminalGuard,
    },
};
use engine_core::context::env::EnvContext;
use engine_core::plan::execution::ExecutionPlan as CoreExecutionPlan;
use engine_infra::event_bus::bus::EventBus;
use engine_infra::shutdown::ShutdownSignal;
use indicatif::{ProgressBar, ProgressStyle};
use model::{
    events::migration::MigrationEvent,
    execution::flags::{ExecutionFlags, IntegrityMode},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

/// Orchestrates the TUI lifecycle and background engine tasks
pub async fn run_tui(
    config_path: String,
    exact_filter: bool,
    integrity: IntegrityMode,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    // Build Plan (Outside TUI mode so errors/logs show in standard terminal)
    let mut plan_context = build_plan(&config_path, exact_filter, env.clone()).await?;

    // If this migration already finished, don't take over the screen with a TUI
    if run_completed(&plan_context.core_plan.run_id()).await {
        println!("Migration for '{config_path}' already completed.");
        return Ok(());
    }

    // Seed already-migrated rows from the checkpoint
    seed_resume_progress(&mut plan_context.pipelines, &plan_context.core_plan).await;

    // Initialize Terminal Guard (Restores terminal on drop)
    let mut guard = TerminalGuard::init()?;

    // Setup Communication Channels
    let channels = setup_channels();

    // Start background tasks
    let integrity_enabled = integrity.is_enabled();
    let flags = ExecutionFlags::new(false, integrity);
    let event_bus = EventBus::new();
    let (outcome_tx, outcome_rx) = mpsc::channel(4);

    spawn_event_forwarder(event_bus.clone(), channels.event_tx);
    spawn_command_handler(channels.command_rx, shutdown.clone());
    spawn_executor(
        flags,
        event_bus,
        plan_context.core_plan,
        plan_context.dag,
        shutdown,
        env,
        outcome_tx,
    );

    // Run Application
    let mut app = App::new(
        channels.event_rx,
        channels.command_tx,
        channels.terminal_rx,
        plan_context.pipelines,
        plan_context.report,
        outcome_rx,
        integrity_enabled,
    );

    app.run(guard.terminal())
        .await
        .map_err(|e| CliError::Unknown(e.to_string()))?;

    Ok(())
}

/// Seed each pipeline's already-migrated row count from its checkpoint, so a
/// resumed run displays progress from where the previous run stopped.
async fn seed_resume_progress(
    pipelines: &mut HashMap<String, PipelineState>,
    plan: &CoreExecutionPlan,
) {
    let Ok(store) = open_state_store() else {
        return;
    };

    let run_id = plan.run_id();

    for (item_id, state) in pipelines.iter_mut() {
        let done = store.total_rows_done(&run_id, item_id).await.unwrap_or(0);

        if done == 0 {
            continue;
        }

        state.resume_baseline = done;
        state.processed_rows = done;

        if state.source_rows > 0 && done >= state.source_rows {
            state.status = PipelineStatus::Completed;
            state.completed_at = Some(Instant::now());
        }
    }
}

/// Builds execution plan with animated spinner feedback
async fn build_plan(
    config_path: &str,
    exact_filter: bool,
    env: Arc<EnvContext>,
) -> Result<crate::tui::plan::PlanContext, CliError> {
    let msg = if exact_filter {
        format!(
            "Building execution plan from {config_path} (using exact COUNT - this may take longer)"
        )
    } else {
        format!("Building execution plan from {config_path}")
    };

    let spinner = ProgressBar::new_spinner()
        .with_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        )
        .with_message(msg);

    spinner.enable_steady_tick(std::time::Duration::from_millis(80));

    let plan_context = build_plan_context(config_path, exact_filter, env).await?;

    // Clear the spinner before TUI takes over
    spinner.finish_and_clear();

    Ok(plan_context)
}

/// Communication channels between TUI components
struct Channels {
    event_tx: mpsc::Sender<MigrationEvent>,
    event_rx: mpsc::Receiver<MigrationEvent>,
    command_tx: mpsc::Sender<MigrationCommand>,
    command_rx: mpsc::Receiver<MigrationCommand>,
    terminal_rx: mpsc::Receiver<TerminalEvent>,
}

/// Sets up all communication channels
fn setup_channels() -> Channels {
    let (event_tx, event_rx) = mpsc::channel(1000);
    let (command_tx, command_rx) = mpsc::channel(100);
    let terminal_rx = spawn_terminal_events();

    Channels {
        event_tx,
        event_rx,
        command_tx,
        command_rx,
        terminal_rx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channels_setup() {
        // Verify channels can be created
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let _channels = setup_channels();
            // If we got here, channels were created successfully
        });
    }
}
