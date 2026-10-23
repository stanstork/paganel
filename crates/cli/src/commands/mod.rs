use crate::{Cli, error::CliError};
use clap::{Args, Subcommand, ValueEnum};
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::EnvContext;
use engine_state::{StateStore, models::RunStatus};
use model::execution::flags::IntegrityMode;
use std::{path::PathBuf, sync::Arc, time::Duration};

pub mod apply;
pub mod drift;
pub mod pause;
pub mod ping;
pub mod plan;
pub mod plugin;
pub mod receipt;
pub mod reset;
pub mod resume;
pub mod state;
pub mod status;
pub mod verify;
pub mod version;

/// Returns the path to the state directory (~/.paganel/state/).
pub fn state_dir() -> Result<PathBuf, CliError> {
    engine_state::default_state_dir().map_err(|e| CliError::Unknown(e.to_string()))
}

/// Whether the saved run state for `run_id` is already `Completed`.
pub async fn run_completed(run_id: &str) -> bool {
    let Ok(store) = open_state_store() else {
        return false;
    };
    matches!(
        store.load_run_state(run_id).await.ok().flatten(),
        Some(run) if matches!(run.status, RunStatus::Completed { .. })
    )
}

/// Opens the state store from the default location.
pub fn open_state_store() -> Result<StateStore, CliError> {
    let path = state_dir()?;
    match try_open_state_store()? {
        Some(store) => Ok(store),
        None => Err(CliError::UserMessage(format!(
            "The state store at {} stayed locked by another Paganel process for more than 5 seconds.\nTry again in a moment.",
            path.display()
        ))),
    }
}

/// Open the state store, or `None` when another process keeps it locked past the busy timeout.
fn try_open_state_store() -> Result<Option<StateStore>, CliError> {
    let path = state_dir()?;
    StateStore::try_open(&path).map_err(|e| {
        CliError::Unknown(format!(
            "Failed to open state store at {}: {e}",
            path.display()
        ))
    })
}

/// Sampling method for data preview
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum SampleMethod {
    /// Take first N rows (fastest, deterministic)
    #[default]
    First,
    /// Take random N rows (varied sample)
    Random,
    /// By specific IDs
    Id,
}

#[derive(Subcommand)]
pub enum PluginCmd {
    /// Compile a JavaScript plugin to WASM.
    Compile {
        input: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
        #[arg(long)]
        minify: bool,
        #[arg(long)]
        esbuild_path: Option<PathBuf>,
        #[arg(long)]
        runtime_wasm: Option<PathBuf>,
    },
    /// Print a plugin's metadata (name, version, role, schema).
    Inspect {
        /// Path to a .wasm plugin.
        path: PathBuf,
        /// Emit machine-readable JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Validate every plugin referenced in an PPL config.
    Validate {
        #[arg(short = 'c', long)]
        config: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Run a plugin once with sample input.
    Test {
        /// Path to a .wasm plugin.
        path: PathBuf,
        /// transform | filter | source | sink (defaults to the plugin's role).
        #[arg(long)]
        mode: Option<String>,
        /// Input JSON file (a row for transform/filter, a batch for sink). "-" = stdin.
        #[arg(long)]
        input: Option<String>,
        /// Cursor for source mode (opaque string).
        #[arg(long)]
        cursor: Option<String>,
        /// Plugin config JSON file passed at initialize.
        #[arg(long)]
        config_json: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum StateCmd {
    /// Hash a table Paganel did not migrate and store it as a baseline for drift.
    Import {
        #[arg(short = 'c', long)]
        config: Option<String>,
        /// Connection block name in the config; optional when the config has exactly one.
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        table: String,
        /// Defaults to the table name when it is a valid import name.
        #[arg(long)]
        name: Option<String>,
        /// Key columns when the table has no primary key (comma-separated).
        #[arg(long, value_delimiter = ',')]
        key: Vec<String>,
    },
    /// List imported tables.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Remove an import: its record, receipt and row hashes.
    Rm { name: String },
}

/// Arguments for `pag drift`.
#[derive(Args)]
pub struct DriftArgs {
    #[arg(
        short = 'c',
        long,
        help = "Path to PPL config file (auto-discovered if not specified)"
    )]
    pub config: Option<String>,

    /// Print each run as JSON; with --watch, one line per run
    #[arg(long)]
    pub json: bool,

    /// Check only this pipeline or import (repeatable)
    #[arg(long, value_name = "NAME")]
    pub filters: Vec<String>,

    #[arg(
        long,
        help = "Accept destination rows the receipt does not cover instead of failing on them"
    )]
    pub allow_extra: bool,

    /// Repeat until interrupted
    #[arg(long)]
    pub watch: bool,

    /// Time between runs with --watch: a number and s, m, h or d, e.g. 15m or 24h
    #[arg(
        long,
        default_value = "24h",
        value_parser = drift::parse_interval,
        requires = "watch"
    )]
    pub interval: Duration,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run dry-run migration and show results without making changes
    Plan {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(
            long,
            short = 'o',
            help = "If specified, writes the report to this file instead of stdout"
        )]
        output: Option<String>,

        /// Emit the full machine-readable JSON report instead of the human summary
        #[arg(long)]
        json: bool,

        /// Print the exact CREATE / ALTER DDL the migration would run
        #[arg(long)]
        ddl: bool,

        /// Enable sample data collection in the plan output
        #[arg(long, short = 's')]
        sample: bool,

        /// Number of rows to sample per pipeline (default: 5)
        #[arg(long, default_value = "5")]
        sample_size: usize,

        /// Sampling method: first (default) or random
        #[arg(long, value_enum, default_value = "first")]
        sample_method: SampleMethod,

        /// Name of the ID column for sampling
        #[arg(long, default_value = "id")]
        id_column: Option<String>,

        /// Specific IDs to sample (comma-separated)
        #[arg(long, value_delimiter = ',')]
        sample_ids: Option<Vec<String>>,

        /// Use exact COUNT for filtered rows (slower but accurate). By default uses EXPLAIN estimates (faster)
        #[arg(long)]
        exact_filter: bool,
    },
    /// Execute the migration
    Apply {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(long, help = "Run with interactive TUI (experimental)")]
        tui: bool,

        #[arg(long, help = "Run with pretty colored output")]
        pretty: bool,

        /// Use exact COUNT for filtered rows (slower but accurate). By default uses EXPLAIN estimates (faster)
        #[arg(long)]
        exact_filter: bool,

        #[arg(long, help = "Compute integrity hashes and receipts during migration")]
        integrity: bool,
    },
    /// Check the destination against the integrity receipts from `apply --integrity` (no source connection)
    #[command(after_help = "Exit codes: 0 match, 1 error, 3 mismatch, 4 inconclusive")]
    Verify {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(
            long,
            short = 'o',
            help = "If specified, writes the verification report to this file instead of stdout"
        )]
        output: Option<String>,

        #[arg(long, help = "Run with pretty colored output")]
        pretty: bool,

        #[arg(long, conflicts_with = "pretty")]
        json: bool,

        #[arg(
            long,
            help = "Accept destination rows the receipt does not cover (data that was already there, or another run's rows) instead of failing on them"
        )]
        allow_extra: bool,
    },
    /// Test database connectivity
    Ping {
        #[arg(
            long,
            help = "Connection URL (e.g., mysql://user:pass@host:3306/db or postgresql://user:pass@host:5432/db)"
        )]
        url: String,

        #[arg(
            long,
            help = "Database format (mysql, postgres). Auto-detected from URL if not specified"
        )]
        format: Option<String>,
    },
    /// Show migration run status
    Status {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file. If provided, shows status for that migration only"
        )]
        config: Option<String>,
    },
    /// Resume a previously paused migration
    Resume {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(long, help = "Run with interactive TUI (experimental)")]
        tui: bool,

        #[arg(long, help = "Run with pretty colored output")]
        pretty: bool,

        #[arg(long, help = "Compute integrity hashes and receipts during migration")]
        integrity: bool,
    },
    /// Clear all state for a migration (checkpoints, WAL, run state)
    Reset {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file (auto-discovered if not specified)"
        )]
        config: Option<String>,

        #[arg(long, help = "Skip confirmation prompt")]
        force: bool,
    },
    /// Print stored integrity receipts (full table roots) from `apply --integrity`
    Receipt {
        #[arg(
            short = 'c',
            long,
            help = "Path to PPL config file. If provided, shows receipts for that migration's pipelines only"
        )]
        config: Option<String>,

        /// Emit the receipts as JSON instead of text
        #[arg(long)]
        json: bool,
    },
    /// Send pause signal to a running migration
    Pause {
        #[arg(short = 'c', long, help = "Path to PPL config file")]
        config: String,
    },
    /// Show version information
    Version,
    /// Plugin tooling: compile, inspect, validate, and test WASM/JS plugins.
    Plugin {
        #[command(subcommand)]
        cmd: PluginCmd,
    },
    /// Drift baselines for tables Paganel did not migrate: import, list, remove.
    State {
        #[command(subcommand)]
        cmd: StateCmd,
    },
    /// Re-check tables against their receipts and imports, once or on an interval
    #[command(after_help = "Exit codes: 0 match, 1 error, 3 mismatch, 4 inconclusive")]
    Drift(DriftArgs),
}

/// Executes the appropriate command based on CLI arguments
pub async fn execute_command(
    cli: &Cli,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    match &cli.command {
        Commands::Plan { .. } => plan::execute(cli, &cli.command, env).await,
        Commands::Apply {
            config,
            tui,
            pretty,
            exact_filter,
            integrity,
        } => {
            let integrity_mode = IntegrityMode::new(*integrity);
            apply::execute(
                config.clone(),
                *tui,
                *pretty,
                *exact_filter,
                integrity_mode,
                shutdown,
                env,
            )
            .await
        }
        Commands::Verify {
            config,
            output,
            pretty,
            json,
            allow_extra,
        } => {
            verify::execute(
                cli,
                config.clone(),
                output.clone(),
                *pretty,
                *json,
                *allow_extra,
                env.clone(),
            )
            .await
        }
        Commands::Status { config } => status::execute(config.clone(), env).await,
        Commands::Ping { url, format } => ping::execute(cli, url.clone(), format.clone()).await,
        Commands::Version => {
            version::execute();
            Ok(())
        }
        Commands::Resume {
            config,
            tui,
            pretty,
            integrity,
        } => {
            let integrity_mode = IntegrityMode::new(*integrity);
            resume::execute(config.clone(), *tui, *pretty, integrity_mode, shutdown, env).await
        }
        Commands::Reset { config, force } => reset::execute(config.clone(), *force, env).await,
        Commands::Pause { config } => pause::execute(Some(config.clone()), env).await,
        Commands::Receipt { config, json } => receipt::execute(config.clone(), *json, env).await,
        Commands::Plugin { cmd } => plugin::run(cmd, env.clone()).await,
        Commands::State { cmd } => state::run(cmd, env.clone()).await,
        Commands::Drift(drift_args) => drift::execute(drift_args, env).await,
    }
}
