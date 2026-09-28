use crate::{commands::Commands, version::build_version_string};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "pag",
    version = env!("CARGO_PKG_VERSION"),
    about = "Data migration tool",
    long_version = build_version_string(),
    after_help = "ENVIRONMENT VARIABLES:
  PAGANEL_CONFIG      Path to config file (overrides auto-discovery)
  PAGANEL_LOG_LEVEL   Log level: error, warn, info, debug, trace"
)]
pub struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,

    /// Load environment variables from file
    #[arg(short = 'e', long, global = true)]
    pub(crate) env_file: Option<String>,

    /// Increase verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub(crate) verbose: u8,

    /// Suppress non-essential output
    #[arg(short, long, global = true)]
    pub(crate) quiet: bool,

    /// Disable colored output
    #[arg(long, global = true)]
    pub(crate) no_color: bool,

    /// Set log level (error, warn, info, debug, trace)
    #[arg(long, value_name = "LEVEL", global = true)]
    pub(crate) log_level: Option<String>,

    /// Write logs to file
    #[arg(long, value_name = "FILE", global = true)]
    pub(crate) log_file: Option<String>,
}

impl Cli {
    /// Returns true if running in TUI mode
    pub fn is_tui_mode(&self) -> bool {
        matches!(
            self.command,
            Commands::Apply { tui: true, .. } | Commands::Resume { tui: true, .. }
        )
    }

    /// Returns true if running in pretty output mode
    pub fn is_pretty_mode(&self) -> bool {
        matches!(
            self.command,
            Commands::Apply { pretty: true, .. }
                | Commands::Resume { pretty: true, .. }
                | Commands::Verify { pretty: true, .. }
        )
    }

    pub fn is_plan_mode(&self) -> bool {
        matches!(
            self.command,
            Commands::Plan { .. } | Commands::Receipt { .. }
        )
    }

    pub fn can_pause(&self) -> bool {
        matches!(
            self.command,
            Commands::Apply { .. } | Commands::Resume { .. }
        )
    }
}
