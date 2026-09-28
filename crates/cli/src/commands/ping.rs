use crate::{Cli, error::CliError};
use engine_planner::{
    connection::{ConnectionTester, MySqlConnectionTester, PostgresConnectionTester},
    plan::connection::utils::mask_url,
};
use std::str::FromStr;
use tracing::info;

/// What kind of connection to check
#[derive(Debug)]
pub enum ConnectionKind {
    MySql,
    Postgres,
    Ftp,
}

impl FromStr for ConnectionKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(ConnectionKind::MySql),
            "pg" | "postgres" | "postgresql" => Ok(ConnectionKind::Postgres),
            "ftp" => Ok(ConnectionKind::Ftp),
            other => Err(format!("Unknown connection kind: {other}")),
        }
    }
}

impl ConnectionKind {
    /// Detect connection kind from URL scheme
    pub fn from_url(url: &str) -> Result<Self, String> {
        if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Ok(ConnectionKind::MySql)
        } else if url.starts_with("postgresql://")
            || url.starts_with("postgres://")
            || url.starts_with("pg://")
        {
            Ok(ConnectionKind::Postgres)
        } else if url.starts_with("ftp://") || url.starts_with("ftps://") {
            Ok(ConnectionKind::Ftp)
        } else {
            Err("Cannot detect connection type from URL. Supported schemes: mysql://, postgresql://, postgres://, ftp://".to_string())
        }
    }
}

/// Executes the ping command (test database connectivity)
pub async fn execute(cli: &Cli, url: String, format: Option<String>) -> Result<(), CliError> {
    // Determine connection kind from format or URL
    let kind = if let Some(format_str) = format {
        ConnectionKind::from_str(&format_str).map_err(CliError::InvalidConnectionFormat)?
    } else {
        ConnectionKind::from_url(&url).map_err(CliError::InvalidConnectionFormat)?
    };

    info!(kind = ?kind, "testing connection");

    // Test the connection based on the kind
    match kind {
        ConnectionKind::MySql => {
            test_mysql_connection(cli, url).await?;
        }
        ConnectionKind::Postgres => {
            test_postgres_connection(cli, url).await?;
        }
        _ => return Err(CliError::UnsupportedConnectionKind),
    }

    Ok(())
}

/// Tests a MySQL connection
async fn test_mysql_connection(cli: &Cli, url: String) -> Result<(), CliError> {
    let result = MySqlConnectionTester {
        name: mask_url(&url),
        conn_str: url.clone(),
    }
    .test()
    .await?;

    if !cli.quiet {
        println!("✓ MySQL connection successful");
        println!("  Version: {}", result.version);
    }

    Ok(())
}

/// Tests a PostgreSQL connection
async fn test_postgres_connection(cli: &Cli, url: String) -> Result<(), CliError> {
    let result = PostgresConnectionTester {
        name: mask_url(&url),
        conn_str: url.clone(),
    }
    .test()
    .await?;

    if !cli.quiet {
        println!("✓ PostgreSQL connection successful");
        println!("  Version: {}", result.version);
    }

    Ok(())
}
