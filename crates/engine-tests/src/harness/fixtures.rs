//! Test databases and how to reset them.

use connectors::error::DriverError;
use mysql_async::Pool;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::sync::{Arc, LazyLock};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tracing::{error, warn};

// Host ports of the test databases.
fn mysql_port() -> u16 {
    env_port("MYSQL_PORT", 13306)
}

fn pg_port() -> u16 {
    env_port("POSTGRES_PORT", 15432)
}

fn env_port(var: &str, default: u16) -> u16 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// Read-only source fixtures. Both hold the DVD-rental schema; see `matrix`.
pub(crate) static MYSQL_SOURCE_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "mysql://sakila_user:qwerty123@localhost:{}/sakila",
        mysql_port()
    )
});

pub(crate) static PG_SOURCE_URL: LazyLock<String> =
    LazyLock::new(|| format!("postgres://user:password@localhost:{}/pagila", pg_port()));

// Scratch destinations, emptied before each case.
pub(crate) static PG_DEST_URL: LazyLock<String> =
    LazyLock::new(|| format!("postgres://user:password@localhost:{}/testdb", pg_port()));

pub(crate) static MYSQL_DEST_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "mysql://user:password@localhost:{}/paganel_dest",
        mysql_port()
    )
});

pub(crate) async fn mysql_pool(source_db: &str) -> Pool {
    Pool::from_url(match source_db {
        "sakila" => MYSQL_SOURCE_URL.as_str(),
        "dest" => MYSQL_DEST_URL.as_str(),
        _ => panic!("Unknown mysql database: {source_db}"),
    })
    .expect("connect mysql")
}

pub(crate) async fn pg_pool() -> Arc<Client> {
    Arc::new(
        connect_client(PG_DEST_URL.as_str())
            .await
            .expect("connect postgres"),
    )
}

/// Client for the read-only Pagila source database.
pub(crate) async fn pg_pagila_pool() -> Arc<Client> {
    Arc::new(
        connect_client(PG_SOURCE_URL.as_str())
            .await
            .expect("connect postgres pagila (see .github/workflows/ci.yml for seeding)"),
    )
}

/// Empty the MySQL destination database and clear the state store.
pub(crate) async fn reset_mysql_dest() {
    use mysql_async::prelude::Queryable;

    let pool = mysql_pool("dest").await;
    let mut conn = pool.get_conn().await.expect("connect mysql destination");

    let tables: Vec<String> = conn
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
        )
        .await
        .expect("list destination tables");

    if !tables.is_empty() {
        let list = tables
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")
            .await
            .expect("disable fk checks");
        conn.query_drop(format!("DROP TABLE IF EXISTS {list}"))
            .await
            .expect("drop destination tables");
        conn.query_drop("SET FOREIGN_KEY_CHECKS = 1")
            .await
            .expect("enable fk checks");
    }

    // `LOAD DATA LOCAL INFILE` - the MySQL fast path - is silently unavailable
    // when the server has `local_infile` off, which pushes every write onto the
    // INSERT path and produces confusing downstream failures. Fail loudly instead.
    let local_infile: Option<i64> = conn
        .query_first("SELECT @@GLOBAL.local_infile")
        .await
        .expect("read local_infile");
    assert_eq!(
        local_infile,
        Some(1),
        "MySQL `local_infile` must be ON for the LOAD DATA fast path; \
         run: SET GLOBAL local_infile = 1;"
    );

    clear_state_store();
}

/// Remove the state store so a test starts from a clean checkpoint.
pub(crate) fn clear_state_store() {
    let home_dir = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok();

    if let Some(home) = home_dir {
        let state_path = std::path::PathBuf::from(home).join(".paganel/state");
        if state_path.exists() {
            let _ = std::fs::remove_dir_all(&state_path);
        }
    }
}

/// Drop & recreate the public schema in Postgres so it's empty.
/// Also clears the state store to ensure tests start with clean state.
pub(crate) async fn reset_postgres_schema() {
    let pool = pg_pool().await;

    // A heavy prior test can leave a Postgres session alive for a moment during
    // teardown. If it still holds a lock on a `public` object, the
    // `DROP SCHEMA ... CASCADE` below would block forever (it has no timeout),
    // which manifests as the *next* test hanging. Terminate any other backends
    // on this database first.
    let _ = pool
        .batch_execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid();",
        )
        .await;

    // Drop and recreate public schema (removes all tables, types, etc.).
    // `statement_timeout` ensures a stuck reset fails loudly instead of hanging
    // CI indefinitely.
    pool.batch_execute(
        r#"
        SET statement_timeout = '30s';
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
    "#,
    )
    .await
    .expect("reset postgres schema");

    // Clear the state store to prevent test pollution
    clear_state_store();
}

pub(crate) async fn connect_client(url: &str) -> Result<Client, DriverError> {
    let config = url
        .parse::<Config>()
        .map_err(|e| DriverError::InvalidUrl(e.to_string()))?;
    let ssl_mode = config.get_ssl_mode();

    match ssl_mode {
        SslMode::Disable => connect_without_tls(config).await,
        SslMode::Require => connect_with_tls(config).await,
        SslMode::Prefer => match connect_with_tls(config.clone()).await {
            Ok(client) => Ok(client),
            Err(error) => {
                warn!(%error, "Postgres TLS handshake failed, retrying without TLS");
                connect_without_tls(config).await
            }
        },
        _ => connect_with_tls(config).await,
    }
}

async fn connect_with_tls(config: Config) -> Result<Client, DriverError> {
    let connector = TlsConnector::builder()
        .build()
        .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}

async fn connect_without_tls(config: Config) -> Result<Client, DriverError> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}

/// Tables present, with identical names, in both fixtures.
///
/// Excludes Sakila's `film_text`. Row counts differ for `staff`, `store`,
/// `film_category` and `payment`, so only parity assertions are safe.
pub const COMMON_TABLES: &[&str] = &[
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
];
