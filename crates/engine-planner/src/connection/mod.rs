use crate::{
    builder::errors::ConnectionError,
    plan::connection::{plan::DatabaseDriver, utils::mask_url},
};
use async_trait::async_trait;
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    traits::driver::Driver,
};
use std::{fmt::Display, future::Future, time::Duration};
use tracing::{error, info};

/// How long a single connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Result of a connection test
pub struct ConnectionTestResult {
    pub version: String,
}

#[async_trait]
pub trait ConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError>;
}

/// MySQL/MariaDB connection tester
pub struct MySqlConnectionTester {
    pub name: String,
    pub conn_str: String,
}

/// Postgres connection tester
pub struct PostgresConnectionTester {
    pub name: String,
    pub conn_str: String,
}

async fn connect<T, E: Display>(
    name: &str,
    url: &str,
    engine: &str,
    limit: Duration,
    connect: impl Future<Output = Result<T, E>>,
) -> Result<T, ConnectionError> {
    match tokio::time::timeout(limit, connect).await {
        Ok(Ok(driver)) => Ok(driver),
        Ok(Err(e)) => {
            error!(url = %mask_url(url), engine, error = %e, "connection failed");
            Err(ConnectionError::Failed {
                name: name.to_string(),
                reason: format!("Connection failed: {e}"),
            })
        }
        Err(_) => {
            let timeout_ms = limit.as_millis() as u64;
            error!(url = %mask_url(url), engine, timeout_ms, "connection timed out");
            Err(ConnectionError::Timeout {
                name: name.to_string(),
                timeout_ms,
            })
        }
    }
}

#[async_trait]
impl ConnectionTester for MySqlConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!(url = %mask_url(&self.conn_str), "pinging MySQL");

        let driver = connect(
            &self.name,
            &self.conn_str,
            "mysql",
            CONNECT_TIMEOUT,
            MySqlDriver::connect(&self.conn_str),
        )
        .await?;

        let version = driver.capabilities().version.clone();
        info!(url = %mask_url(&self.conn_str), version = %version, "MySQL ping succeeded");

        Ok(ConnectionTestResult { version })
    }
}

#[async_trait]
impl ConnectionTester for PostgresConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!(url = %mask_url(&self.conn_str), "pinging Postgres");

        let driver = connect(
            &self.name,
            &self.conn_str,
            "postgres",
            CONNECT_TIMEOUT,
            PgDriver::connect(&self.conn_str),
        )
        .await?;

        let version = driver.capabilities().version.clone();
        info!(url = %mask_url(&self.conn_str), version = %version, "Postgres ping succeeded");

        Ok(ConnectionTestResult { version })
    }
}

pub async fn test_connection(
    name: &str,
    url: &str,
    driver: &DatabaseDriver,
) -> Result<ConnectionTestResult, ConnectionError> {
    match driver {
        DatabaseDriver::MySql => {
            MySqlConnectionTester {
                name: name.to_string(),
                conn_str: url.to_string(),
            }
            .test()
            .await
        }
        DatabaseDriver::Postgres => {
            PostgresConnectionTester {
                name: name.to_string(),
                conn_str: url.to_string(),
            }
            .test()
            .await
        }
        _ => Err(ConnectionError::Failed {
            name: name.to_string(),
            reason: format!("Unsupported database driver: {:?}", driver),
        }),
    }
}
