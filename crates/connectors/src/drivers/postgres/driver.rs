use super::{config::CopyFormat, queries::escape_identifier, tls};
use crate::{
    error::DriverError,
    session::SessionSettings,
    sql::metadata::capabilities::Capabilities,
    traits::driver::{Driver, DriverInfo},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::Client;
use tracing::{info, warn};

const PG_MAX_PREPARED_STMT_PARAMS: usize = 65535;

/// Default schema when a connection doesn't specify one.
pub const DEFAULT_SCHEMA: &str = "public";

#[derive(Clone)]
pub struct PgDriver {
    client: Arc<RwLock<Client>>,
    url: String,
    schema: String,
    capabilities: Capabilities,
    copy_format: CopyFormat,
}

impl PgDriver {
    /// Static driver info for registration
    pub const INFO: DriverInfo = DriverInfo {
        id: "postgres",
        name: "PostgreSQL",
        schemes: &["postgres", "postgresql"],
    };

    /// Establishes a connection (schema `public`) and detects server capabilities.
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        Self::connect_with_schema(url, DEFAULT_SCHEMA).await
    }

    /// A driver on its own connection with `settings` applied.
    pub async fn connect_session(
        url: &str,
        schema: &str,
        settings: &SessionSettings,
    ) -> Result<Self, DriverError> {
        let driver = Self::connect_with_schema(url, schema).await?;
        {
            let client = driver.client.read().await;
            for statement in settings.postgres_statements() {
                client
                    .batch_execute(&statement)
                    .await
                    .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
            }
        }
        Ok(driver)
    }

    /// Establishes a connection scoped to `schema`. The session `search_path` is
    /// set so that unqualified reads, writes, and DDL target that schema, and
    /// the schema is used to scope introspection queries.
    pub async fn connect_with_schema(url: &str, schema: &str) -> Result<Self, DriverError> {
        let client = tls::connect(url).await?;
        set_search_path(&client, schema).await?;

        let client = Arc::new(RwLock::new(client));
        let capabilities = Self::detect_capabilities(&client).await?;

        info!(
            driver = "postgres",
            schema, "database connection established"
        );

        Ok(Self {
            client,
            url: url.to_string(),
            schema: schema.to_string(),
            capabilities,
            copy_format: CopyFormat::default(),
        })
    }

    /// Override the COPY format.
    pub fn with_copy_format(mut self, copy_format: CopyFormat) -> Self {
        self.copy_format = copy_format;
        self
    }

    /// The COPY format this driver writes with.
    pub fn copy_format(&self) -> CopyFormat {
        self.copy_format
    }

    pub fn client(&self) -> &Arc<RwLock<Client>> {
        &self.client
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Fetches the version string from the DB and resolves capabilities.
    async fn detect_capabilities(
        client: &Arc<RwLock<Client>>,
    ) -> Result<Capabilities, DriverError> {
        let client = client.read().await;

        let row = client
            .query_one("SELECT version()", &[])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let version: String = row.get(0);

        let has_postgis: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')",
                &[],
            )
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .get(0);

        Ok(Self::resolve_capabilities(version, has_postgis))
    }

    /// Resolves server capabilities from the version string and PostGIS probe.
    fn resolve_capabilities(version: String, has_postgis: bool) -> Capabilities {
        // If the version string can't be parsed (should not happen for real
        // servers), assume a modern release rather than disabling features.
        let ver = Self::parse_pg_version(&version).unwrap_or_else(|| {
            warn!(version = %version, "could not parse PostgreSQL version; assuming latest");
            (u32::MAX, 0)
        });
        let at_least = |major: u32, minor: u32| ver >= (major, minor);

        Capabilities {
            version,
            // Present in every supported PostgreSQL release.
            transactions: true,
            savepoints: true,
            copy_protocol: true,
            array_type: true,
            // Gated at the release that introduced each feature.
            returning_clause: at_least(8, 2),
            uuid_type: at_least(8, 3),
            json_type: at_least(9, 2),
            jsonb_type: at_least(9, 4),
            upsert: at_least(9, 5), // INSERT ... ON CONFLICT
            geometry_type: has_postgis,
            max_parameters: Some(PG_MAX_PREPARED_STMT_PARAMS),
            max_query_size: None,
        }
    }

    fn parse_pg_version(version: &str) -> Option<(u32, u32)> {
        // The version number is the second whitespace-separated token.
        let token = version.split_whitespace().nth(1)?;
        let mut parts = token.split('.');
        let major = leading_u32(parts.next()?)?;
        let minor = parts.next().and_then(leading_u32).unwrap_or(0);
        Some((major, minor))
    }
}

/// Parses the leading run of ASCII digits (handles suffixes like `17beta1`).
fn leading_u32(s: &str) -> Option<u32> {
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Set the session `search_path` so unqualified names resolve to `schema`.
pub(crate) async fn set_search_path(client: &Client, schema: &str) -> Result<(), DriverError> {
    if schema == DEFAULT_SCHEMA {
        return Ok(());
    }

    let sql = format!(
        "SET search_path TO {}, {}",
        escape_identifier(schema),
        escape_identifier(DEFAULT_SCHEMA)
    );

    client
        .batch_execute(&sql)
        .await
        .map_err(|e| DriverError::QueryError(e.to_string()))
}

impl Driver for PgDriver {
    fn info(&self) -> &DriverInfo {
        &Self::INFO
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
}
