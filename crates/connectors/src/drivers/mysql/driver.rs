use crate::{
    drivers::mysql::tls,
    error::DriverError,
    session::SessionSettings,
    sql::metadata::capabilities::Capabilities,
    traits::driver::{Driver, DriverInfo},
};
use mysql_async::{Conn, Pool, prelude::Queryable};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{info, warn};

const MYSQL_MAX_PREPARED_STMT_PARAMS: u16 = 65535;

/// Below this, a bulk load stalls repeatedly on InnoDB checkpoint flushing.
const MIN_HEALTHY_REDO_BYTES: i64 = 1024 * 1024 * 1024; // 1 GiB

#[derive(Clone)]
pub struct MySqlDriver {
    pool: Pool,
    capabilities: Capabilities,
    advised: Arc<AtomicBool>,
}

impl MySqlDriver {
    /// Static driver info for registration
    pub const INFO: DriverInfo = DriverInfo {
        id: "mysql",
        name: "MySQL",
        schemes: &["mysql", "mariadb"],
    };

    /// Establishes a connection pool and detects server capabilities.
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        let pool = tls::pool_from_url(url)?;
        let capabilities = Self::detect_capabilities(&pool).await?;

        info!(driver = "mysql", "database connection established");

        Ok(Self {
            pool,
            capabilities,
            advised: Arc::new(AtomicBool::new(false)),
        })
    }

    /// A driver on one connection, with `settings` applied to it: for a
    /// comparison whose queries must share a session.
    pub async fn connect_session(
        url: &str,
        settings: &SessionSettings,
    ) -> Result<Self, DriverError> {
        let pool = tls::pinned_pool_from_url(url, settings.mysql_statements())?;
        let capabilities = Self::detect_capabilities(&pool).await?;

        info!(
            driver = "mysql",
            "database session established with settings: {:?}", settings
        );

        Ok(Self {
            pool,
            capabilities,
            advised: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// One-time, read-only advisory run before the first bulk write. Paganel does
    /// not change server config - it only surfaces settings the DBA controls that
    /// throttle bulk loads, so the operator knows what to ask for. No-op after the
    /// first call and never invoked on read-only source connections.
    pub(crate) async fn bulk_preflight_advisory(&self, conn: &mut Conn) {
        if self.advised.swap(true, Ordering::Relaxed) {
            return;
        }

        // Without `local_infile`, LOAD DATA is unavailable and bulk writes fall
        // back to slow multi-row INSERT.
        if !self.capabilities.copy_protocol {
            warn!(
                driver = "mysql",
                "local_infile is off; bulk writes fall back to slow INSERT. Set local_infile=1 \
                 to enable the LOAD DATA fast path."
            );
        }

        // A small redo log forces frequent checkpoints; large loads stall on
        // flushing. `innodb_redo_log_capacity` exists on MySQL 8.0.30+; on older
        // servers the query errors and we silently skip (nothing to advise on).
        if let Ok(Some(redo)) = conn
            .query_first::<i64, _>("SELECT @@GLOBAL.innodb_redo_log_capacity")
            .await
            && redo < MIN_HEALTHY_REDO_BYTES
        {
            warn!(
                driver = "mysql",
                redo_mb = redo / (1024 * 1024),
                "redo log is small; large loads stall on checkpoint flushing. Raise \
                 innodb_redo_log_capacity (e.g. 4G)."
            );
        }
    }

    /// Fetches the version string from the DB and resolves capabilities.
    async fn detect_capabilities(pool: &Pool) -> Result<Capabilities, DriverError> {
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| DriverError::ConnectionError(e.to_string()))?;

        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .ok_or_else(|| DriverError::QueryError("Failed to retrieve database version".into()))?;

        // `LOAD DATA LOCAL INFILE` (fast-path bulk load) only works when the
        // server has `local_infile` enabled. `@@GLOBAL.local_infile` returns 0/1.
        let local_infile_enabled = conn
            .query_first::<i64, _>("SELECT @@GLOBAL.local_infile")
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .map(|v| v != 0)
            .unwrap_or(false);

        // The largest single packet the server accepts.
        let max_allowed_packet = conn
            .query_first::<u64, _>("SELECT @@max_allowed_packet")
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .map(|v| v as usize);

        // Drop connection explicitly or let it drop out of scope.
        drop(conn);

        Ok(Self::resolve_capabilities(
            version,
            local_infile_enabled,
            max_allowed_packet,
        ))
    }

    fn resolve_capabilities(
        version: String,
        local_infile_enabled: bool,
        max_allowed_packet: Option<usize>,
    ) -> Capabilities {
        // MySQL has no `RETURNING`. MariaDB added `INSERT ... RETURNING` in 10.5;
        // since our write path uses INSERT, gate the capability on that version.
        let supports_returning = Self::mariadb_supports_returning(&version);

        Capabilities {
            version,
            transactions: true,
            savepoints: true,
            copy_protocol: local_infile_enabled, // LOAD DATA LOCAL INFILE requires server `local_infile=1`
            upsert: true,                        // ON DUPLICATE KEY UPDATE
            returning_clause: supports_returning,
            json_type: true,   // Supported in MySQL 5.7+ and MariaDB 10.2+ (as alias)
            jsonb_type: false, // MySQL has JSON, but not a distinct JSONB binary type like PG
            array_type: false,
            uuid_type: false, // Usually stored as BINARY(16) or CHAR(36)
            geometry_type: true,
            max_parameters: Some(MYSQL_MAX_PREPARED_STMT_PARAMS.into()),
            max_query_size: max_allowed_packet, // server's max_allowed_packet
        }
    }

    /// Returns `true` when the version string denotes MariaDB 10.5 or newer,
    /// which is when `INSERT ... RETURNING` became available.
    fn mariadb_supports_returning(version: &str) -> bool {
        if !version.to_lowercase().contains("mariadb") {
            return false;
        }

        // Strip the legacy `5.5.5-` compatibility prefix if present.
        let core = version.strip_prefix("5.5.5-").unwrap_or(version);

        // Parse the leading `major.minor` from the remaining string.
        let mut parts = core.split('.');
        let major: u32 = match parts.next().and_then(|p| p.parse().ok()) {
            Some(v) => v,
            None => return false,
        };
        // The minor segment may carry a suffix, e.g. `6-MariaDB`.
        let minor: u32 = match parts.next().map(|p| {
            p.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(0)
        }) {
            Some(v) => v,
            None => return false,
        };

        (major, minor) >= (10, 5)
    }
}

impl Driver for MySqlDriver {
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
