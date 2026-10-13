use crate::{context::env::EnvContext, drivers::DriverRef, plan::execution::ExecutionPlan};
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    traits::{driver::Driver, introspector::SchemaIntrospector},
};
use engine_schema::metadata_cache::MetadataCache;
use engine_state::{RowHashLog, StateStore};
use model::execution::connection::Connection;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

const METADATA_CACHE_TIMEOUT: Duration = Duration::from_secs(30);

/// Holds connections and file adapters for the duration of a migration.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Connection pool - reuses drivers across pipelines
    conn_pool: Arc<RwLock<ConnectionPool>>,

    /// Read-through metadata caches, shared across every pipeline.
    /// Only source (read-only) introspection is cached here; the
    /// destination is mutated by DDL mid-pipeline and must not be cached.
    meta_caches: Arc<RwLock<HashMap<String, Arc<dyn SchemaIntrospector>>>>,

    run_id: String,
    state: Arc<StateStore>,
    hash_log: Arc<RowHashLog>,
    env: Arc<EnvContext>,
}

impl ExecutionContext {
    pub fn new(
        plan: &ExecutionPlan,
        state: Arc<StateStore>,
        hash_log: Arc<RowHashLog>,
        env: Arc<EnvContext>,
    ) -> Self {
        let run_id = plan.run_id();
        let conn_pool = Arc::new(RwLock::new(ConnectionPool::new()));
        let meta_caches = Arc::new(RwLock::new(HashMap::new()));

        ExecutionContext {
            conn_pool,
            meta_caches,
            run_id,
            state,
            hash_log,
            env,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn hash_log(&self) -> &Arc<RowHashLog> {
        &self.hash_log
    }

    pub fn state(&self) -> &Arc<StateStore> {
        &self.state
    }

    pub fn env(&self) -> &Arc<EnvContext> {
        &self.env
    }

    /// Resolve a connection to a typed `DriverRef`, reusing pooled connections.
    pub async fn resolve_driver(&self, conn: &Connection) -> Result<DriverRef, DriverError> {
        let mut pool = self.conn_pool.write().await;
        DriverRef::resolve(&conn.driver, conn, &mut pool).await
    }

    /// A read-through introspector for a **source** connection,
    /// cached by `conn.name` and shared across pipelines.
    pub async fn source_introspector(
        &self,
        conn: &Connection,
    ) -> Result<Arc<dyn SchemaIntrospector>, DriverError> {
        if let Some(cache) = self.meta_caches.read().await.get(&conn.name) {
            return Ok(cache.clone());
        }

        let driver = self.resolve_driver(conn).await?;
        let dialect = driver.dialect();
        let cache: Arc<dyn SchemaIntrospector> = crate::dispatch_driver!(driver, |d| {
            Arc::new(MetadataCache::new(d, dialect, METADATA_CACHE_TIMEOUT))
                as Arc<dyn SchemaIntrospector>
        });

        let mut caches = self.meta_caches.write().await;
        if let Some(existing_cache) = caches.get(&conn.name) {
            return Ok(existing_cache.clone());
        }

        caches.insert(conn.name.clone(), cache.clone());
        Ok(cache)
    }
}

/// Connection pool for reusing drivers.
pub struct ConnectionPool {
    pg_drivers: HashMap<String, Arc<PgDriver>>,
    mysql_drivers: HashMap<String, Arc<MySqlDriver>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        ConnectionPool {
            pg_drivers: HashMap::new(),
            mysql_drivers: HashMap::new(),
        }
    }

    pub async fn driver(&mut self, conn: &Connection) -> Result<Arc<dyn Driver>, DriverError> {
        let driver_ref = DriverRef::resolve(&conn.driver, conn, self).await?;

        match driver_ref {
            DriverRef::Postgres(pg) => Ok(pg as Arc<dyn Driver>),
            DriverRef::MySql(mysql) => Ok(mysql as Arc<dyn Driver>),
        }
    }

    /// Get or create a PostgreSQL driver with full type information.
    pub async fn postgres(&mut self, conn: &Connection) -> Result<Arc<PgDriver>, DriverError> {
        if let Some(driver) = self.pg_drivers.get(&conn.name) {
            return Ok(driver.clone());
        }

        let url = Self::get_url(conn)?;
        let driver = match conn.properties.get_string("schema") {
            Some(schema) => PgDriver::connect_with_schema(&url, &schema).await?,
            None => PgDriver::connect(&url).await?,
        };

        let driver = Arc::new(driver);
        self.pg_drivers.insert(conn.name.clone(), driver.clone());
        Ok(driver)
    }

    /// Get or create a MySQL driver with full type information.
    pub async fn mysql(&mut self, conn: &Connection) -> Result<Arc<MySqlDriver>, DriverError> {
        if let Some(driver) = self.mysql_drivers.get(&conn.name) {
            return Ok(driver.clone());
        }

        let url = Self::get_url(conn)?;
        let driver = Arc::new(MySqlDriver::connect(&url).await?);

        self.mysql_drivers.insert(conn.name.clone(), driver.clone());
        Ok(driver)
    }

    fn get_url(conn: &Connection) -> Result<String, DriverError> {
        conn.properties
            .get_string("url")
            .ok_or_else(|| DriverError::InvalidUrl("missing 'url' property".to_string()))
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
