use crate::io::{
    filter::{Filter, combine_filters},
    format::DataFormat,
    linked::LinkedSource,
    source::{
        csv::reader::CsvSourceReader, db::reader::DbSourceReader, reader::SourceReader,
        wasm::reader::WasmSourceReader,
    },
};
use connectors::{
    drivers::csv::{
        adapter::CsvAdapter, compile as csv_filter, metadata::CsvMetadata, settings::CsvSettings,
        source::CsvDataSource,
    },
    error::DriverError,
    sql::{filter::compile as sql_filter, metadata::table::TableMetadata},
    traits::{introspector::SchemaIntrospector, reader::DataReader},
};
use engine_wasm::runtime::instance::PluginInstance;
use model::{
    execution::pipeline::{Pipeline, ValidationKind},
    pagination::{cursor::Cursor, page::FetchResult},
    transform::mapping::TransformationMetadata,
};
use query_builder::{
    dialect::{self, QueryDialect},
    offsets::{OffsetStrategy, OffsetStrategyFactory},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::warn;

pub mod csv;
pub mod db;
pub mod reader;
pub mod wasm;

#[derive(Clone)]
pub struct Source {
    pub name: String,
    pub format: DataFormat,
    pub primary: Arc<dyn SourceReader>,
    pub linked: Option<LinkedSource>,
    pub filter: Option<Filter>,
}

impl Source {
    pub async fn new<D>(
        driver: Arc<D>,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<Self, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        Self::with_cascade(driver, pipeline, mapping, offset_strategy, None).await
    }

    pub fn from_plugin(plugin: PluginInstance, pipeline: &Pipeline) -> Result<Self, DriverError> {
        let name = pipeline.source.table.clone();
        let format = DataFormat::Wasm;
        let reader = Arc::new(WasmSourceReader::new(plugin, name.clone()));

        Ok(Source {
            name,
            format,
            primary: reader,
            linked: None, // joins not supported for WASM sources
            filter: None, // filter pushdown not supported; rely on validate{} rules
        })
    }

    pub fn from_csv(
        pipeline: &Pipeline,
        path: &str,
        settings: CsvSettings,
        meta: CsvMetadata,
    ) -> Result<Self, DriverError> {
        let name = pipeline.source.table.clone();
        let adapter = CsvAdapter::new(path, settings)
            .map_err(|e| DriverError::ConnectionError(format!("open CSV '{path}': {e}")))?;

        let filter = match Self::create_filter(pipeline, &DataFormat::Csv)? {
            Some(Filter::Csv(f)) => Some(f),
            _ => None,
        };

        // Stamp the inferred schema with the logical table name so emitted
        // records (and destination-table creation) use it, not the file path.
        let mut meta = meta;
        meta.name = name.clone();
        let data_source = CsvDataSource::new(adapter, filter, meta);

        let reader = Arc::new(CsvSourceReader::new(data_source, name.clone()));

        Ok(Source {
            name,
            format: DataFormat::Csv,
            primary: reader,
            linked: None,
            filter: None,
        })
    }

    /// Create a source with optional cascade metadata for graph-based migration.
    pub async fn with_cascade<D>(
        driver: Arc<D>,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
        cascade_meta: Option<HashMap<String, TableMetadata>>,
    ) -> Result<Self, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        let name = pipeline.source.table.clone();
        let format = DataFormat::parse(&pipeline.source.connection.driver).ok_or_else(|| {
            DriverError::UnsupportedFormat(pipeline.source.connection.driver.clone())
        })?;

        let linked =
            LinkedSource::new(driver.clone(), &format, &pipeline.source.joins, mapping).await?;
        let filter = Self::create_filter(pipeline, &format)?;

        if !pipeline.source.joins.is_empty()
            && pipeline
                .source
                .pagination
                .as_ref()
                .and_then(|p| p.tiebreaker.as_ref())
                .is_none()
        {
            warn!(
                table = %name,
                "`with` join without a `paginate` tiebreaker: if the join fans out (1:N), \
                 keyset pagination can drop rows at batch boundaries. Add a row-unique \
                 `paginate {{ strategy = \"pk\", cursor = \"<pk>\", tiebreaker = \"<unique col>\" }}`."
            );
        }

        // Fetch primary table metadata upfront so the reader always knows which
        // columns to select, even for simple (non-cascade) pipelines.
        let primary_meta = driver.table_metadata(&name).await.ok();

        let offset_strategy = match &primary_meta {
            Some(meta) => {
                OffsetStrategyFactory::keyset_over_pk(offset_strategy, &name, &meta.primary_keys)
            }
            None => offset_strategy,
        };

        let is_cascade = cascade_meta
            .as_ref()
            .is_some_and(|m| m.keys().any(|t| !t.eq_ignore_ascii_case(&name)));

        // Projection pushdown: when the pipeline declares a `select`, read only
        // the source columns actually referenced (plus the pagination columns).
        let projection = Self::compute_projection(pipeline, &offset_strategy, &filter, is_cascade);

        let primary = Self::build_primary_reader(
            &name,
            &format,
            driver,
            &linked,
            &filter,
            offset_strategy,
            cascade_meta,
            primary_meta,
            projection,
        )?;

        Ok(Source {
            name,
            format,
            primary,
            linked,
            filter,
        })
    }

    /// Build a source that reads one full table (no joins, no filter, no cascade scoping).
    pub async fn single_table<D>(
        driver: Arc<D>,
        table: &str,
        format: DataFormat,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<Self, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        let meta = driver.table_metadata(table).await?;
        let offset_strategy =
            OffsetStrategyFactory::keyset_over_pk(offset_strategy, table, &meta.primary_keys);

        let primary = Self::build_primary_reader(
            table,
            &format,
            driver,
            &None, // no joins
            &None, // no filter
            offset_strategy,
            None,       // no cascade metadata -> fetch_single (whole table)
            Some(meta), // primary metadata for column selection
            None,       // whole-table read: no projection pushdown
        )?;

        Ok(Source {
            name: table.to_string(),
            format,
            primary,
            linked: None,
            filter: None,
        })
    }

    pub async fn fetch(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Result<FetchResult, DriverError> {
        self.primary.fetch(batch_size, cursor).await
    }

    pub fn format(&self) -> DataFormat {
        self.format
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query_dialect(&self) -> Box<dyn QueryDialect> {
        match self.format {
            DataFormat::MySql => Box::new(dialect::MySql),
            DataFormat::Postgres => Box::new(dialect::Postgres),
            _ => panic!("Unsupported dialect for source"),
        }
    }

    /// The set of source columns the pipeline actually needs from the primary
    /// table: everything referenced by the `select`, `when`, plugin inputs,
    /// validations, and filter, plus the pagination cursor columns. `None` when
    /// the pipeline has no projection (migrate every column - no pushdown).
    fn compute_projection(
        pipeline: &Pipeline,
        offset_strategy: &Arc<dyn OffsetStrategy>,
        filter: &Option<Filter>,
        is_cascade: bool,
    ) -> Option<HashSet<String>> {
        if !pipeline.has_projection() {
            return None;
        }

        if !pipeline.source.joins.is_empty() || is_cascade {
            return None;
        }

        let mut cols: HashSet<String> = HashSet::new();

        // `select` expressions (primary and named/cascade selects).
        for t in &pipeline.transformations {
            t.expression.collect_column_refs(&mut cols);
        }

        for transforms in pipeline.named_transformations.values() {
            for t in transforms {
                t.expression.collect_column_refs(&mut cols);
            }
        }

        // Plugin transform inputs are source columns.
        for call in &pipeline.plugin_transforms {
            for source_col in call.input_mapping.values() {
                cols.insert(source_col.to_ascii_lowercase());
            }
        }

        // Validations run on the row and may reference source columns.
        for rule in &pipeline.validations {
            match &rule.kind {
                ValidationKind::Assert { check } => {
                    check.collect_column_refs(&mut cols);
                }
                ValidationKind::WasmFilter { input_mapping, .. } => {
                    for source_col in input_mapping.values() {
                        cols.insert(source_col.to_ascii_lowercase());
                    }
                }
            }
        }

        // Row filter pushed to the source references columns too.
        if let Some(Filter::Sql(sf)) = filter {
            for c in sf.columns() {
                cols.insert(c.to_ascii_lowercase());
            }
        }

        // Pagination cursor columns MUST be read or the cursor advances to NULL.
        for c in offset_strategy.required_columns() {
            cols.insert(c.to_ascii_lowercase());
        }

        Some(cols)
    }

    /// Helper to isolate the complex logic of constructing the primary data reader
    #[allow(clippy::too_many_arguments)]
    fn build_primary_reader<D>(
        name: &str,
        format: &DataFormat,
        driver: Arc<D>,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
        offset_strategy: Arc<dyn OffsetStrategy>,
        cascade_meta: Option<HashMap<String, TableMetadata>>,
        primary_meta_fallback: Option<TableMetadata>,
        projection: Option<HashSet<String>>,
    ) -> Result<Arc<dyn SourceReader>, DriverError>
    where
        D: DataReader + SchemaIntrospector,
    {
        match format {
            DataFormat::MySql | DataFormat::Postgres => {
                let sql_filter = match filter {
                    Some(Filter::Sql(sf)) => Some(sf.clone()),
                    _ => None,
                };

                let join = match linked {
                    Some(LinkedSource::Table(j)) => Some((**j).clone()),
                    _ => None,
                };

                let mut reader = DbSourceReader::new(
                    driver as Arc<dyn DataReader>,
                    join,
                    sql_filter,
                    offset_strategy,
                    projection,
                );

                if let Some(mut cascade) = cascade_meta {
                    if let Some(primary_meta) = cascade.remove(name) {
                        reader.set_primary_meta(primary_meta);
                    }

                    // The remaining map natively represents all related tables.
                    if !cascade.is_empty() {
                        reader.set_related_meta(cascade);
                    }
                }

                // If cascade didn't provide primary metadata (non-cascade pipeline),
                // use the directly-fetched metadata so the reader knows which columns to select.
                if !reader.has_primary_meta()
                    && let Some(meta) = primary_meta_fallback
                {
                    reader.set_primary_meta(meta);
                }

                Ok(Arc::new(reader))
            }
            _ => Err(DriverError::UnsupportedFormat(format!("{:?}", format))),
        }
    }

    fn create_filter(
        pipeline: &Pipeline,
        format: &DataFormat,
    ) -> Result<Option<Filter>, DriverError> {
        let combined_condition = match combine_filters(&pipeline.source.filters) {
            Some(cond) => cond,
            None => return Ok(None),
        };

        match format {
            DataFormat::MySql | DataFormat::Postgres => {
                let filter = sql_filter::compile_filter(&combined_condition)
                    .map_err(|e| DriverError::QueryError(e.to_string()))?;
                Ok(Some(Filter::Sql(filter)))
            }
            DataFormat::Csv => {
                let filter = csv_filter::compile_filter(&combined_condition)
                    .map_err(|e| DriverError::QueryError(e.to_string()))?;
                Ok(Some(Filter::Csv(filter)))
            }
            _ => Err(DriverError::UnsupportedFormat(format!(
                "filters not supported for format {:?}",
                format
            ))),
        }
    }
}
