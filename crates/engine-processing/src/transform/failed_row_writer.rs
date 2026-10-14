use crate::transform::error::FailedRowWriterError;
use connectors::{
    error::DriverError,
    traits::{introspector::SchemaIntrospector, writer::DataWriter},
};
use engine_core::{context::exec::ExecutionContext, dispatch_driver};
use model::{
    execution::{
        connection::Connection,
        failed_row::FailedRow,
        pipeline::{FailedRowsDestination, FileFormat},
    },
    records::Record,
};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};
use tracing::{debug, error, info};

/// Writer for failed rows to various destinations.
pub struct FailedRowWriter {
    destination: FailedRowsDestination,
    context: Arc<ExecutionContext>,
}

impl FailedRowWriter {
    pub fn new(destination: FailedRowsDestination, context: Arc<ExecutionContext>) -> Self {
        Self {
            destination,
            context,
        }
    }

    pub async fn write(&self, failed_row: &FailedRow) -> Result<(), FailedRowWriterError> {
        self.write_batch(std::slice::from_ref(failed_row)).await
    }

    pub async fn write_batch(&self, failed_rows: &[FailedRow]) -> Result<(), FailedRowWriterError> {
        if failed_rows.is_empty() {
            return Ok(());
        }

        match &self.destination {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                self.write_to_table(failed_rows, connection, table, schema.as_deref())
                    .await
            }
            FailedRowsDestination::File { path, format } => {
                self.write_to_file(failed_rows, path, format)
            }
        }
    }

    async fn write_to_table(
        &self,
        failed_rows: &[FailedRow],
        connection: &Connection,
        table: &str,
        schema: Option<&str>,
    ) -> Result<(), FailedRowWriterError> {
        let table_name = schema.map_or_else(|| table.to_string(), |s| format!("{s}.{table}"));

        let driver = match self.context.resolve_driver(connection).await {
            Ok(driver) => driver,
            Err(DriverError::UnsupportedDriver(_)) => {
                return Err(FailedRowWriterError::NoDestination);
            }
            Err(e) => return Err(e.into()),
        };

        dispatch_driver!(&driver, |d| {
            self.write_with_driver(&**d, failed_rows, &table_name).await
        })
    }

    /// Generic write method that works with any driver implementing the required traits.
    async fn write_with_driver<D>(
        &self,
        driver: &D,
        failed_rows: &[FailedRow],
        table_name: &str,
    ) -> Result<(), FailedRowWriterError>
    where
        D: SchemaIntrospector + DataWriter,
    {
        // Fetch table metadata
        let metadata = driver.table_metadata(table_name).await?;

        // Convert failed rows to RowData
        let rows: Vec<Record> = failed_rows
            .iter()
            .map(|fr| fr.to_row_data(table_name))
            .collect();

        // Write batch using driver's write_batch method
        driver.write_batch(&metadata, &rows).await?;

        info!(count = failed_rows.len(), table = %table_name, "wrote failed rows to table");
        Ok(())
    }

    fn write_to_file(
        &self,
        failed_rows: &[FailedRow],
        path: &str,
        format: &FileFormat,
    ) -> Result<(), FailedRowWriterError> {
        match format {
            FileFormat::Json => self.write_to_json(failed_rows, path),
            FileFormat::Csv => {
                error!("CSV format not yet implemented for failed rows");
                Err(FailedRowWriterError::UnsupportedFormat(FileFormat::Csv))
            }
            FileFormat::Parquet => {
                error!("Parquet format not yet implemented for failed rows");
                Err(FailedRowWriterError::UnsupportedFormat(FileFormat::Parquet))
            }
        }
    }

    fn write_to_json(
        &self,
        failed_rows: &[FailedRow],
        path: &str,
    ) -> Result<(), FailedRowWriterError> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let mut writer = BufWriter::new(file);

        // Stream JSON directly to the buffered writer instead of allocating intermediate Strings
        for row in failed_rows {
            serde_json::to_writer(&mut writer, row)?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;

        debug!(count = failed_rows.len(), path = %path, "wrote failed rows to file");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::plan::execution::ExecutionPlan;
    use engine_state::StateStore;
    use model::{core::value::Value, execution::failed_row::ProcessingStage};
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn create_test_failed_row() -> FailedRow {
        let mut original_data = HashMap::new();
        original_data.insert("user_id".to_string(), Value::Int(123));
        original_data.insert(
            "email".to_string(),
            Value::String("test@example.com".to_string()),
        );

        FailedRow::new(
            "test_pipeline".to_string(),
            ProcessingStage::Transform,
            original_data,
            "TransformError".to_string(),
            "Test error message".to_string(),
        )
    }

    async fn create_test_context() -> Arc<ExecutionContext> {
        use engine_core::context::env::EnvContext;
        use ppl_syntax::ast::{doc::PplDocument, span::Span};

        let doc = PplDocument {
            define_block: None,
            execution_block: None,
            connections: vec![],
            pipelines: vec![],
            plugins: vec![],
            span: Span::new(0, 0, 0, 0),
        };
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(StateStore::open(dir.path()).unwrap());
        let hash_log = Arc::new(engine_state::RowHashLog::in_state_dir(dir.path()));
        Arc::new(ExecutionContext::new(&plan, state, hash_log, env))
    }

    #[tokio::test]
    async fn test_write_to_json_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_ok());

        // Verify file contains JSON
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains("test_pipeline"));
        assert!(contents.contains("TransformError"));
    }

    #[tokio::test]
    async fn test_write_batch_to_json_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_rows = vec![
            create_test_failed_row(),
            create_test_failed_row(),
            create_test_failed_row(),
        ];

        let result = writer.write_batch(&failed_rows).await;
        assert!(result.is_ok());

        // Verify file contains 3 lines of JSON
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn test_write_to_csv_returns_unsupported() {
        let destination = FailedRowsDestination::File {
            path: "/tmp/test.csv".to_string(),
            format: FileFormat::Csv,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FailedRowWriterError::UnsupportedFormat(FileFormat::Csv)
        ));
    }

    #[tokio::test]
    async fn test_write_creates_parent_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("nested")
            .join("dir")
            .join("failed.json");
        let path_str = path.to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path_str.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_ok());
        assert!(path.exists());
    }
}
