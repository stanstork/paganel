use connectors::{
    sql::{
        filter::SqlFilter,
        metadata::table::TableMetadata,
        request::{FetchRowsRequest, FetchRowsRequestBuilder},
    },
    traits::reader::DataReader,
};
use model::{core::value::Value, pagination::cursor::Cursor, records::Record};
use query_builder::offsets::OffsetStrategy;
use std::sync::Arc;

pub struct TableReader {
    driver: Arc<dyn DataReader>,
    table: TableMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
    filter: Option<SqlFilter>,
    keys: Option<(String, Vec<Value>)>,
}

impl TableReader {
    pub fn new(
        driver: Arc<dyn DataReader>,
        table: TableMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Self {
        Self {
            driver,
            table,
            offset_strategy,
            filter: None,
            keys: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<SqlFilter>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_keys(mut self, column: String, values: Vec<Value>) -> Self {
        self.keys = Some((column, values));
        self
    }

    /// Fetch the next batch of rows starting from `cursor`, with `limit` rows.
    /// Returns `(rows, next_cursor)` where `next_cursor = None` signals the table is exhausted.
    pub async fn next_batch(
        &self,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Cursor>), crate::error::VerifyError> {
        let request = self.build_request(&cursor, limit);
        let rows = self.driver.fetch(request).await?;

        let reached_end = rows.len() < limit;
        let last_row = rows.last().cloned();
        let next_cursor =
            self.offset_strategy
                .next_after_batch(last_row.as_ref(), &cursor, limit, reached_end);

        Ok((rows, next_cursor))
    }

    fn build_request(&self, cursor: &Cursor, limit: usize) -> FetchRowsRequest {
        let table = self.table.name.clone();
        let columns = self.table.select_fields();

        let mut request = FetchRowsRequestBuilder::new(table.clone())
            .alias(table)
            .columns(columns)
            .filter(self.filter.clone())
            .limit(limit)
            .cursor(cursor.clone())
            .strategy(self.offset_strategy.clone());

        if let Some((column, values)) = &self.keys {
            request = request.in_clause(column.clone(), values.clone());
        }

        request.build()
    }
}
