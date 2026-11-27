use super::side::Side;
use crate::{error::VerifyError, table::table_reader};
use connectors::sql::checksum;
use model::{
    core::value::Value,
    integrity::{
        canonical::serialize_value,
        normal::{Family, normal_text},
    },
    pagination::cursor::Cursor,
    records::Record,
};
use std::collections::HashMap;

/// Rows the pre-flight compares per side.
const PREFLIGHT_ROWS: usize = 200;

impl Side<'_> {
    /// The first rows' normal text as the database writes it, against
    /// `normal_text` of the values the reader returns for the same rows.
    pub(super) async fn preflight(
        &self,
        families: &[Family],
    ) -> Result<Option<String>, VerifyError> {
        let keys = &self.spec.key_columns;
        let select = checksum::normal_select(
            self.sql,
            self.table,
            keys,
            &self.columns,
            self.filter_expr.as_ref(),
            Some(PREFLIGHT_ROWS),
        );
        let rows = self.query(&select).await?;
        if rows.is_empty() {
            return Ok(None);
        }

        let mut firsts = Vec::new();
        for v in rows.iter().filter_map(|r| r.value("k0").cloned()) {
            if !firsts.contains(&v) {
                firsts.push(v);
            }
        }
        let reader = table_reader(self.driver, self.meta, self.spec)?
            .with_filter(self.filter.cloned())
            .with_keys(keys[0].clone(), firsts);

        let mut read = HashMap::with_capacity(rows.len());
        let mut cursor = Some(Cursor::None);
        while let Some(c) = cursor {
            let (batch, next) = reader.next_batch(c, PREFLIGHT_ROWS * 4).await?;
            for row in batch {
                read.insert(encode_key(&row, keys), row);
            }
            cursor = next;
        }

        let sql_key_names: Vec<String> = (0..keys.len()).map(|i| format!("k{i}")).collect();
        for row in &rows {
            // A row written between the two reads is skipped, not judged.
            let Some(typed) = read.get(&encode_key(row, &sql_key_names)) else {
                continue;
            };

            for (column, family) in self.columns.iter().zip(families) {
                let from_database = text_of(row.value(&column.name));
                let value = typed.value(&column.name).cloned().unwrap_or(Value::Null);
                let from_reader = match normal_text(*family, &value) {
                    Ok(text) => text,
                    Err(e) => {
                        return Ok(Some(format!("{} column {}: {e}", self.name, column.name)));
                    }
                };
                if from_database.as_deref() != from_reader.as_deref() {
                    return Ok(Some(format!(
                        "{} column {}: the database writes {from_database:?} where the reader returns {from_reader:?}",
                        self.name, column.name
                    )));
                }
            }
        }
        Ok(None)
    }
}

fn encode_key(row: &Record, columns: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for col in columns {
        serialize_value(row.value(col).unwrap_or(&Value::Null), &mut buf);
    }
    buf
}

/// A text the database returned.
fn text_of(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Binary(b) => Some(String::from_utf8_lossy(b).into_owned()),
        other => Some(format!("{other:?}")),
    }
}
