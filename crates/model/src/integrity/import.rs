use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const IMPORT_PREFIX: &str = "@import/";

/// The pipeline name an import's receipt and row hashes are stored under.
pub fn import_pipeline(name: &str) -> String {
    format!("{IMPORT_PREFIX}{name}")
}

/// Import names are path- and key-safe: [A-Za-z0-9_-]{1,64}.
pub fn validate_import_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');

    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid import name '{name}': use 1-64 letters, digits, '_' or '-'"
        ))
    }
}

/// Where an imported baseline came from. Its receipt and row hashes are stored
/// under `import_pipeline(&name)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRecord {
    pub name: String,
    /// Connection block name from the config, never the URL.
    pub connection: String,
    pub table: String,
    pub key_columns: Vec<String>,
    pub imported_at: DateTime<Utc>,
    pub rows: u64,
}
