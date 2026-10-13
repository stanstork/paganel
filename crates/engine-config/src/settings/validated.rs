use std::collections::HashMap;

use connectors::drivers::postgres::config::PkCreation;
use engine_processing::producer::config::DEFAULT_BATCH_SIZE;
use model::{core::value::Value, execution::flags::IntegrityMode};
use serde::Serialize;

/// Immutable, validated configuration used throughout the migration.
#[derive(Serialize, Debug, Clone)]
pub struct ValidatedSettings {
    /// Batch size for reading and writing data
    pub batch_size: usize,
    /// Whether to create missing tables at destination
    pub create_missing_tables: bool,
    /// Whether to create missing columns at destination
    pub create_missing_columns: bool,
    /// Skip primary keys: create destination tables without a PK and never add
    /// one (also disables `pk_creation` deferral).
    pub skip_pk: bool,
    /// Skip foreign keys: don't create FK constraints on the destination.
    pub skip_fk: bool,
    /// Skip secondary indexes: don't create non-constraint indexes on the destination.
    pub skip_idx: bool,
    /// Skip sequences: don't create identity/auto-increment sequences on the destination.
    pub skip_seq: bool,
    /// Skip UNIQUE constraints on the destination.
    pub skip_unique: bool,
    /// Skip CHECK constraints on the destination.
    pub skip_check: bool,
    /// Parallel range lanes for a single-table snapshot copy (>= 1).
    pub lanes: usize,
    /// When the destination primary key is created relative to the bulk load.
    pub pk_creation: PkCreation,
    /// Whether this is a dry run (no changes applied)
    pub dry_run: bool,
    /// Integrity hashing mode for this migration run.
    pub integrity: IntegrityMode,
}

impl ValidatedSettings {
    pub fn default(dry_run: bool) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            create_missing_tables: false,
            create_missing_columns: false,
            skip_pk: false,
            skip_fk: false,
            skip_idx: false,
            skip_seq: false,
            skip_unique: false,
            skip_check: false,
            lanes: 1,
            pk_creation: PkCreation::Pre,
            dry_run,
            integrity: IntegrityMode::Off,
        }
    }

    pub fn from_pipeline(
        settings: &HashMap<String, Value>,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Self {
        let mut s = Self::default(dry_run);
        s.integrity = integrity;
        if let Some(n) = read_usize(settings, "batch_size") {
            s.batch_size = n;
        }
        if let Some(n) = read_usize(settings, "lanes") {
            s.lanes = n.clamp(1, 32);
        }
        s
    }

    pub fn from_builder(builder: ValidatedSettingsBuilder) -> Self {
        Self {
            batch_size: builder.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            create_missing_tables: builder.create_missing_tables.unwrap_or(false),
            create_missing_columns: builder.create_missing_columns.unwrap_or(false),
            skip_pk: builder.skip_pk.unwrap_or(false),
            skip_fk: builder.skip_fk.unwrap_or(false),
            skip_idx: builder.skip_idx.unwrap_or(false),
            skip_seq: builder.skip_seq.unwrap_or(false),
            skip_unique: builder.skip_unique.unwrap_or(false),
            skip_check: builder.skip_check.unwrap_or(false),
            lanes: builder.lanes.unwrap_or(1),
            pk_creation: builder.pk_creation.unwrap_or_default(),
            dry_run: builder.dry_run,
            integrity: builder.integrity,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn create_missing_tables(&self) -> bool {
        self.create_missing_tables
    }

    pub fn create_missing_columns(&self) -> bool {
        self.create_missing_columns
    }

    pub fn skip_pk(&self) -> bool {
        self.skip_pk
    }

    pub fn skip_fk(&self) -> bool {
        self.skip_fk
    }

    pub fn skip_idx(&self) -> bool {
        self.skip_idx
    }

    pub fn skip_seq(&self) -> bool {
        self.skip_seq
    }

    pub fn skip_unique(&self) -> bool {
        self.skip_unique
    }

    pub fn skip_check(&self) -> bool {
        self.skip_check
    }

    pub fn lanes(&self) -> usize {
        self.lanes
    }

    pub fn pk_creation(&self) -> PkCreation {
        self.pk_creation
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn requires_schema_op(&self) -> bool {
        self.create_missing_tables || self.create_missing_columns
    }

    pub fn integrity(&self) -> IntegrityMode {
        self.integrity
    }
}

/// Read a settings value as a positive integer, accepting either `UInt` or a
/// positive `Int` (PPL literals can land as either). Returns `None` otherwise.
fn read_usize(settings: &HashMap<String, Value>, key: &str) -> Option<usize> {
    match settings.get(key) {
        Some(Value::UInt(n)) => Some(*n as usize),
        Some(Value::Int(n)) if *n > 0 => Some(*n as usize),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub struct ValidatedSettingsBuilder {
    pub batch_size: Option<usize>,
    pub create_missing_tables: Option<bool>,
    pub create_missing_columns: Option<bool>,
    pub skip_pk: Option<bool>,
    pub skip_fk: Option<bool>,
    pub skip_idx: Option<bool>,
    pub skip_seq: Option<bool>,
    pub skip_unique: Option<bool>,
    pub skip_check: Option<bool>,
    pub lanes: Option<usize>,
    pub pk_creation: Option<PkCreation>,
    pub dry_run: bool,
    pub integrity: IntegrityMode,
}

impl ValidatedSettingsBuilder {
    pub fn new(dry_run: bool, integrity: IntegrityMode) -> Self {
        Self {
            dry_run,
            integrity,
            ..Default::default()
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn create_missing_tables(mut self, create_missing_tables: bool) -> Self {
        self.create_missing_tables = Some(create_missing_tables);
        self
    }

    pub fn create_missing_columns(mut self, create_missing_columns: bool) -> Self {
        self.create_missing_columns = Some(create_missing_columns);
        self
    }

    pub fn skip_pk(mut self, skip: bool) -> Self {
        self.skip_pk = Some(skip);
        self
    }

    pub fn skip_fk(mut self, skip: bool) -> Self {
        self.skip_fk = Some(skip);
        self
    }

    pub fn skip_idx(mut self, skip: bool) -> Self {
        self.skip_idx = Some(skip);
        self
    }

    pub fn skip_seq(mut self, skip: bool) -> Self {
        self.skip_seq = Some(skip);
        self
    }

    pub fn skip_unique(mut self, skip: bool) -> Self {
        self.skip_unique = Some(skip);
        self
    }

    pub fn skip_check(mut self, skip: bool) -> Self {
        self.skip_check = Some(skip);
        self
    }

    pub fn lanes(mut self, lanes: usize) -> Self {
        self.lanes = Some(lanes);
        self
    }

    pub fn build(self) -> ValidatedSettings {
        ValidatedSettings::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = ValidatedSettings::default(false);
        assert_eq!(settings.batch_size(), DEFAULT_BATCH_SIZE);
        assert!(!settings.is_dry_run());
        assert!(!settings.requires_schema_op());
    }

    #[test]
    fn test_builder() {
        let settings = ValidatedSettingsBuilder::new(true, IntegrityMode::On)
            .batch_size(500)
            .create_missing_tables(true)
            .build();

        assert_eq!(settings.batch_size(), 500);
        assert!(settings.is_dry_run());
        assert!(settings.requires_schema_op());
    }
}
