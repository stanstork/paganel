use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{BufReader, BufWriter, ErrorKind},
    path::{Path, PathBuf},
};

/// Destination write path, the primary determinant of throughput.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteClass {
    /// PostgreSQL destination (binary/text COPY).
    Postgres,
    /// MySQL destination (`LOAD DATA` fast path).
    MySql,
    /// Anything else (e.g. INSERT fallback, plugin sinks).
    Other,
}

impl WriteClass {
    /// Classify from a connection driver string.
    pub fn from_driver(driver: &str) -> Self {
        match driver.to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Self::Postgres,
            "mysql" => Self::MySql,
            _ => Self::Other,
        }
    }
}

/// Moving-average throughput for one write class.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ClassStat {
    /// Exponential moving average of observed throughput (rows/sec).
    pub avg_throughput: u64,
    /// How many runs have contributed.
    pub observation_count: u64,
}

/// Historical throughput, per write class, learned from prior runs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CalibrationData {
    classes: HashMap<WriteClass, ClassStat>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl CalibrationData {
    pub fn path_for(home: &Path) -> PathBuf {
        home.join(".paganel").join("calibration.json")
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match File::open(path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                Ok(serde_json::from_reader(reader).unwrap_or_default())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut tmp = path.to_path_buf();
        let mut name = tmp.file_name().unwrap_or_default().to_os_string();

        name.push(".tmp");
        tmp.set_file_name(name);

        {
            let file = File::create(&tmp)?;
            let writer = BufWriter::new(file);
            serde_json::to_writer_pretty(writer, self)?;
        }

        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn record(&mut self, class: WriteClass, throughput: u64) {
        let stat = self.classes.entry(class).or_default();

        stat.avg_throughput = match stat.observation_count {
            0 => throughput,
            // Exponential moving average: 90% old, 10% new
            _ => (stat.avg_throughput * 9 + throughput) / 10,
        };

        stat.observation_count += 1;
        self.updated_at = Some(Utc::now());
    }

    pub fn throughput_for(&self, class: WriteClass) -> Option<u64> {
        self.classes
            .get(&class)
            .filter(|s| s.observation_count > 0 && s.avg_throughput > 0)
            .map(|s| s.avg_throughput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn from_driver_classifies() {
        assert_eq!(WriteClass::from_driver("postgres"), WriteClass::Postgres);
        assert_eq!(WriteClass::from_driver("PostgreSQL"), WriteClass::Postgres);
        assert_eq!(WriteClass::from_driver("mysql"), WriteClass::MySql);
        assert_eq!(WriteClass::from_driver("csv"), WriteClass::Other);
    }

    #[test]
    fn first_observation_seeds_then_ewma() {
        let mut data = CalibrationData::default();
        assert_eq!(data.throughput_for(WriteClass::Postgres), None);

        data.record(WriteClass::Postgres, 400_000);
        assert_eq!(data.throughput_for(WriteClass::Postgres), Some(400_000));

        // (400000*9 + 500000)/10 = 410000
        data.record(WriteClass::Postgres, 500_000);
        assert_eq!(data.throughput_for(WriteClass::Postgres), Some(410_000));

        // Classes are independent.
        assert_eq!(data.throughput_for(WriteClass::MySql), None);
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("calibration.json");

        let mut data = CalibrationData::default();
        data.record(WriteClass::MySql, 120_000);
        data.save(&path).unwrap();

        let loaded = CalibrationData::load(&path).unwrap();
        assert_eq!(loaded.throughput_for(WriteClass::MySql), Some(120_000));
        assert_eq!(loaded.throughput_for(WriteClass::Postgres), None);
    }

    #[test]
    fn missing_or_corrupt_file_loads_as_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("calibration.json");
        assert!(CalibrationData::load(&path).unwrap().updated_at.is_none());

        std::fs::write(&path, b"not json").unwrap();
        assert!(CalibrationData::load(&path).unwrap().updated_at.is_none());
    }
}
