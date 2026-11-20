use std::time::Duration;

/// What a block comparison sets on its pinned connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionSettings {
    pub statement_timeout: Option<Duration>,
}

impl SessionSettings {
    /// Run on every new MySQL connection. `max_execution_time` limits only
    /// `SELECT`s, which is all a comparison runs.
    pub fn mysql_statements(&self) -> Vec<String> {
        self.build_statements(
            &["SET time_zone = '+00:00'", "SET NAMES utf8mb4"],
            "SET SESSION max_execution_time",
            u32::MAX,
        )
    }

    /// Run once on a dedicated Postgres connection.
    pub fn postgres_statements(&self) -> Vec<String> {
        self.build_statements(
            &["SET TIME ZONE 'UTC'", "SET client_encoding = 'UTF8'"],
            "SET statement_timeout",
            i32::MAX as u32,
        )
    }

    fn build_statements(&self, base: &[&str], timeout_var: &str, max_ms: u32) -> Vec<String> {
        let timeout = self
            .statement_timeout
            .map(|d| format!("{timeout_var} = {}", timeout_ms(d, max_ms)));

        base.iter()
            .map(|s| (*s).to_owned())
            .chain(timeout)
            .collect()
    }
}

fn timeout_ms(d: Duration, max_ms: u32) -> u32 {
    let ms = d.as_millis();
    if ms == 0 && !d.is_zero() {
        1
    } else {
        ms.min(u128::from(max_ms)) as u32
    }
}
