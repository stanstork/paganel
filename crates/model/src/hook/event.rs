use serde::{Deserialize, Serialize};
use std::fmt;

pub const RESERVED_EVENTS: [&str; 3] = ["apply.completed", "apply.failed", "verify.mismatch"];

/// An event a command emits at the end of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookEvent {
    /// Every drift run, whatever the result.
    #[serde(rename = "drift.completed")]
    DriftCompleted,
    /// A watched table is a mismatch.
    #[serde(rename = "drift.diverged")]
    DriftDiverged,
    /// A watched table could not be checked.
    #[serde(rename = "drift.failed")]
    DriftFailed,
}

impl HookEvent {
    pub const ALL: [HookEvent; 3] = [
        HookEvent::DriftCompleted,
        HookEvent::DriftDiverged,
        HookEvent::DriftFailed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            HookEvent::DriftCompleted => "drift.completed",
            HookEvent::DriftDiverged => "drift.diverged",
            HookEvent::DriftFailed => "drift.failed",
        }
    }

    pub fn command(self) -> &'static str {
        match self {
            HookEvent::DriftCompleted | HookEvent::DriftDiverged | HookEvent::DriftFailed => {
                "drift"
            }
        }
    }

    pub fn parse(name: &str) -> Result<Self, String> {
        if let Some(event) = Self::ALL.into_iter().find(|e| e.as_str() == name) {
            return Ok(event);
        }
        if RESERVED_EVENTS.contains(&name) {
            return Err(format!("event `{name}` is not supported yet"));
        }

        let known: Vec<&str> = Self::ALL.iter().map(|e| e.as_str()).collect();
        Err(format!(
            "unknown event `{name}`; use one of {}",
            known.join(", ")
        ))
    }
}

impl fmt::Display for HookEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
