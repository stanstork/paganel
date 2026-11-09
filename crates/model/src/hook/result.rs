use crate::hook::event::HookEvent;
use serde::{Deserialize, Serialize};

/// What happened to one hook's POST.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookDeliveryResult {
    pub hook: String,
    pub events: Vec<HookEvent>,
    pub ok: bool,
    /// The last HTTP status, if a response came back.
    pub status: Option<u16>,
    pub attempts: u32,
    pub error: Option<String>,
}
