use crate::error::StateStoreError;
use std::path::PathBuf;

/// Where `apply`, `verify` and every other command keep state: `~/.paganel/state`.
pub fn default_state_dir() -> Result<PathBuf, StateStoreError> {
    dirs::home_dir()
        .map(|home| home.join(".paganel").join("state"))
        .ok_or_else(|| StateStoreError::Storage("could not determine the home directory".into()))
}
