#![deny(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod drivers;
pub mod error;
pub mod session;
pub mod sql;
pub mod traits;
