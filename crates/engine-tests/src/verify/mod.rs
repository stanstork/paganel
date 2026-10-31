#[cfg(test)]
macro_rules! verify_config {
    ($name:expr) => {
        $crate::harness::Direction::MYSQL_TO_POSTGRES.config(concat!("verify/", $name))
    };
}

pub mod drift;
pub mod extra;
pub mod matches;
pub mod pair;
pub mod receipts;
pub mod tampering;

#[cfg(test)]
mod support;
