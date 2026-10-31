pub const PAIR_PREFIX: &str = "@pair/";

/// The pipeline name a pair's scratch row hashes and reports are kept under.
pub fn pair_pipeline(name: &str) -> String {
    format!("{PAIR_PREFIX}{name}")
}
