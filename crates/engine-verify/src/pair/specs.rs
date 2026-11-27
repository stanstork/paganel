use crate::{HashSpec, error::VerifyError};
use connectors::sql::metadata::table::TableMetadata;
use engine_core::plan::pair::target_column;
use model::integrity::algorithm::HashAlgorithm;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SideSpecs {
    pub source: HashSpec,
    pub target: HashSpec,
}

/// Resolve which columns both sides hash, and in what order.
pub(crate) fn side_specs(
    name: &str,
    map: &BTreeMap<String, String>,
    source_ignore: &[String],
    target_ignore: &[String],
    key: Option<&[String]>,
    source: &TableMetadata,
    target: &TableMetadata,
) -> Result<SideSpecs, VerifyError> {
    let err = |msg: String| VerifyError::InitializationError(format!("pair \"{name}\": {msg}"));

    let find_col = |meta: &TableMetadata, column: &str| {
        meta.columns
            .keys()
            .find(|c| c.eq_ignore_ascii_case(column))
            .cloned()
    };

    let is_ignored =
        |list: &[String], column: &str| list.iter().any(|i| i.eq_ignore_ascii_case(column));

    // Validate table metadata presence
    for (side, meta) in [("source", source), ("target", target)] {
        if meta.columns.is_empty() {
            return Err(err(format!("{side} table '{}' not found", meta.name)));
        }
    }

    // Validate mapped columns exist
    for (s, t) in map {
        if find_col(source, s).is_none() {
            return Err(err(format!("map: '{}' has no column {s}", source.name)));
        }
        if find_col(target, t).is_none() {
            return Err(err(format!("map: '{}' has no column {t}", target.name)));
        }
    }

    // Validate ignored columns exist
    for (meta, list) in [(source, source_ignore), (target, target_ignore)] {
        if let Some(i) = list.iter().find(|i| find_col(meta, i).is_none()) {
            return Err(err(format!("ignore: '{}' has no column {i}", meta.name)));
        }
    }

    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut source_only = Vec::new();
    let mut half_ignored = Vec::new();

    for column in source
        .columns
        .keys()
        .filter(|c| !is_ignored(source_ignore, c))
    {
        match find_col(target, &target_column(map, column)) {
            Some(t) if is_ignored(target_ignore, &t) => half_ignored.push(column.clone()),
            Some(t) => pairs.push((column.clone(), t)),
            None => source_only.push(column.clone()),
        }
    }

    let source_name = |t: &str| {
        map.iter()
            .find(|(_, mapped)| mapped.eq_ignore_ascii_case(t))
            .map_or_else(|| t.to_string(), |(s, _)| s.clone())
    };

    let mut target_only = Vec::new();
    for column in target
        .columns
        .keys()
        .filter(|t| !is_ignored(target_ignore, t))
    {
        if pairs.iter().any(|(_, p)| p.eq_ignore_ascii_case(column)) {
            continue;
        }
        match find_col(source, &source_name(column)) {
            Some(s) if is_ignored(source_ignore, &s) => half_ignored.push(column.clone()),
            _ => target_only.push(column.clone()),
        }
    }

    source_only.sort();
    target_only.sort();
    half_ignored.sort();

    let mut problems = Vec::new();
    if !source_only.is_empty() {
        problems.push(format!(
            "source has {} not in target",
            source_only.join(", ")
        ));
    }
    if !target_only.is_empty() {
        problems.push(format!(
            "target has {} not in source",
            target_only.join(", ")
        ));
    }
    if !problems.is_empty() {
        return Err(err(format!(
            "{}; add them to that side's ignore or to map",
            problems.join("; ")
        )));
    }
    if !half_ignored.is_empty() {
        return Err(err(format!(
            "{} ignored on one side only; ignore on both sides or neither",
            half_ignored.join(", ")
        )));
    }

    pairs.sort_by_key(|(s, _)| s.to_ascii_lowercase());

    let key_source: Vec<String> = match key {
        Some(k_list) => k_list.to_vec(),
        None if !target.primary_keys.is_empty() => target
            .primary_keys
            .iter()
            .map(|t| {
                pairs
                    .iter()
                    .find(|(_, p)| p.eq_ignore_ascii_case(t))
                    .map(|(s, _)| s.clone())
                    .ok_or_else(|| err(format!("target key column {t} is ignored")))
            })
            .collect::<Result<_, _>>()?,
        None if !source.primary_keys.is_empty() => source.primary_keys.clone(),
        None => {
            return Err(err(
                "neither table has a primary key; set key = [\"column\", ...]".into(),
            ));
        }
    };

    let key_pairs: Vec<&(String, String)> = key_source
        .iter()
        .map(|k| {
            pairs
                .iter()
                .find(|(s, _)| s.eq_ignore_ascii_case(k))
                .ok_or_else(|| err(format!("key column {k} is not compared on both sides")))
        })
        .collect::<Result<_, _>>()?;

    let build_side_spec = |pick: fn(&(String, String)) -> &String| HashSpec {
        column_order: pairs.iter().map(|p| pick(p).clone()).collect(),
        key_columns: key_pairs.iter().map(|p| pick(p).clone()).collect(),
        algorithm: HashAlgorithm::default(),
    };

    Ok(SideSpecs {
        source: build_side_spec(|(s, _)| s),
        target: build_side_spec(|(_, t)| t),
    })
}
