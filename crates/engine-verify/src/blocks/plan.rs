use crate::pair::SideSpecs;
use connectors::sql::{
    checksum::{self, ColumnKind, MySqlNormal, NormalColumn, NormalSql, PostgresNormal},
    metadata::table::TableMetadata,
};
use engine_core::drivers::DriverRef;
use model::integrity::normal::Family;

/// Each side's compared columns with their kinds, and their families.
pub(super) struct ColumnPlan {
    pub(super) source: Vec<NormalColumn>,
    pub(super) target: Vec<NormalColumn>,
    pub(super) families: Vec<Family>,
}

/// Classify every compared column.
pub(super) fn column_plan(
    specs: &SideSpecs,
    source_meta: &TableMetadata,
    target_meta: &TableMetadata,
    source_sql: &dyn NormalSql,
    target_sql: &dyn NormalSql,
) -> Result<ColumnPlan, String> {
    let len = specs.source.column_order.len();
    let mut source = Vec::with_capacity(len);
    let mut target = Vec::with_capacity(len);
    let mut families = Vec::with_capacity(len);

    for (s, t) in specs
        .source
        .column_order
        .iter()
        .zip(&specs.target.column_order)
    {
        let (s_kind, s_type) = column_kind(source_meta, source_sql, s)?;
        let (t_kind, t_type) = column_kind(target_meta, target_sql, t)?;
        let family = checksum::family(s_kind, t_kind).ok_or_else(|| {
            format!(
                "column {s} ({s_type}) against {t} ({t_type}) is not supported in block mode; ignore it on both sides to use blocks"
            )
        })?;

        source.push(NormalColumn {
            name: s.clone(),
            kind: s_kind,
        });
        target.push(NormalColumn {
            name: t.clone(),
            kind: t_kind,
        });
        families.push(family);
    }

    let key =
        specs.source.key_columns.first().ok_or_else(|| {
            "table has no key columns; block mode requires an integer key".to_owned()
        })?;
    let key_index = specs
        .source
        .column_order
        .iter()
        .position(|c| c == key)
        .ok_or_else(|| format!("key column {key} is not compared"))?;

    if source[key_index].kind != ColumnKind::Int || target[key_index].kind != ColumnKind::Int {
        return Err(format!(
            "the first key column {key} is not an integer; block mode segments integer keys only"
        ));
    }

    Ok(ColumnPlan {
        source,
        target,
        families,
    })
}

fn column_kind<'a>(
    meta: &'a TableMetadata,
    sql: &dyn NormalSql,
    name: &str,
) -> Result<(ColumnKind, &'a str), String> {
    let column = meta
        .columns
        .get(name)
        .ok_or_else(|| format!("column {name} not found in '{}'", meta.name))?;
    let kind = sql.kind(column).ok_or_else(|| {
        format!(
            "column {name} ({}) is not supported in block mode; ignore it on both sides to use blocks",
            column.data_type
        )
    })?;
    Ok((kind, &column.data_type))
}

pub(super) fn normal_sql(driver: &DriverRef) -> &'static dyn NormalSql {
    match driver {
        DriverRef::MySql(_) => &MySqlNormal,
        DriverRef::Postgres(_) => &PostgresNormal,
    }
}
