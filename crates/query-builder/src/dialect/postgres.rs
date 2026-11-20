use super::QueryDialect;
use model::core::types::Type;

#[derive(Debug, Clone)]
pub struct Postgres;

impl QueryDialect for Postgres {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#""{ident}""#)
    }

    fn placeholder(&self, index: usize) -> String {
        // PostgreSQL uses $1, $2, etc.
        format!("${}", index + 1)
    }

    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String {
        use model::core::types::{FloatSize, GeomKind, IntSize, IntervalFields};

        match data_type {
            Type::Int {
                bits,
                auto_increment,
                ..
            } => {
                if *auto_increment {
                    match bits {
                        IntSize::I8 | IntSize::I16 => "smallserial".to_string(),
                        IntSize::I24 | IntSize::I32 => "serial".to_string(),
                        IntSize::I64 => "bigserial".to_string(),
                    }
                } else {
                    match bits {
                        IntSize::I8 | IntSize::I16 => "smallint".to_string(),
                        IntSize::I24 | IntSize::I32 => "integer".to_string(),
                        IntSize::I64 => "bigint".to_string(),
                    }
                }
            }
            Type::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("numeric({p},{s})"),
                (Some(p), None) => format!("numeric({p})"),
                _ => "numeric".to_string(),
            },
            Type::Float { bits } => match bits {
                FloatSize::F32 => "real".to_string(),
                FloatSize::F64 => "double precision".to_string(),
            },
            Type::Char { length, .. } => match length.or(max_length) {
                Some(l) => format!("char({l})"),
                None => "char".to_string(),
            },
            Type::Varchar { length, .. } => match length.or(max_length) {
                Some(l) => format!("varchar({l})"),
                None => "varchar".to_string(),
            },
            Type::Text { .. } => "text".to_string(),
            Type::Binary { .. } | Type::Varbinary { .. } | Type::Blob { .. } => "bytea".to_string(),
            Type::Date => "date".to_string(),
            Type::Time { precision, with_tz } => {
                let base = if *with_tz {
                    "time with time zone"
                } else {
                    "time"
                };
                match precision {
                    Some(p) => format!("{base}({p})"),
                    None => base.to_string(),
                }
            }
            Type::Timestamp { precision, with_tz } => {
                let base = if *with_tz {
                    "timestamp with time zone"
                } else {
                    "timestamp"
                };
                match precision {
                    Some(p) => format!("{base}({p})"),
                    None => base.to_string(),
                }
            }
            Type::Interval { fields } => match fields {
                Some(IntervalFields::Year) => "interval year".to_string(),
                Some(IntervalFields::Month) => "interval month".to_string(),
                Some(IntervalFields::Day) => "interval day".to_string(),
                Some(IntervalFields::Hour) => "interval hour".to_string(),
                Some(IntervalFields::Minute) => "interval minute".to_string(),
                Some(IntervalFields::Second) => "interval second".to_string(),
                Some(IntervalFields::YearMonth) => "interval year to month".to_string(),
                Some(IntervalFields::DayTime) => "interval day to second".to_string(),
                Some(IntervalFields::Full) | None => "interval".to_string(),
            },
            Type::Year => "smallint".to_string(), // No YEAR in PostgreSQL
            Type::Boolean => "boolean".to_string(),
            Type::Uuid => "uuid".to_string(),
            Type::Json { binary } => {
                if *binary {
                    "jsonb".to_string()
                } else {
                    "json".to_string()
                }
            }
            Type::Bit { length } => match length {
                Some(l) => format!("bit({l})"),
                None => "bit".to_string(),
            },
            Type::Array { element } => {
                format!("{}[]", self.format_type(element, None))
            }
            Type::Enum { name, .. } => name.clone(),
            Type::Set { .. } => "text[]".to_string(),
            Type::Geometry { kind, .. } => match kind {
                Some(GeomKind::Point) => "point".to_string(),
                Some(GeomKind::LineString) => "path".to_string(),
                Some(GeomKind::Polygon) => "polygon".to_string(),
                _ => "geometry".to_string(),
            },
            Type::Inet => "inet".to_string(),
            Type::Cidr => "cidr".to_string(),
            Type::MacAddr => "macaddr".to_string(),
            Type::Composite { name, .. } => name.clone(),
            Type::Domain { name, .. } => name.clone(),
            Type::Unknown { fallback_ddl, .. } => fallback_ddl.clone(),
        }
    }

    fn name(&self) -> String {
        "PostgreSQL".into()
    }

    fn is_integer_type(&self, data_type: &str) -> bool {
        matches!(
            data_type.trim().to_lowercase().as_str(),
            "int"
                | "integer"
                | "bigint"
                | "smallint"
                | "int2"
                | "int4"
                | "int8"
                | "serial"
                | "bigserial"
                | "smallserial"
        )
    }

    fn key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        if keys_batch == 0 || key_columns.is_empty() {
            return String::new();
        }

        let select_clause = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("v.c{} AS {}", i + 1, self.quote_identifier(col_name)))
            .collect::<Vec<_>>()
            .join(", ");

        let value_columns: String = (1..=key_columns.len())
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        let join_conditions = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("t.{} = v.c{}", self.quote_identifier(col_name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let mut placeholder_idx = 1;
        let placeholders: String = (0..keys_batch)
            .map(|_| {
                let p = (0..key_columns.len())
                    .map(|_| {
                        let p_str = format!("${placeholder_idx}");
                        placeholder_idx += 1;
                        p_str
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({p})")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} FROM (VALUES {}) AS v({}) INNER JOIN {} AS t ON {}",
            select_clause,
            placeholders,
            value_columns,
            self.quote_identifier(table_name),
            join_conditions
        )
    }

    fn drop_primary_key(&self, table: &str) -> String {
        // PostgreSQL has no `DROP PRIMARY KEY`; the constraint must be dropped by
        // name. Look it up in the catalog and drop it if present.
        let quoted = self.quote_identifier(table);
        format!(
            "DO $$ DECLARE c text; BEGIN \
             SELECT conname INTO c FROM pg_constraint \
             WHERE conrelid = '{table}'::regclass AND contype = 'p'; \
             IF c IS NOT NULL THEN EXECUTE 'ALTER TABLE {quoted} DROP CONSTRAINT ' || quote_ident(c); \
             END IF; END $$;"
        )
    }

    fn drop_foreign_key(&self, table: &str, constraint: &str) -> String {
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
            self.quote_identifier(table),
            self.quote_identifier(constraint)
        )
    }

    fn random_fn(&self) -> &'static str {
        "RANDOM()"
    }

    fn supports_enums(&self) -> bool {
        true
    }

    fn cast_null_literals(&self) -> bool {
        true
    }

    fn values_row_keyword(&self) -> &'static str {
        ""
    }

    fn int_div_operator(&self) -> &'static str {
        " / "
    }
}
