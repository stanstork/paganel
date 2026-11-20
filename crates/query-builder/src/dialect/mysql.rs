use super::QueryDialect;
use model::core::types::Type;

#[derive(Debug, Clone)]
pub struct MySql;

impl QueryDialect for MySql {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#"`{ident}`"#)
    }

    fn placeholder(&self, _index: usize) -> String {
        // MySQL uses ?
        "?".into()
    }

    fn format_type(&self, data_type: &Type, max_length: Option<usize>) -> String {
        use model::core::types::{FloatSize, GeomKind, IntSize};

        match data_type {
            Type::Int {
                bits,
                unsigned,
                auto_increment,
            } => {
                let base = match bits {
                    IntSize::I8 => "TINYINT",
                    IntSize::I16 => "SMALLINT",
                    IntSize::I24 => "MEDIUMINT",
                    IntSize::I32 => "INT",
                    IntSize::I64 => "BIGINT",
                };
                let mut result = base.to_string();
                if *unsigned {
                    result.push_str(" UNSIGNED");
                }
                if *auto_increment {
                    result.push_str(" AUTO_INCREMENT");
                }
                result
            }
            Type::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("DECIMAL({p},{s})"),
                (Some(p), None) => format!("DECIMAL({p})"),
                _ => "DECIMAL".to_string(),
            },
            Type::Float { bits } => match bits {
                FloatSize::F32 => "FLOAT".to_string(),
                FloatSize::F64 => "DOUBLE".to_string(),
            },
            Type::Char { length, .. } => match length.or(max_length) {
                Some(l) => format!("CHAR({l})"),
                None => "CHAR".to_string(),
            },
            Type::Varchar { length, .. } => match length.or(max_length) {
                Some(l) => format!("VARCHAR({l})"),
                None => "VARCHAR(255)".to_string(),
            },
            Type::Text { .. } => "TEXT".to_string(),
            Type::Binary { length } => match length.or(max_length) {
                Some(l) => format!("BINARY({l})"),
                None => "BINARY".to_string(),
            },
            Type::Varbinary { length } => match length.or(max_length) {
                Some(l) => format!("VARBINARY({l})"),
                None => "VARBINARY(255)".to_string(),
            },
            Type::Blob { max_bytes } => match max_bytes {
                Some(b) if *b <= 255 => "TINYBLOB".to_string(),
                Some(b) if *b <= 65535 => "BLOB".to_string(),
                Some(b) if *b <= 16777215 => "MEDIUMBLOB".to_string(),
                _ => "LONGBLOB".to_string(),
            },
            Type::Date => "DATE".to_string(),
            Type::Time { precision, .. } => match precision {
                Some(p) => format!("TIME({p})"),
                None => "TIME".to_string(),
            },
            Type::Timestamp { precision, .. } => match precision {
                Some(p) => format!("DATETIME({p})"),
                None => "DATETIME".to_string(),
            },
            Type::Interval { .. } => "VARCHAR(255)".to_string(), // No interval in MySQL
            Type::Year => "YEAR".to_string(),
            Type::Boolean => "TINYINT(1)".to_string(),
            Type::Uuid => "CHAR(36)".to_string(),
            Type::Json { .. } => "JSON".to_string(),
            Type::Bit { length } => match length {
                Some(l) => format!("BIT({l})"),
                None => "BIT".to_string(),
            },
            Type::Array { .. } => "JSON".to_string(), // No native arrays in MySQL
            Type::Enum { values, .. } => {
                let vals = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("ENUM({vals})")
            }
            Type::Set { values } => {
                let vals = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("SET({vals})")
            }
            Type::Geometry { kind, .. } => match kind {
                Some(GeomKind::Point) => "POINT".to_string(),
                Some(GeomKind::LineString) => "LINESTRING".to_string(),
                Some(GeomKind::Polygon) => "POLYGON".to_string(),
                Some(GeomKind::MultiPoint) => "MULTIPOINT".to_string(),
                Some(GeomKind::MultiLineString) => "MULTILINESTRING".to_string(),
                Some(GeomKind::MultiPolygon) => "MULTIPOLYGON".to_string(),
                Some(GeomKind::GeometryCollection) => "GEOMETRYCOLLECTION".to_string(),
                None => "GEOMETRY".to_string(),
            },
            Type::Inet => "VARCHAR(45)".to_string(),
            Type::Cidr => "VARCHAR(45)".to_string(),
            Type::MacAddr => "VARCHAR(17)".to_string(),
            Type::Composite { .. } => "JSON".to_string(),
            Type::Domain { base_type, .. } => self.format_type(base_type, max_length),
            Type::Unknown { fallback_ddl, .. } => fallback_ddl.clone(),
        }
    }

    fn name(&self) -> String {
        "MySQL".into()
    }

    fn is_integer_type(&self, data_type: &str) -> bool {
        let dt = data_type.trim().to_lowercase();
        // Match the base integer types, allowing a display width like `int(11)`
        // but excluding `point`/`interval` types that merely contain "int".
        matches!(
            dt.as_str(),
            "int" | "integer" | "bigint" | "smallint" | "tinyint" | "mediumint"
        ) || dt.starts_with("int(")
            || dt.starts_with("integer(")
            || dt.starts_with("bigint(")
            || dt.starts_with("smallint(")
            || dt.starts_with("tinyint(")
            || dt.starts_with("mediumint(")
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

        let join_conditions = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("t.{} = v.c{}", self.quote_identifier(col_name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let derived = (0..keys_batch)
            .map(|row| {
                let cols = (0..key_columns.len())
                    .map(|c| {
                        if row == 0 {
                            format!("? AS c{}", c + 1)
                        } else {
                            "?".to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SELECT {cols}")
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        format!(
            "SELECT {} FROM ({}) AS v INNER JOIN {} AS t ON {}",
            select_clause,
            derived,
            self.quote_identifier(table_name),
            join_conditions
        )
    }

    fn drop_primary_key(&self, table: &str) -> String {
        format!(
            "ALTER TABLE {} DROP PRIMARY KEY",
            self.quote_identifier(table)
        )
    }

    fn drop_foreign_key(&self, table: &str, constraint: &str) -> String {
        format!(
            "ALTER TABLE {} DROP FOREIGN KEY {};",
            self.quote_identifier(table),
            self.quote_identifier(constraint)
        )
    }

    fn random_fn(&self) -> &'static str {
        "RAND()"
    }

    fn supports_index_if_not_exists(&self) -> bool {
        false
    }

    fn supports_index_concurrently(&self) -> bool {
        false
    }

    fn supports_partial_index(&self) -> bool {
        false
    }

    fn supports_index_nulls(&self) -> bool {
        false
    }

    fn index_method_before_cols(&self) -> bool {
        false
    }

    fn supports_index_prefix(&self) -> bool {
        true
    }

    fn auto_inc_requires_key(&self) -> bool {
        true
    }

    fn values_row_keyword(&self) -> &'static str {
        "ROW"
    }

    fn int_div_operator(&self) -> &'static str {
        " DIV "
    }
}
