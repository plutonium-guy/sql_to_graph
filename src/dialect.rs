use crate::types::SqlDialect;
use sqlparser::dialect::{
    Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
};

pub fn to_sqlparser_dialect(dialect: &SqlDialect) -> Box<dyn Dialect> {
    match dialect {
        SqlDialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
        SqlDialect::MySQL => Box::new(MySqlDialect {}),
        SqlDialect::SQLite => Box::new(SQLiteDialect {}),
        SqlDialect::Generic => Box::new(GenericDialect {}),
    }
}

pub fn detect_dialect_from_url(url: &str) -> SqlDialect {
    let lower = url.to_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        SqlDialect::PostgreSQL
    } else if lower.starts_with("mysql://") || lower.starts_with("mariadb://") {
        SqlDialect::MySQL
    } else if lower.starts_with("sqlite://") || lower.starts_with("sqlite:") {
        SqlDialect::SQLite
    } else {
        SqlDialect::Generic
    }
}
