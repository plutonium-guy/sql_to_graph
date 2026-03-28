use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlToGraphError {
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Chart rendering error: {0}")]
    ChartError(String),

    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    #[error("Optimization error: {0}")]
    OptimizationError(String),

    #[error("Image conversion error: {0}")]
    ImageError(String),
}

impl From<SqlToGraphError> for PyErr {
    fn from(err: SqlToGraphError) -> PyErr {
        match &err {
            SqlToGraphError::ParseError(_) | SqlToGraphError::ConfigError(_) => {
                PyValueError::new_err(err.to_string())
            }
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}

impl From<sqlx::Error> for SqlToGraphError {
    fn from(err: sqlx::Error) -> Self {
        SqlToGraphError::DatabaseError(err.to_string())
    }
}

impl From<sqlparser::parser::ParserError> for SqlToGraphError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        SqlToGraphError::ParseError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SqlToGraphError>;
