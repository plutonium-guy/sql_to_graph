use std::sync::Arc;
use std::time::Instant;

use pyo3::prelude::*;
use sqlx::any::AnyRow;
use sqlx::pool::PoolOptions;
use sqlx::{AnyPool, Column, Executor as _, Row, TypeInfo};
use tokio::sync::Mutex;

use crate::dialect::detect_dialect_from_url;
use crate::error::{Result, SqlToGraphError};
use crate::metadata::{fetch_metadata, fetch_schemas, fetch_table_metadata};
use crate::types::{CellValue, CellValueInner, EnrichedError, QueryResult, SqlDialect};

#[pyclass]
pub struct Connection {
    url: String,
    dialect: SqlDialect,
    pool: Arc<Mutex<Option<AnyPool>>>,
    read_only: bool,
    default_schema: Option<String>,
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (connection_string, dialect=None, read_only=false, schema=None))]
    pub fn new(
        connection_string: String,
        dialect: Option<SqlDialect>,
        read_only: bool,
        schema: Option<String>,
    ) -> Self {
        let detected = dialect.unwrap_or_else(|| detect_dialect_from_url(&connection_string));
        Self {
            url: connection_string,
            dialect: detected,
            pool: Arc::new(Mutex::new(None)),
            read_only,
            default_schema: schema,
        }
    }

    pub fn connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let url = self.url.clone();
        let pool_ref = self.pool.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            {
                let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
                if guard.is_some() {
                    return Ok(());
                }
            }

            sqlx::any::install_default_drivers();
            let pool = PoolOptions::new()
                .max_connections(5)
                .connect(&url)
                .await
                .map_err(|e| SqlToGraphError::ConnectionError(e.to_string()))?;

            let mut guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            *guard = Some(pool);
            Ok(())
        })
    }

    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            if let Some(p) = guard.take() {
                AnyPool::close(&p).await;
            }
            Ok(())
        })
    }

    pub fn list_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let dialect = self.dialect.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;
            let schemas = fetch_schemas(pool, &dialect).await?;
            Ok(schemas)
        })
    }

    #[pyo3(signature = (schema=None))]
    pub fn get_metadata<'py>(
        &self,
        py: Python<'py>,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let dialect = self.dialect.clone();
        let schema = schema.or(self.default_schema.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;
            let metadata = fetch_metadata(pool, &dialect, schema.as_deref()).await?;
            Ok(metadata)
        })
    }

    #[pyo3(signature = (table, schema=None))]
    pub fn describe_table<'py>(
        &self,
        py: Python<'py>,
        table: String,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let dialect = self.dialect.clone();
        let schema = schema.or(self.default_schema.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;
            let meta = fetch_table_metadata(pool, &dialect, &table, schema.as_deref()).await?;
            Ok(meta)
        })
    }

    #[pyo3(signature = (table, n=10, schema=None))]
    pub fn sample_table<'py>(
        &self,
        py: Python<'py>,
        table: String,
        n: u32,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let dialect = self.dialect.clone();
        let schema = schema.or(self.default_schema.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;

            let qualified = match &schema {
                Some(s) => format!(
                    "{}.{}",
                    quote_ident(s, &dialect),
                    quote_ident(&table, &dialect)
                ),
                None => quote_ident(&table, &dialect),
            };

            // For Postgres, cast all columns to text to avoid Any driver type issues
            let sql = if dialect == SqlDialect::PostgreSQL {
                // Get column list and cast each to text
                let meta = fetch_table_metadata(pool, &dialect, &table, schema.as_deref()).await?;
                let cols: Vec<String> = meta
                    .columns
                    .iter()
                    .map(|c| {
                        format!(
                            "{}::TEXT as {}",
                            quote_ident(&c.name, &dialect),
                            quote_ident(&c.name, &dialect)
                        )
                    })
                    .collect();
                format!("SELECT {} FROM {} LIMIT {}", cols.join(", "), qualified, n)
            } else {
                format!("SELECT * FROM {} LIMIT {}", qualified, n)
            };

            let result = execute_query_internal(pool, &sql).await?;
            Ok(result)
        })
    }

    pub fn execute<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let read_only = self.read_only;
        let dialect = self.dialect.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if read_only {
                validate_read_only(&sql, &dialect)?;
            }
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;
            let result = execute_query_internal(pool, &sql).await?;
            Ok(result)
        })
    }

    #[pyo3(signature = (sql, limit=1000, offset=0))]
    pub fn execute_paginated<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        limit: u32,
        offset: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let read_only = self.read_only;
        let dialect = self.dialect.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if read_only {
                validate_read_only(&sql, &dialect)?;
            }
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;

            let paginated = format!(
                "SELECT * FROM ({}) AS _paged LIMIT {} OFFSET {}",
                sql.trim().trim_end_matches(';'),
                limit,
                offset
            );
            let mut result = execute_query_internal(pool, &paginated).await?;

            // Get total count
            let count_sql = format!(
                "SELECT COUNT(*) as cnt FROM ({}) AS _cnt",
                sql.trim().trim_end_matches(';')
            );
            if let Ok(count_result) = execute_query_internal(pool, &count_sql).await {
                if let Some(row) = count_result.rows.first() {
                    if let Some(cell) = row.first() {
                        if let CellValueInner::Int(n) = cell.value {
                            let total = n as usize;
                            result.total_row_count = Some(total);
                            result.has_more = (offset as usize + result.row_count) < total;
                        }
                    }
                }
            }

            Ok(result)
        })
    }

    #[pyo3(signature = (sql, schema=None))]
    pub fn execute_with_context<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool_ref = self.pool.clone();
        let read_only = self.read_only;
        let dialect = self.dialect.clone();
        let schema = schema.or(self.default_schema.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if read_only {
                validate_read_only(&sql, &dialect)?;
            }
            let guard: tokio::sync::MutexGuard<'_, Option<AnyPool>> = pool_ref.lock().await;
            let pool = guard
                .as_ref()
                .ok_or(SqlToGraphError::ConnectionError("Not connected".into()))?;

            match execute_query_internal(pool, &sql).await {
                Ok(result) => Ok(result),
                Err(e) => {
                    // Fetch metadata for error enrichment
                    let metadata = fetch_metadata(pool, &dialect, schema.as_deref())
                        .await
                        .unwrap_or_default();

                    let available_tables: Vec<String> =
                        metadata.iter().map(|t| t.table_name.clone()).collect();
                    let available_columns: Vec<String> = metadata
                        .iter()
                        .flat_map(|t| {
                            t.columns
                                .iter()
                                .map(|c| format!("{}.{}", t.table_name, c.name))
                        })
                        .collect();

                    // Generate fuzzy suggestions
                    let error_msg = e.to_string();
                    let mut suggestions = Vec::new();

                    // Try to find similar table names mentioned in error
                    for table in &available_tables {
                        if fuzzy_match(&error_msg, table) {
                            suggestions.push(format!("Did you mean table '{}'?", table));
                        }
                    }

                    // Build schema context for LLM
                    let mut schema_ctx = String::new();
                    for t in &metadata {
                        schema_ctx.push_str(&format!("Table: {}\n", t.table_name));
                        for c in &t.columns {
                            schema_ctx.push_str(&format!("  {} ({})\n", c.name, c.data_type));
                        }
                    }

                    let enriched = EnrichedError {
                        error_type: "execution_error".into(),
                        message: error_msg,
                        original_sql: sql,
                        available_tables,
                        available_columns,
                        suggestions,
                        schema_context: schema_ctx,
                    };

                    Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "{}",
                        serde_json::to_string(&EnrichedErrorSerializable::from(&enriched))
                            .unwrap_or(enriched.message.clone())
                    )))
                }
            }
        })
    }

    #[getter]
    pub fn dialect(&self) -> SqlDialect {
        self.dialect.clone()
    }

    #[getter]
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    #[getter]
    pub fn schema(&self) -> Option<String> {
        self.default_schema.clone()
    }

    fn __repr__(&self) -> String {
        let display_url = if let Some(at_pos) = self.url.find('@') {
            if let Some(proto_end) = self.url.find("://") {
                format!(
                    "{}://***@{}",
                    &self.url[..proto_end],
                    &self.url[at_pos + 1..]
                )
            } else {
                "***".to_string()
            }
        } else {
            self.url.clone()
        };
        let ro = if self.read_only { ", read_only" } else { "" };
        format!(
            "Connection(url='{}', dialect={:?}{})",
            display_url, self.dialect, ro
        )
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn quote_ident(name: &str, dialect: &SqlDialect) -> String {
    match dialect {
        SqlDialect::MySQL => format!("`{}`", name.replace('`', "``")),
        _ => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

fn validate_read_only(sql: &str, dialect: &SqlDialect) -> Result<()> {
    use crate::dialect::to_sqlparser_dialect;
    use sqlparser::parser::Parser;

    let d = to_sqlparser_dialect(dialect);
    let statements = Parser::parse_sql(d.as_ref(), sql)
        .map_err(|e| SqlToGraphError::ParseError(e.to_string()))?;

    for stmt in &statements {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(_)
            | Statement::ExplainTable { .. }
            | Statement::Explain { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowColumns { .. } => {}
            _ => {
                return Err(SqlToGraphError::ConfigError(format!(
                    "Read-only mode: '{}' statements are not allowed",
                    stmt.to_string()
                        .split_whitespace()
                        .next()
                        .unwrap_or("Unknown")
                )));
            }
        }
    }
    Ok(())
}

fn fuzzy_match(haystack: &str, needle: &str) -> bool {
    let h = haystack.to_lowercase();
    let n = needle.to_lowercase();
    if h.contains(&n) {
        return false; // exact match, no suggestion needed
    }
    // Simple edit distance check for short names
    if n.len() < 3 {
        return false;
    }
    // Check if any word in haystack is within edit distance 2 of needle
    for word in h.split_whitespace() {
        let word_clean = word.trim_matches(|c: char| !c.is_alphanumeric());
        if levenshtein(word_clean, &n) <= 2 {
            return true;
        }
    }
    false
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();
    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev: Vec<usize> = (0..=b_len).collect();
    let mut curr = vec![0; b_len + 1];

    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_len]
}

pub async fn execute_query_internal(pool: &AnyPool, sql: &str) -> Result<QueryResult> {
    let start = Instant::now();
    let rows: Vec<AnyRow> = sqlx::query(sql).fetch_all(pool).await?;
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;

    if rows.is_empty() {
        let describe = pool.describe(sql).await?;
        let columns = describe
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        return Ok(QueryResult {
            columns,
            rows: vec![],
            row_count: 0,
            execution_time_ms: elapsed,
            total_row_count: None,
            has_more: false,
        });
    }

    let columns: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    let mut result_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut cells = Vec::with_capacity(columns.len());
        for (i, col) in row.columns().iter().enumerate() {
            let cell = extract_cell_value(row, i, col.type_info().name());
            cells.push(cell);
        }
        result_rows.push(cells);
    }

    let row_count = result_rows.len();
    Ok(QueryResult {
        columns,
        rows: result_rows,
        row_count,
        execution_time_ms: elapsed,
        total_row_count: None,
        has_more: false,
    })
}

fn extract_cell_value(row: &AnyRow, idx: usize, type_name: &str) -> CellValue {
    let upper = type_name.to_uppercase();

    // Try typed extraction first, fall back gracefully for unsupported Any driver types
    let value = if upper.contains("INT") || upper.contains("SERIAL") {
        row.try_get::<i64, _>(idx)
            .map(CellValueInner::Int)
            .or_else(|_| {
                row.try_get::<i32, _>(idx)
                    .map(|v| CellValueInner::Int(v as i64))
            })
            .or_else(|_| row.try_get::<String, _>(idx).map(CellValueInner::Text))
            .unwrap_or(CellValueInner::Null)
    } else if upper.contains("FLOAT")
        || upper.contains("DOUBLE")
        || upper.contains("REAL")
        || upper.contains("NUMERIC")
        || upper.contains("DECIMAL")
    {
        row.try_get::<f64, _>(idx)
            .map(CellValueInner::Float)
            .or_else(|_| {
                row.try_get::<f32, _>(idx)
                    .map(|v| CellValueInner::Float(v as f64))
            })
            .or_else(|_| row.try_get::<String, _>(idx).map(CellValueInner::Text))
            .unwrap_or(CellValueInner::Null)
    } else if upper.contains("BOOL") {
        row.try_get::<bool, _>(idx)
            .map(CellValueInner::Bool)
            .or_else(|_| row.try_get::<String, _>(idx).map(CellValueInner::Text))
            .unwrap_or(CellValueInner::Null)
    } else {
        // For DATE, TIMESTAMP, UUID, JSONB, and other types the Any driver
        // may not support natively — always try String first
        row.try_get::<String, _>(idx)
            .map(CellValueInner::Text)
            .or_else(|_| row.try_get::<i64, _>(idx).map(CellValueInner::Int))
            .or_else(|_| row.try_get::<f64, _>(idx).map(CellValueInner::Float))
            .or_else(|_| row.try_get::<bool, _>(idx).map(CellValueInner::Bool))
            .unwrap_or(CellValueInner::Null)
    };

    CellValue { value }
}

// Serializable version of EnrichedError for JSON encoding in error messages
#[derive(serde::Serialize)]
struct EnrichedErrorSerializable {
    error_type: String,
    message: String,
    original_sql: String,
    available_tables: Vec<String>,
    available_columns: Vec<String>,
    suggestions: Vec<String>,
    schema_context: String,
}

impl From<&EnrichedError> for EnrichedErrorSerializable {
    fn from(e: &EnrichedError) -> Self {
        Self {
            error_type: e.error_type.clone(),
            message: e.message.clone(),
            original_sql: e.original_sql.clone(),
            available_tables: e.available_tables.clone(),
            available_columns: e.available_columns.clone(),
            suggestions: e.suggestions.clone(),
            schema_context: e.schema_context.clone(),
        }
    }
}
