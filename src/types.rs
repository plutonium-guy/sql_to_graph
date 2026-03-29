use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass(eq)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SqlDialect {
    PostgreSQL,
    MySQL,
    SQLite,
    Generic,
}

#[pyclass(eq)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChartType {
    Bar,
    HorizontalBar,
    StackedBar,
    Line,
    Area,
    Pie,
    Donut,
    Scatter,
    Histogram,
    Heatmap,
}

#[pyclass(eq)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OutputFormat {
    Html,
    Png,
    Jpg,
    Svg,
}

#[pyclass(eq)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnCategory {
    Numeric,
    Categorical,
    Temporal,
    Boolean,
    Unknown,
}

// ─── Schema ─────────────────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
    pub table_count: usize,
}

#[pymethods]
impl SchemaInfo {
    #[new]
    pub fn new(name: String, table_count: usize) -> Self {
        Self { name, table_count }
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaInfo(name='{}', tables={})",
            self.name, self.table_count
        )
    }
}

// ─── Column / Table Metadata ────────────────────────────────────────────────

#[pyclass(get_all, set_all)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[pymethods]
impl ColumnInfo {
    #[new]
    pub fn new(name: String, data_type: String, is_nullable: bool) -> Self {
        Self {
            name,
            data_type,
            is_nullable,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ColumnInfo(name='{}', data_type='{}', nullable={})",
            self.name, self.data_type, self.is_nullable
        )
    }
}

#[pyclass(get_all, set_all)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub row_count_estimate: Option<i64>,
}

#[pymethods]
impl TableMetadata {
    #[new]
    #[pyo3(signature = (table_name, columns, schema_name=None, row_count_estimate=None))]
    pub fn new(
        table_name: String,
        columns: Vec<ColumnInfo>,
        schema_name: Option<String>,
        row_count_estimate: Option<i64>,
    ) -> Self {
        Self {
            schema_name,
            table_name,
            columns,
            row_count_estimate,
        }
    }

    pub fn qualified_name(&self) -> String {
        match &self.schema_name {
            Some(s) => format!("{}.{}", s, self.table_name),
            None => self.table_name.clone(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "TableMetadata(schema={:?}, table='{}', columns={}, rows≈{:?})",
            self.schema_name,
            self.table_name,
            self.columns.len(),
            self.row_count_estimate
        )
    }
}

// ─── Cell Values ────────────────────────────────────────────────────────────

#[pyclass]
#[derive(Clone, Debug)]
pub struct CellValue {
    pub value: CellValueInner,
}

#[derive(Clone, Debug)]
pub enum CellValueInner {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

#[pymethods]
impl CellValue {
    pub fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.value {
            CellValueInner::Null => py.None(),
            CellValueInner::Bool(b) => b.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
            CellValueInner::Int(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
            CellValueInner::Float(f) => f.into_pyobject(py).unwrap().into_any().unbind(),
            CellValueInner::Text(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        }
    }

    fn __repr__(&self) -> String {
        match &self.value {
            CellValueInner::Null => "None".to_string(),
            CellValueInner::Bool(b) => b.to_string(),
            CellValueInner::Int(i) => i.to_string(),
            CellValueInner::Float(f) => f.to_string(),
            CellValueInner::Text(s) => format!("'{}'", s),
        }
    }
}

// ─── Query Result ───────────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
    pub row_count: usize,
    pub execution_time_ms: f64,
    pub total_row_count: Option<usize>,
    pub has_more: bool,
}

#[pymethods]
impl QueryResult {
    #[new]
    #[pyo3(signature = (columns, rows, row_count, execution_time_ms, total_row_count=None, has_more=false))]
    pub fn new(
        columns: Vec<String>,
        rows: Vec<Vec<CellValue>>,
        row_count: usize,
        execution_time_ms: f64,
        total_row_count: Option<usize>,
        has_more: bool,
    ) -> Self {
        Self {
            columns,
            rows,
            row_count,
            execution_time_ms,
            total_row_count,
            has_more,
        }
    }

    fn to_dicts(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let mut result = Vec::new();
        for row in &self.rows {
            let dict = pyo3::types::PyDict::new(py);
            for (col, cell) in self.columns.iter().zip(row.iter()) {
                dict.set_item(col, cell.to_python(py))?;
            }
            result.push(dict.into_any().unbind());
        }
        Ok(result)
    }

    fn __repr__(&self) -> String {
        let more = if self.has_more { ", has_more" } else { "" };
        format!(
            "QueryResult(columns={}, rows={}, time={:.2}ms{})",
            self.columns.len(),
            self.row_count,
            self.execution_time_ms,
            more
        )
    }
}

// ─── Chart Config / Output ──────────────────────────────────────────────────

#[pyclass(get_all, set_all)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChartConfig {
    pub chart_type: ChartType,
    pub title: Option<String>,
    pub x_column: String,
    pub y_column: String,
    pub z_column: Option<String>,
    pub bin_count: u32,
    pub width: u32,
    pub height: u32,
    pub output_format: OutputFormat,
}

#[pymethods]
impl ChartConfig {
    #[new]
    #[pyo3(signature = (chart_type, x_column, y_column, title=None, z_column=None, bin_count=10, width=800, height=600, output_format=OutputFormat::Html))]
    pub fn new(
        chart_type: ChartType,
        x_column: String,
        y_column: String,
        title: Option<String>,
        z_column: Option<String>,
        bin_count: u32,
        width: u32,
        height: u32,
        output_format: OutputFormat,
    ) -> Self {
        Self {
            chart_type,
            title,
            x_column,
            y_column,
            z_column,
            bin_count,
            width,
            height,
            output_format,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ChartOutput {
    pub format: OutputFormat,
    pub data: Vec<u8>,
    pub mime_type: String,
}

#[pymethods]
impl ChartOutput {
    pub fn save_to_file(&self, path: &str) -> PyResult<()> {
        std::fs::write(path, &self.data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "ChartOutput(format={:?}, size={} bytes)",
            self.format,
            self.data.len()
        )
    }
}

// ─── Correction Context / Parse Result ──────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct CorrectionContext {
    pub original_sql: String,
    pub parse_errors: Vec<String>,
    pub available_tables: Vec<TableMetadata>,
    pub suggested_prompt: String,
}

#[pymethods]
impl CorrectionContext {
    fn __repr__(&self) -> String {
        format!(
            "CorrectionContext(errors={}, tables={})",
            self.parse_errors.len(),
            self.available_tables.len()
        )
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ParseResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub statement_count: usize,
    pub normalized_sql: Option<String>,
}

#[pymethods]
impl ParseResult {
    fn __repr__(&self) -> String {
        format!(
            "ParseResult(valid={}, errors={}, statements={})",
            self.is_valid,
            self.errors.len(),
            self.statement_count
        )
    }
}

// ─── Statistics ─────────────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub column_name: String,
    pub category: ColumnCategory,
    pub null_count: usize,
    pub distinct_count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub stddev: Option<f64>,
    pub top_values: Vec<(String, usize)>,
}

#[pymethods]
impl ColumnStats {
    fn __repr__(&self) -> String {
        format!(
            "ColumnStats(col='{}', category={:?}, nulls={}, distinct={})",
            self.column_name, self.category, self.null_count, self.distinct_count
        )
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ResultSummary {
    pub row_count: usize,
    pub column_stats: Vec<ColumnStats>,
    pub warnings: Vec<String>,
}

#[pymethods]
impl ResultSummary {
    fn __repr__(&self) -> String {
        format!(
            "ResultSummary(rows={}, columns={}, warnings={})",
            self.row_count,
            self.column_stats.len(),
            self.warnings.len()
        )
    }
}

// ─── Chart Suggestion ───────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ChartSuggestion {
    pub chart_type: ChartType,
    pub x_column: String,
    pub y_column: String,
    pub z_column: Option<String>,
    pub title: String,
    pub confidence: f64,
    pub reasoning: String,
}

#[pymethods]
impl ChartSuggestion {
    fn __repr__(&self) -> String {
        format!(
            "ChartSuggestion({:?}, x='{}', y='{}', conf={:.0}%)",
            self.chart_type,
            self.x_column,
            self.y_column,
            self.confidence * 100.0
        )
    }
}

// ─── Enriched Error ─────────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct EnrichedError {
    pub error_type: String,
    pub message: String,
    pub original_sql: String,
    pub available_tables: Vec<String>,
    pub available_columns: Vec<String>,
    pub suggestions: Vec<String>,
    pub schema_context: String,
}

#[pymethods]
impl EnrichedError {
    fn __repr__(&self) -> String {
        format!(
            "EnrichedError(type='{}', suggestions={})",
            self.error_type,
            self.suggestions.len()
        )
    }
}
