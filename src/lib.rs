use pyo3::prelude::*;

mod agent;
mod chart;
mod dialect;
mod error;
mod executor;
mod export;
mod metadata;
mod optimizer;
mod parser;
mod renderer;
mod stats;
mod suggest;
mod types;

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Types
    m.add_class::<types::SqlDialect>()?;
    m.add_class::<types::ChartType>()?;
    m.add_class::<types::OutputFormat>()?;
    m.add_class::<types::ColumnCategory>()?;
    m.add_class::<types::SchemaInfo>()?;
    m.add_class::<types::ColumnInfo>()?;
    m.add_class::<types::TableMetadata>()?;
    m.add_class::<types::CellValue>()?;
    m.add_class::<types::QueryResult>()?;
    m.add_class::<types::ChartConfig>()?;
    m.add_class::<types::ChartOutput>()?;
    m.add_class::<types::CorrectionContext>()?;
    m.add_class::<types::ParseResult>()?;
    m.add_class::<types::ColumnStats>()?;
    m.add_class::<types::ResultSummary>()?;
    m.add_class::<types::ChartSuggestion>()?;
    m.add_class::<types::EnrichedError>()?;

    // Connection
    m.add_class::<executor::Connection>()?;

    // SQL functions
    m.add_function(wrap_pyfunction!(parser::parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(parser::build_correction_context, m)?)?;
    m.add_function(wrap_pyfunction!(parser::apply_correction, m)?)?;
    m.add_function(wrap_pyfunction!(optimizer::optimize_query, m)?)?;

    // Chart functions
    m.add_function(wrap_pyfunction!(chart::render_chart, m)?)?;

    // Statistics & suggestion
    m.add_function(wrap_pyfunction!(stats::summarize_result, m)?)?;
    m.add_function(wrap_pyfunction!(suggest::suggest_charts, m)?)?;

    // Export
    m.add_function(wrap_pyfunction!(export::export_csv, m)?)?;
    m.add_function(wrap_pyfunction!(export::export_json, m)?)?;

    // Agent functions
    m.add_function(wrap_pyfunction!(agent::get_tool_schema, m)?)?;
    m.add_function(wrap_pyfunction!(agent::get_tool_definition, m)?)?;

    Ok(())
}
