use pyo3::prelude::*;

use crate::error::{Result, SqlToGraphError};
use crate::types::{CellValueInner, QueryResult};

fn cell_to_export_string(cell: &crate::types::CellValue) -> String {
    match &cell.value {
        CellValueInner::Null => String::new(),
        CellValueInner::Bool(b) => b.to_string(),
        CellValueInner::Int(i) => i.to_string(),
        CellValueInner::Float(f) => f.to_string(),
        CellValueInner::Text(s) => s.clone(),
    }
}

pub fn export_csv_internal(result: &QueryResult) -> Result<Vec<u8>> {
    let mut wtr = csv::Writer::from_writer(Vec::new());

    wtr.write_record(&result.columns)
        .map_err(|e| SqlToGraphError::ConfigError(format!("CSV write error: {}", e)))?;

    for row in &result.rows {
        let fields: Vec<String> = row.iter().map(cell_to_export_string).collect();
        wtr.write_record(&fields)
            .map_err(|e| SqlToGraphError::ConfigError(format!("CSV write error: {}", e)))?;
    }

    wtr.into_inner()
        .map_err(|e| SqlToGraphError::ConfigError(format!("CSV flush error: {}", e)))
}

pub fn export_json_records_internal(result: &QueryResult) -> Result<String> {
    let mut records = Vec::new();
    for row in &result.rows {
        let mut map = serde_json::Map::new();
        for (col, cell) in result.columns.iter().zip(row.iter()) {
            let val = match &cell.value {
                CellValueInner::Null => serde_json::Value::Null,
                CellValueInner::Bool(b) => serde_json::Value::Bool(*b),
                CellValueInner::Int(i) => serde_json::json!(*i),
                CellValueInner::Float(f) => serde_json::json!(*f),
                CellValueInner::Text(s) => serde_json::Value::String(s.clone()),
            };
            map.insert(col.clone(), val);
        }
        records.push(serde_json::Value::Object(map));
    }

    serde_json::to_string_pretty(&records)
        .map_err(|e| SqlToGraphError::ConfigError(format!("JSON error: {}", e)))
}

#[pyfunction]
pub fn export_csv(result: &QueryResult) -> PyResult<Vec<u8>> {
    Ok(export_csv_internal(result)?)
}

#[pyfunction]
pub fn export_json(result: &QueryResult) -> PyResult<String> {
    Ok(export_json_records_internal(result)?)
}
