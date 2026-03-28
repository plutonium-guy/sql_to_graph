use pyo3::prelude::*;
use sqlparser::parser::Parser;

use crate::dialect::to_sqlparser_dialect;
use crate::error::{Result, SqlToGraphError};
use crate::types::{CorrectionContext, ParseResult, SqlDialect, TableMetadata};

pub fn parse_sql_internal(sql: &str, dialect: &SqlDialect) -> Result<ParseResult> {
    let d = to_sqlparser_dialect(dialect);
    match Parser::parse_sql(d.as_ref(), sql) {
        Ok(statements) => {
            let normalized = if statements.len() == 1 {
                Some(statements[0].to_string())
            } else {
                Some(
                    statements
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .join(";\n"),
                )
            };
            Ok(ParseResult {
                is_valid: true,
                errors: vec![],
                statement_count: statements.len(),
                normalized_sql: normalized,
            })
        }
        Err(e) => Ok(ParseResult {
            is_valid: false,
            errors: vec![e.to_string()],
            statement_count: 0,
            normalized_sql: None,
        }),
    }
}

pub fn build_correction_context_internal(
    sql: &str,
    dialect: &SqlDialect,
    metadata: Vec<TableMetadata>,
) -> CorrectionContext {
    let parse_result = parse_sql_internal(sql, dialect).unwrap_or(ParseResult {
        is_valid: false,
        errors: vec!["Failed to parse SQL".to_string()],
        statement_count: 0,
        normalized_sql: None,
    });

    let mut schema_desc = String::new();
    for table in &metadata {
        schema_desc.push_str(&format!("Table: {}\n", table.table_name));
        for col in &table.columns {
            schema_desc.push_str(&format!(
                "  - {} ({}, nullable: {})\n",
                col.name, col.data_type, col.is_nullable
            ));
        }
        schema_desc.push('\n');
    }

    let error_desc = if parse_result.errors.is_empty() {
        "No parse errors detected, but the query may have logical issues.".to_string()
    } else {
        format!("Parse errors:\n{}", parse_result.errors.join("\n"))
    };

    let prompt = format!(
        r#"You are a SQL expert. Fix the following SQL query so it is valid and correct.

## Original SQL:
```sql
{}
```

## Issues:
{}

## Available Database Schema:
{}

## Instructions:
1. Fix any syntax errors
2. Ensure all table and column names match the schema exactly
3. Fix any logical issues (wrong joins, missing conditions, etc.)
4. Return ONLY the corrected SQL query, no explanations
5. Do NOT wrap the SQL in markdown code blocks

Corrected SQL:"#,
        sql, error_desc, schema_desc
    );

    CorrectionContext {
        original_sql: sql.to_string(),
        parse_errors: parse_result.errors,
        available_tables: metadata,
        suggested_prompt: prompt,
    }
}

pub fn apply_correction_internal(
    _original_sql: &str,
    corrected_sql: &str,
    dialect: &SqlDialect,
) -> Result<String> {
    // Strip any markdown code fences the LLM might have added
    let cleaned = corrected_sql
        .trim()
        .trim_start_matches("```sql")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let result = parse_sql_internal(cleaned, dialect)?;
    if !result.is_valid {
        return Err(SqlToGraphError::ParseError(format!(
            "LLM correction produced invalid SQL: {}",
            result.errors.join(", ")
        )));
    }

    Ok(result.normalized_sql.unwrap_or_else(|| cleaned.to_string()))
}

// PyO3-exposed functions

#[pyfunction]
#[pyo3(signature = (sql, dialect=SqlDialect::Generic))]
pub fn parse_sql(sql: &str, dialect: SqlDialect) -> PyResult<ParseResult> {
    Ok(parse_sql_internal(sql, &dialect)?)
}

#[pyfunction]
#[pyo3(signature = (sql, metadata, dialect=SqlDialect::Generic))]
pub fn build_correction_context(
    sql: &str,
    metadata: Vec<TableMetadata>,
    dialect: SqlDialect,
) -> CorrectionContext {
    build_correction_context_internal(sql, &dialect, metadata)
}

#[pyfunction]
#[pyo3(signature = (original_sql, corrected_sql, dialect=SqlDialect::Generic))]
pub fn apply_correction(
    original_sql: &str,
    corrected_sql: &str,
    dialect: SqlDialect,
) -> PyResult<String> {
    Ok(apply_correction_internal(original_sql, corrected_sql, &dialect)?)
}
