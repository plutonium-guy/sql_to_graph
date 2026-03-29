use std::collections::HashMap;

use pyo3::prelude::*;

use crate::types::{CellValueInner, ColumnCategory, ColumnStats, QueryResult, ResultSummary};

pub fn compute_summary_internal(result: &QueryResult) -> ResultSummary {
    let mut column_stats = Vec::new();
    let mut warnings = Vec::new();

    for (col_idx, col_name) in result.columns.iter().enumerate() {
        let mut nulls = 0usize;
        let mut numeric_vals = Vec::new();
        let mut text_vals = Vec::new();
        let mut bool_count = 0usize;
        let mut distinct_set: HashMap<String, usize> = HashMap::new();

        for row in &result.rows {
            if col_idx >= row.len() {
                continue;
            }
            let cell = &row[col_idx];
            let key = format!("{:?}", cell.value);
            *distinct_set.entry(key).or_insert(0) += 1;

            match &cell.value {
                CellValueInner::Null => nulls += 1,
                CellValueInner::Int(i) => numeric_vals.push(*i as f64),
                CellValueInner::Float(f) => numeric_vals.push(*f),
                CellValueInner::Bool(_) => bool_count += 1,
                CellValueInner::Text(s) => {
                    text_vals.push(s.clone());
                    // Also try parsing as numeric
                    if let Ok(f) = s.parse::<f64>() {
                        numeric_vals.push(f);
                    }
                }
            }
        }

        let total = result.rows.len();
        let non_null = total - nulls;

        // Classify column
        let category = if bool_count > 0 && bool_count == non_null {
            ColumnCategory::Boolean
        } else if !numeric_vals.is_empty() && numeric_vals.len() == non_null && text_vals.is_empty()
        {
            ColumnCategory::Numeric
        } else if !text_vals.is_empty() {
            // Check if temporal
            let temporal_count = text_vals.iter().filter(|s| looks_temporal(s)).count();
            if temporal_count > text_vals.len() / 2 {
                ColumnCategory::Temporal
            } else {
                ColumnCategory::Categorical
            }
        } else if !numeric_vals.is_empty() {
            ColumnCategory::Numeric
        } else {
            ColumnCategory::Unknown
        };

        // Compute stats
        let (min, max, mean, median, stddev) =
            if category == ColumnCategory::Numeric && !numeric_vals.is_empty() {
                let mut sorted = numeric_vals.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                let min = sorted.first().copied();
                let max = sorted.last().copied();
                let sum: f64 = sorted.iter().sum();
                let mean_val = sum / sorted.len() as f64;

                let median_val = if sorted.len() % 2 == 0 {
                    (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
                } else {
                    sorted[sorted.len() / 2]
                };

                let variance = sorted.iter().map(|v| (v - mean_val).powi(2)).sum::<f64>()
                    / sorted.len() as f64;
                let stddev_val = variance.sqrt();

                (min, max, Some(mean_val), Some(median_val), Some(stddev_val))
            } else {
                (None, None, None, None, None)
            };

        // Top values for categorical
        let top_values = if category == ColumnCategory::Categorical {
            let mut value_counts: HashMap<String, usize> = HashMap::new();
            for row in &result.rows {
                if col_idx < row.len() {
                    let display = match &row[col_idx].value {
                        CellValueInner::Null => continue,
                        CellValueInner::Text(s) => s.clone(),
                        CellValueInner::Int(i) => i.to_string(),
                        CellValueInner::Float(f) => format!("{:.2}", f),
                        CellValueInner::Bool(b) => b.to_string(),
                    };
                    *value_counts.entry(display).or_insert(0) += 1;
                }
            }
            let mut sorted: Vec<(String, usize)> = value_counts.into_iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(&a.1));
            sorted.truncate(10);
            sorted
        } else {
            vec![]
        };

        let distinct_count = distinct_set.len();

        // Warnings
        if total > 0 {
            let null_pct = nulls as f64 / total as f64 * 100.0;
            if null_pct > 30.0 {
                warnings.push(format!(
                    "Column '{}' has {:.0}% null values",
                    col_name, null_pct
                ));
            }
            if distinct_count == 1 && total > 1 {
                warnings.push(format!("Column '{}' has a single distinct value", col_name));
            }
            if category == ColumnCategory::Numeric {
                if let (Some(mn), Some(mx)) = (min, max) {
                    if (mx - mn).abs() < f64::EPSILON && total > 1 {
                        warnings.push(format!(
                            "Column '{}' has zero variance (all values = {})",
                            col_name, mn
                        ));
                    }
                }
            }
        }

        column_stats.push(ColumnStats {
            column_name: col_name.clone(),
            category,
            null_count: nulls,
            distinct_count,
            min,
            max,
            mean,
            median,
            stddev,
            top_values,
        });
    }

    ResultSummary {
        row_count: result.rows.len(),
        column_stats,
        warnings,
    }
}

fn looks_temporal(s: &str) -> bool {
    // Simple heuristic: contains date/time patterns
    let s = s.trim();
    // YYYY-MM-DD
    if s.len() >= 10
        && s.as_bytes()[4] == b'-'
        && s.as_bytes()[7] == b'-'
        && s[0..4].chars().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    // MM/DD/YYYY
    if s.len() >= 10
        && s.as_bytes()[2] == b'/'
        && s.as_bytes()[5] == b'/'
        && s[6..10].chars().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    false
}

#[pyfunction]
pub fn summarize_result(result: &QueryResult) -> ResultSummary {
    compute_summary_internal(result)
}
