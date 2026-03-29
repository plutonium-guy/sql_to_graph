use pyo3::prelude::*;

use crate::stats::compute_summary_internal;
use crate::types::{ChartSuggestion, ChartType, ColumnCategory, QueryResult};

#[pyfunction]
pub fn suggest_charts(result: &QueryResult) -> Vec<ChartSuggestion> {
    let summary = compute_summary_internal(result);
    let mut suggestions = Vec::new();

    // Classify columns
    let numeric_cols: Vec<&str> = summary
        .column_stats
        .iter()
        .filter(|s| s.category == ColumnCategory::Numeric)
        .map(|s| s.column_name.as_str())
        .collect();

    let categorical_cols: Vec<&str> = summary
        .column_stats
        .iter()
        .filter(|s| s.category == ColumnCategory::Categorical)
        .map(|s| s.column_name.as_str())
        .collect();

    let temporal_cols: Vec<&str> = summary
        .column_stats
        .iter()
        .filter(|s| s.category == ColumnCategory::Temporal)
        .map(|s| s.column_name.as_str())
        .collect();

    let row_count = summary.row_count;

    // Rule 1: 1 temporal + 1 numeric → Line chart (highest priority)
    if let (Some(&time_col), Some(&num_col)) = (temporal_cols.first(), numeric_cols.first()) {
        suggestions.push(ChartSuggestion {
            chart_type: ChartType::Line,
            x_column: time_col.to_string(),
            y_column: num_col.to_string(),
            z_column: None,
            title: format!("{} over {}", num_col, time_col),
            confidence: 0.95,
            reasoning: "Temporal x-axis with numeric values is best shown as a line chart".into(),
        });
        // Also suggest area
        suggestions.push(ChartSuggestion {
            chart_type: ChartType::Area,
            x_column: time_col.to_string(),
            y_column: num_col.to_string(),
            z_column: None,
            title: format!("{} over {}", num_col, time_col),
            confidence: 0.75,
            reasoning: "Area chart emphasizes magnitude over time".into(),
        });
    }

    // Rule 2: 1 categorical + 1 numeric → Bar or Pie
    if let (Some(&cat_col), Some(&num_col)) = (categorical_cols.first(), numeric_cols.first()) {
        let distinct = summary
            .column_stats
            .iter()
            .find(|s| s.column_name == cat_col)
            .map(|s| s.distinct_count)
            .unwrap_or(0);

        if distinct <= 20 {
            suggestions.push(ChartSuggestion {
                chart_type: ChartType::Bar,
                x_column: cat_col.to_string(),
                y_column: num_col.to_string(),
                z_column: None,
                title: format!("{} by {}", num_col, cat_col),
                confidence: 0.9,
                reasoning: format!(
                    "Categorical column '{}' with {} distinct values maps well to bar chart",
                    cat_col, distinct
                ),
            });

            // Horizontal bar for longer labels or more categories
            if distinct > 6 {
                suggestions.push(ChartSuggestion {
                    chart_type: ChartType::HorizontalBar,
                    x_column: cat_col.to_string(),
                    y_column: num_col.to_string(),
                    z_column: None,
                    title: format!("{} by {}", num_col, cat_col),
                    confidence: 0.85,
                    reasoning: "Many categories are easier to read as horizontal bars".into(),
                });
            }
        }

        if distinct >= 2 && distinct <= 8 {
            suggestions.push(ChartSuggestion {
                chart_type: ChartType::Pie,
                x_column: cat_col.to_string(),
                y_column: num_col.to_string(),
                z_column: None,
                title: format!("{} distribution by {}", num_col, cat_col),
                confidence: 0.7,
                reasoning: format!(
                    "{} categories is ideal for pie/donut chart proportion view",
                    distinct
                ),
            });
            suggestions.push(ChartSuggestion {
                chart_type: ChartType::Donut,
                x_column: cat_col.to_string(),
                y_column: num_col.to_string(),
                z_column: None,
                title: format!("{} share by {}", num_col, cat_col),
                confidence: 0.65,
                reasoning: "Donut chart provides proportion view with cleaner center".into(),
            });
        }
    }

    // Rule 3: 1 categorical + 2 numeric → Stacked bar
    if let Some(&cat_col) = categorical_cols.first() {
        if numeric_cols.len() >= 2 {
            suggestions.push(ChartSuggestion {
                chart_type: ChartType::StackedBar,
                x_column: cat_col.to_string(),
                y_column: numeric_cols[0].to_string(),
                z_column: Some(numeric_cols[1].to_string()),
                title: format!("{} & {} by {}", numeric_cols[0], numeric_cols[1], cat_col),
                confidence: 0.8,
                reasoning: "Two numeric series grouped by category → stacked bar comparison".into(),
            });
        }
    }

    // Rule 4: 2 numeric, no categorical → Scatter
    if numeric_cols.len() >= 2 && categorical_cols.is_empty() && temporal_cols.is_empty() {
        suggestions.push(ChartSuggestion {
            chart_type: ChartType::Scatter,
            x_column: numeric_cols[0].to_string(),
            y_column: numeric_cols[1].to_string(),
            z_column: None,
            title: format!("{} vs {}", numeric_cols[1], numeric_cols[0]),
            confidence: 0.85,
            reasoning: "Two numeric columns without categories → scatter plot for correlation"
                .into(),
        });
    }

    // Rule 5: 1 numeric only → Histogram
    if numeric_cols.len() == 1 && categorical_cols.is_empty() && temporal_cols.is_empty() {
        suggestions.push(ChartSuggestion {
            chart_type: ChartType::Histogram,
            x_column: numeric_cols[0].to_string(),
            y_column: numeric_cols[0].to_string(),
            z_column: None,
            title: format!("Distribution of {}", numeric_cols[0]),
            confidence: 0.9,
            reasoning: "Single numeric column → histogram shows value distribution".into(),
        });
    }

    // Rule 6: Many numeric values + rows suggest histogram even with categories
    if row_count > 50 {
        for &num_col in &numeric_cols {
            let stats = summary
                .column_stats
                .iter()
                .find(|s| s.column_name == num_col);
            if let Some(s) = stats {
                if s.distinct_count > 20 {
                    suggestions.push(ChartSuggestion {
                        chart_type: ChartType::Histogram,
                        x_column: num_col.to_string(),
                        y_column: num_col.to_string(),
                        z_column: None,
                        title: format!("Distribution of {}", num_col),
                        confidence: 0.6,
                        reasoning: format!(
                            "Column '{}' has {} distinct values across {} rows — histogram reveals distribution",
                            num_col, s.distinct_count, row_count
                        ),
                    });
                    break; // Only suggest one histogram
                }
            }
        }
    }

    // Rule 7: 2 categorical + 1 numeric → Heatmap
    if categorical_cols.len() >= 2 {
        if let Some(&num_col) = numeric_cols.first() {
            suggestions.push(ChartSuggestion {
                chart_type: ChartType::Heatmap,
                x_column: categorical_cols[0].to_string(),
                y_column: categorical_cols[1].to_string(),
                z_column: Some(num_col.to_string()),
                title: format!(
                    "{} by {} and {}",
                    num_col, categorical_cols[0], categorical_cols[1]
                ),
                confidence: 0.7,
                reasoning: "Two categorical dimensions with a numeric measure → heatmap".into(),
            });
        }
    }

    // Sort by confidence descending, take top 5
    suggestions.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
    suggestions.truncate(5);
    suggestions
}
