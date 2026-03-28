use pyo3::prelude::*;
use serde_json::json;

#[pyfunction]
pub fn get_tool_schema() -> PyResult<String> {
    let schema = json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute. Will be auto-corrected and optimized if enabled."
            },
            "connection_string": {
                "type": "string",
                "description": "Database connection URL (e.g., 'postgresql://user:pass@host/db', 'mysql://...', 'sqlite:///path.db')"
            },
            "schema": {
                "type": "string",
                "description": "Database schema to use (e.g., 'public', 'analytics'). Defaults to 'public' for PostgreSQL."
            },
            "auto_correct": {
                "type": "boolean",
                "default": true,
                "description": "Whether to auto-correct the SQL query using LLM based on actual DB schema"
            },
            "optimize": {
                "type": "boolean",
                "default": true,
                "description": "Whether to apply query optimizations"
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of rows to return. Use for pagination."
            },
            "offset": {
                "type": "integer",
                "default": 0,
                "description": "Number of rows to skip. Use with limit for pagination."
            },
            "include_stats": {
                "type": "boolean",
                "default": false,
                "description": "Include column statistics (min/max/mean/median/stddev/distinct) in the response"
            },
            "suggest_charts": {
                "type": "boolean",
                "default": false,
                "description": "Include chart suggestions based on result data types and patterns"
            },
            "export_format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Export result in this format (returns raw data instead of rows)"
            },
            "chart": {
                "type": "object",
                "description": "Optional chart configuration. If provided, generates a visualization from query results.",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["bar", "horizontal_bar", "stacked_bar", "line", "area", "pie", "donut", "scatter", "histogram", "heatmap"],
                        "description": "Chart type"
                    },
                    "x_column": {
                        "type": "string",
                        "description": "Column name to use for the X axis / labels"
                    },
                    "y_column": {
                        "type": "string",
                        "description": "Column name to use for the Y axis / values"
                    },
                    "z_column": {
                        "type": "string",
                        "description": "Optional third column for stacked bar (second series) or heatmap (intensity)"
                    },
                    "bin_count": {
                        "type": "integer",
                        "default": 10,
                        "description": "Number of bins for histogram charts"
                    },
                    "title": {
                        "type": "string",
                        "description": "Chart title"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["html", "png", "jpg", "svg"],
                        "default": "html",
                        "description": "Output format for the chart"
                    }
                },
                "required": ["type", "x_column", "y_column"]
            }
        },
        "required": ["sql", "connection_string"]
    });

    serde_json::to_string_pretty(&schema)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
pub fn get_tool_definition() -> PyResult<String> {
    let tools = json!([
        {
            "name": "sql_to_graph",
            "description": "Execute a SQL query against a database with auto-correction, optimization, and optional chart generation. Supports PostgreSQL, MySQL, and SQLite. Includes statistics, chart suggestions, and export capabilities.",
            "input_schema": serde_json::from_str::<serde_json::Value>(&get_tool_schema()?).unwrap()
        },
        {
            "name": "sql_discover_schemas",
            "description": "List all available schemas/databases in the connected database. Use this first to understand what schemas are available.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection URL"
                    }
                },
                "required": ["connection_string"]
            }
        },
        {
            "name": "sql_describe_table",
            "description": "Get detailed metadata for a specific table including column names, types, nullability, and estimated row count.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection URL"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name to describe"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (optional, defaults to 'public' for PostgreSQL)"
                    }
                },
                "required": ["connection_string", "table"]
            }
        },
        {
            "name": "sql_sample_data",
            "description": "Get a sample of rows from a table. Use this to understand the data before writing queries.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection URL"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name to sample"
                    },
                    "n": {
                        "type": "integer",
                        "default": 5,
                        "description": "Number of sample rows to return"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (optional)"
                    }
                },
                "required": ["connection_string", "table"]
            }
        }
    ]);

    serde_json::to_string_pretty(&tools)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}
