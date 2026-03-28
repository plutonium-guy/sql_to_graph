use sqlx::any::AnyRow;
use sqlx::{AnyPool, Row};

use crate::error::{Result, SqlToGraphError};
use crate::types::{ColumnInfo, SchemaInfo, SqlDialect, TableMetadata};

pub async fn fetch_schemas(pool: &AnyPool, dialect: &SqlDialect) -> Result<Vec<SchemaInfo>> {
    match dialect {
        SqlDialect::PostgreSQL => fetch_pg_schemas(pool).await,
        SqlDialect::MySQL => fetch_mysql_schemas(pool).await,
        SqlDialect::SQLite => Ok(vec![SchemaInfo {
            name: "main".into(),
            table_count: fetch_sqlite_metadata(pool, None)
                .await
                .map(|t| t.len())
                .unwrap_or(0),
        }]),
        SqlDialect::Generic => fetch_pg_schemas(pool)
            .await
            .or_else(|_| Ok(vec![SchemaInfo {
                name: "default".into(),
                table_count: 0,
            }])),
    }
}

async fn fetch_pg_schemas(pool: &AnyPool) -> Result<Vec<SchemaInfo>> {
    let rows: Vec<AnyRow> = sqlx::query(
        "SELECT s.schema_name::TEXT as schema_name, \
                COUNT(t.table_name)::INT as table_count \
         FROM information_schema.schemata s \
         LEFT JOIN information_schema.tables t \
           ON t.table_schema = s.schema_name AND t.table_type = 'BASE TABLE' \
         WHERE s.schema_name NOT LIKE 'pg_%' \
           AND s.schema_name != 'information_schema' \
         GROUP BY s.schema_name \
         ORDER BY s.schema_name",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| SchemaInfo {
            name: r.get("schema_name"),
            table_count: r.get::<i32, _>("table_count") as usize,
        })
        .collect())
}

async fn fetch_mysql_schemas(pool: &AnyPool) -> Result<Vec<SchemaInfo>> {
    let rows: Vec<AnyRow> = sqlx::query(
        "SELECT s.schema_name as schema_name, \
                COUNT(t.table_name) as table_count \
         FROM information_schema.schemata s \
         LEFT JOIN information_schema.tables t \
           ON t.table_schema = s.schema_name AND t.table_type = 'BASE TABLE' \
         WHERE s.schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys') \
         GROUP BY s.schema_name \
         ORDER BY s.schema_name",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let tc: i64 = r.try_get("table_count").unwrap_or(0);
            SchemaInfo {
                name: r.get("schema_name"),
                table_count: tc as usize,
            }
        })
        .collect())
}

pub async fn fetch_metadata(
    pool: &AnyPool,
    dialect: &SqlDialect,
    schema: Option<&str>,
) -> Result<Vec<TableMetadata>> {
    match dialect {
        SqlDialect::PostgreSQL => fetch_pg_metadata(pool, schema.unwrap_or("public")).await,
        SqlDialect::MySQL => fetch_mysql_metadata(pool, schema).await,
        SqlDialect::SQLite => fetch_sqlite_metadata(pool, schema).await,
        SqlDialect::Generic => match fetch_pg_metadata(pool, schema.unwrap_or("public")).await {
            Ok(m) if !m.is_empty() => Ok(m),
            _ => fetch_sqlite_metadata(pool, schema).await,
        },
    }
}

pub async fn fetch_table_metadata(
    pool: &AnyPool,
    dialect: &SqlDialect,
    table: &str,
    schema: Option<&str>,
) -> Result<TableMetadata> {
    let all = fetch_metadata(pool, dialect, schema).await?;
    all.into_iter()
        .find(|t| t.table_name == table)
        .ok_or_else(|| {
            SqlToGraphError::DatabaseError(format!(
                "Table '{}' not found in schema '{}'",
                table,
                schema.unwrap_or("default")
            ))
        })
}

async fn fetch_pg_metadata(pool: &AnyPool, schema: &str) -> Result<Vec<TableMetadata>> {
    let table_rows: Vec<AnyRow> = sqlx::query(
        "SELECT t.table_name::TEXT as table_name, \
                COALESCE(s.n_live_tup, 0)::BIGINT as row_estimate \
         FROM information_schema.tables t \
         LEFT JOIN pg_stat_user_tables s \
           ON s.relname = t.table_name AND s.schemaname = t.table_schema \
         WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE' \
         ORDER BY t.table_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    let mut tables = Vec::new();
    for row in &table_rows {
        let table_name: String = row.get("table_name");
        let row_estimate: i64 = row.try_get("row_estimate").unwrap_or(0);

        let col_rows: Vec<AnyRow> = sqlx::query(
            "SELECT column_name::TEXT as column_name, \
                    data_type::TEXT as data_type, \
                    is_nullable::TEXT as is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        )
        .bind(schema)
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        let columns = col_rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get("is_nullable");
                ColumnInfo {
                    name: r.get("column_name"),
                    data_type: r.get("data_type"),
                    is_nullable: nullable_str.to_uppercase() == "YES",
                }
            })
            .collect();

        tables.push(TableMetadata {
            schema_name: Some(schema.to_string()),
            table_name,
            columns,
            row_count_estimate: Some(row_estimate),
        });
    }

    Ok(tables)
}

async fn fetch_mysql_metadata(pool: &AnyPool, schema: Option<&str>) -> Result<Vec<TableMetadata>> {
    let db_name = if let Some(s) = schema {
        s.to_string()
    } else {
        let db_row: AnyRow = sqlx::query("SELECT DATABASE() as db")
            .fetch_one(pool)
            .await?;
        db_row.try_get("db").map_err(|e| {
            SqlToGraphError::DatabaseError(format!("Cannot determine database: {}", e))
        })?
    };

    let table_rows: Vec<AnyRow> = sqlx::query(
        "SELECT CAST(table_name AS CHAR) as table_name \
         FROM information_schema.tables \
         WHERE table_schema = ? AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )
    .bind(&db_name)
    .fetch_all(pool)
    .await?;

    let mut tables = Vec::new();
    for row in &table_rows {
        let table_name: String = row.get("table_name");

        let col_rows: Vec<AnyRow> = sqlx::query(
            "SELECT CAST(column_name AS CHAR) as column_name, \
                    CAST(data_type AS CHAR) as data_type, \
                    CAST(is_nullable AS CHAR) as is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .bind(&db_name)
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        let columns = col_rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get("is_nullable");
                ColumnInfo {
                    name: r.get("column_name"),
                    data_type: r.get("data_type"),
                    is_nullable: nullable_str.to_uppercase() == "YES",
                }
            })
            .collect();

        tables.push(TableMetadata {
            schema_name: Some(db_name.clone()),
            table_name,
            columns,
            row_count_estimate: None,
        });
    }

    Ok(tables)
}

async fn fetch_sqlite_metadata(
    pool: &AnyPool,
    _schema: Option<&str>,
) -> Result<Vec<TableMetadata>> {
    let table_rows: Vec<AnyRow> = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await?;

    let mut tables = Vec::new();
    for row in &table_rows {
        let table_name: String = row.get("name");

        let col_rows: Vec<AnyRow> =
            sqlx::query(&format!("PRAGMA table_info(\"{}\")", table_name))
                .fetch_all(pool)
                .await?;

        let columns = col_rows
            .iter()
            .map(|r| {
                let notnull: i32 = r.get("notnull");
                ColumnInfo {
                    name: r.get("name"),
                    data_type: r.get("type"),
                    is_nullable: notnull == 0,
                }
            })
            .collect();

        tables.push(TableMetadata {
            schema_name: Some("main".into()),
            table_name,
            columns,
            row_count_estimate: None,
        });
    }

    Ok(tables)
}
