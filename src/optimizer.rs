use pyo3::prelude::*;
use sqlparser::ast::*;
use sqlparser::parser::Parser;

use crate::dialect::to_sqlparser_dialect;
use crate::error::Result;
use crate::types::SqlDialect;

pub fn optimize_query_internal(sql: &str, dialect: &SqlDialect) -> Result<String> {
    let d = to_sqlparser_dialect(dialect);
    let mut statements = Parser::parse_sql(d.as_ref(), sql)?;

    for stmt in &mut statements {
        optimize_statement(stmt);
    }

    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";\n"))
}

fn optimize_statement(stmt: &mut Statement) {
    if let Statement::Query(query) = stmt {
        optimize_query_ast(query);
    }
}

fn optimize_query_ast(query: &mut Query) {
    optimize_set_expr(&mut query.body);

    // Push LIMIT into subqueries where safe
    if let Some(limit_expr) = &query.limit {
        if let SetExpr::Select(select) = query.body.as_mut() {
            push_limit_into_subqueries(select, limit_expr);
        }
    }
}

fn optimize_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            optimize_select(select);
        }
        SetExpr::SetOperation { left, right, .. } => {
            optimize_set_expr(left);
            optimize_set_expr(right);
        }
        SetExpr::Query(q) => {
            optimize_query_ast(q);
        }
        _ => {}
    }
}

fn optimize_select(select: &mut Select) {
    // Optimize WHERE clause
    if let Some(selection) = &mut select.selection {
        *selection = fold_constants(selection.clone());
        *selection = flatten_ands(selection.clone());
    }

    // Optimize subqueries in FROM clause
    for item in &mut select.from {
        optimize_table_with_joins(item);
    }
}

fn optimize_table_with_joins(table: &mut TableWithJoins) {
    optimize_table_factor(&mut table.relation);
    for join in &mut table.joins {
        optimize_table_factor(&mut join.relation);
        // Optimize join conditions based on join_operator
        let constraint = match &mut join.join_operator {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c) => Some(c),
            _ => None,
        };
        if let Some(JoinConstraint::On(expr)) = constraint {
            *expr = fold_constants(expr.clone());
            *expr = flatten_ands(expr.clone());
        }
    }
}

fn optimize_table_factor(factor: &mut TableFactor) {
    if let TableFactor::Derived { subquery, .. } = factor {
        optimize_query_ast(subquery);
    }
}

/// Fold constant expressions: `1 + 2` -> `3`, `true AND true` -> `true`
fn fold_constants(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = fold_constants(*left);
            let right = fold_constants(*right);

            // Fold numeric constants
            if let (
                Expr::Value(ValueWithSpan {
                    value: Value::Number(l, _),
                    ..
                }),
                Expr::Value(ValueWithSpan {
                    value: Value::Number(r, _),
                    ..
                }),
            ) = (&left, &right)
            {
                if let (Ok(lv), Ok(rv)) = (l.parse::<f64>(), r.parse::<f64>()) {
                    let result = match op {
                        BinaryOperator::Plus => Some(lv + rv),
                        BinaryOperator::Minus => Some(lv - rv),
                        BinaryOperator::Multiply => Some(lv * rv),
                        BinaryOperator::Divide if rv != 0.0 => Some(lv / rv),
                        _ => None,
                    };
                    if let Some(val) = result {
                        // Use integer representation if it's a whole number
                        let num_str = if val.fract() == 0.0 {
                            format!("{}", val as i64)
                        } else {
                            format!("{}", val)
                        };
                        return Expr::value(Value::Number(num_str, false));
                    }
                }
            }

            // Fold boolean identities
            match (&left, &op, &right) {
                // x AND true -> x
                (
                    _,
                    BinaryOperator::And,
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(true),
                        ..
                    }),
                ) => return left,
                (
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(true),
                        ..
                    }),
                    BinaryOperator::And,
                    _,
                ) => return right,
                // x AND false -> false
                (
                    _,
                    BinaryOperator::And,
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(false),
                        ..
                    }),
                )
                | (
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(false),
                        ..
                    }),
                    BinaryOperator::And,
                    _,
                ) => {
                    return Expr::value(Value::Boolean(false));
                }
                // x OR true -> true
                (
                    _,
                    BinaryOperator::Or,
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(true),
                        ..
                    }),
                )
                | (
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(true),
                        ..
                    }),
                    BinaryOperator::Or,
                    _,
                ) => {
                    return Expr::value(Value::Boolean(true));
                }
                // x OR false -> x
                (
                    _,
                    BinaryOperator::Or,
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(false),
                        ..
                    }),
                ) => return left,
                (
                    Expr::Value(ValueWithSpan {
                        value: Value::Boolean(false),
                        ..
                    }),
                    BinaryOperator::Or,
                    _,
                ) => return right,
                _ => {}
            }

            Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        Expr::Nested(inner) => {
            let folded = fold_constants(*inner);
            // Remove unnecessary nesting around simple values
            if matches!(folded, Expr::Value(_)) {
                folded
            } else {
                Expr::Nested(Box::new(folded))
            }
        }
        other => other,
    }
}

/// Flatten nested AND chains: `(a AND b) AND c` -> `a AND b AND c`
fn flatten_ands(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut conditions = Vec::new();
            collect_and_conditions(*left, &mut conditions);
            collect_and_conditions(*right, &mut conditions);

            conditions
                .into_iter()
                .reduce(|acc, cond| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::And,
                    right: Box::new(cond),
                })
                .unwrap()
        }
        other => other,
    }
}

fn collect_and_conditions(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_conditions(*left, out);
            collect_and_conditions(*right, out);
        }
        Expr::Nested(inner) => {
            if let Expr::BinaryOp {
                op: BinaryOperator::And,
                ..
            } = *inner
            {
                collect_and_conditions(*inner, out);
            } else {
                out.push(Expr::Nested(inner));
            }
        }
        other => out.push(other),
    }
}

/// Push outer LIMIT into subqueries when there's no aggregation or HAVING
fn push_limit_into_subqueries(select: &mut Select, limit: &Expr) {
    for item in &mut select.from {
        if let TableFactor::Derived { subquery, .. } = &mut item.relation {
            // Only push if subquery has no existing LIMIT and no aggregation
            if subquery.limit.is_none() {
                if let SetExpr::Select(sub_select) = subquery.body.as_ref() {
                    let has_agg = sub_select.group_by != GroupByExpr::Expressions(vec![], vec![])
                        || sub_select.having.is_some();
                    if !has_agg {
                        subquery.limit = Some(limit.clone());
                    }
                }
            }
        }
    }
}

#[pyfunction]
#[pyo3(signature = (sql, dialect=SqlDialect::Generic))]
pub fn optimize_query(sql: &str, dialect: SqlDialect) -> PyResult<String> {
    Ok(optimize_query_internal(sql, &dialect)?)
}
