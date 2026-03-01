use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, GroupByExpr, OrderByExpr, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::query::expr::{Expr, Schema, Value};
use crate::query::plan::{AggFunc, LogicalPlan, SortOrder};

/// Error from SQL parsing or translation.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlError(pub String);

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SqlError: {}", self.0)
    }
}

impl std::error::Error for SqlError {}

fn err(msg: impl Into<String>) -> SqlError {
    SqlError(msg.into())
}

/// Parse a SQL SELECT statement and convert it to a LogicalPlan.
///
/// Supported subset:
/// - `SELECT * FROM table`
/// - `SELECT col1, col2 FROM table WHERE expr`
/// - `SELECT col1, COUNT(*), SUM(col) FROM table GROUP BY col1`
/// - `ORDER BY col ASC/DESC`, `LIMIT n`
///
/// The schema must be provided so we can identify the table.
pub fn sql_to_plan(sql: &str, schema: Schema) -> Result<LogicalPlan, SqlError> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| err(format!("parse error: {}", e)))?;

    if stmts.len() != 1 {
        return Err(err("expected exactly one statement"));
    }

    match stmts.remove(0) {
        Statement::Query(q) => translate_query(*q, schema),
        other => Err(err(format!("unsupported statement: {:?}", other))),
    }
}

fn translate_query(query: Query, schema: Schema) -> Result<LogicalPlan, SqlError> {
    let Query {
        body,
        order_by,
        limit,
        ..
    } = query;

    let mut plan = match *body {
        SetExpr::Select(sel) => translate_select(*sel, schema)?,
        other => return Err(err(format!("unsupported query body: {:?}", other))),
    };

    // ORDER BY — sqlparser 0.53: query.order_by is Option<OrderBy>,
    // which wraps Vec<OrderByExpr> in ob.exprs.
    let mut sort_keys = Vec::new();
    if let Some(ob) = order_by {
        for OrderByExpr { expr, asc, .. } in &ob.exprs {
            let col = expr_to_col_name(expr)?;
            let order = match asc {
                Some(false) => SortOrder::Desc,
                _ => SortOrder::Asc,
            };
            sort_keys.push((col, order));
        }
    }
    if !sort_keys.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
    }

    // LIMIT
    if let Some(limit_expr) = limit {
        let n = match limit_expr {
            SqlExpr::Value(SqlValue::Number(s, _)) => s
                .parse::<usize>()
                .map_err(|_| err(format!("invalid LIMIT: {}", s)))?,
            other => return Err(err(format!("unsupported LIMIT expr: {:?}", other))),
        };
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
        };
    }

    Ok(plan)
}

fn translate_select(select: Select, schema: Schema) -> Result<LogicalPlan, SqlError> {
    // FROM clause — validate table name.
    if select.from.len() != 1 {
        return Err(err("expected exactly one FROM table"));
    }
    let from = &select.from[0];
    match &from.relation {
        TableFactor::Table { name, .. } => {
            let table_name = name.to_string();
            if table_name != schema.table {
                return Err(err(format!(
                    "unknown table '{}', expected '{}'",
                    table_name, schema.table
                )));
            }
        }
        other => return Err(err(format!("unsupported FROM: {:?}", other))),
    }

    // Base plan is a Scan.
    let mut plan: LogicalPlan = LogicalPlan::Scan {
        schema: schema.clone(),
    };

    // WHERE clause.
    if let Some(where_expr) = select.selection {
        let predicate = sql_expr_to_expr(&where_expr)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // SELECT items — detect aggregates vs projections.
    let mut projections: Vec<String> = Vec::new();
    let mut agg_funcs: Vec<(String, AggFunc)> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();
    let mut is_star = false;

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                is_star = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some((out_name, agg)) = try_agg_func(expr)? {
                    agg_funcs.push((out_name, agg));
                } else {
                    let col = expr_to_col_name(expr)?;
                    projections.push(col);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some((_, agg)) = try_agg_func(expr)? {
                    agg_funcs.push((alias.value.clone(), agg));
                } else {
                    let _col = expr_to_col_name(expr)?;
                    projections.push(alias.value.clone());
                }
            }
            other => return Err(err(format!("unsupported SELECT item: {:?}", other))),
        }
    }

    // GROUP BY — select.group_by is a GroupByExpr enum (not a Vec).
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                group_by_cols.push(expr_to_col_name(e)?);
            }
        }
        GroupByExpr::All(_) => {
            return Err(err("GROUP BY ALL not supported"));
        }
    }

    // Apply aggregation if needed.
    if !agg_funcs.is_empty() || !group_by_cols.is_empty() {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: group_by_cols,
            agg_funcs,
        };
    } else if !is_star && !projections.is_empty() {
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns: projections,
        };
    }

    Ok(plan)
}

/// Try to extract an aggregate function from a SQL expression.
fn try_agg_func(
    expr: &SqlExpr,
) -> Result<Option<(String, AggFunc)>, SqlError> {
    if let SqlExpr::Function(func) = expr {
        let name = func.name.to_string().to_uppercase();
        let out_name = func.name.to_string().to_lowercase();

        match name.as_str() {
            "COUNT" => return Ok(Some((out_name, AggFunc::Count))),
            "SUM" => {
                let col = agg_arg_col(func)?;
                return Ok(Some((out_name, AggFunc::Sum(col))));
            }
            "MIN" => {
                let col = agg_arg_col(func)?;
                return Ok(Some((out_name, AggFunc::Min(col))));
            }
            "MAX" => {
                let col = agg_arg_col(func)?;
                return Ok(Some((out_name, AggFunc::Max(col))));
            }
            "AVG" => {
                let col = agg_arg_col(func)?;
                return Ok(Some((out_name, AggFunc::Avg(col))));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn agg_arg_col(func: &sqlparser::ast::Function) -> Result<String, SqlError> {
    use sqlparser::ast::FunctionArguments;
    match &func.args {
        FunctionArguments::List(list) => {
            if list.args.len() == 1 {
                match &list.args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                        expr_to_col_name(e)
                    }
                    other => Err(err(format!("unsupported agg arg: {:?}", other))),
                }
            } else {
                Err(err("expected 1 aggregate argument"))
            }
        }
        other => Err(err(format!("unsupported agg args: {:?}", other))),
    }
}

fn expr_to_col_name(expr: &SqlExpr) -> Result<String, SqlError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => Ok(parts.last().map(|p| p.value.clone()).unwrap_or_default()),
        other => Err(err(format!("expected column name, got: {:?}", other))),
    }
}

fn sql_expr_to_expr(expr: &SqlExpr) -> Result<Expr, SqlError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(Expr::col(ident.value.clone())),
        SqlExpr::CompoundIdentifier(parts) => Ok(Expr::col(
            parts.last().map(|p| p.value.clone()).unwrap_or_default(),
        )),
        SqlExpr::Value(v) => Ok(Expr::lit(sql_value_to_value(v)?)),
        SqlExpr::BinaryOp { left, op, right } => {
            let l = sql_expr_to_expr(left)?;
            let r = sql_expr_to_expr(right)?;
            match op {
                BinaryOperator::Eq => Ok(Expr::eq(l, r)),
                BinaryOperator::NotEq => Ok(Expr::ne(l, r)),
                BinaryOperator::Lt => Ok(Expr::lt(l, r)),
                BinaryOperator::Gt => Ok(Expr::gt(l, r)),
                BinaryOperator::LtEq => Ok(Expr::le(l, r)),
                BinaryOperator::GtEq => Ok(Expr::ge(l, r)),
                BinaryOperator::And => Ok(Expr::and(l, r)),
                BinaryOperator::Or => Ok(Expr::or(l, r)),
                other => Err(err(format!("unsupported operator: {:?}", other))),
            }
        }
        SqlExpr::UnaryOp { op, expr } => {
            let e = sql_expr_to_expr(expr)?;
            match op {
                UnaryOperator::Not => Ok(Expr::not(e)),
                other => Err(err(format!("unsupported unary op: {:?}", other))),
            }
        }
        SqlExpr::Nested(inner) => sql_expr_to_expr(inner),
        other => Err(err(format!("unsupported expression: {:?}", other))),
    }
}

fn sql_value_to_value(v: &SqlValue) -> Result<Value, SqlError> {
    match v {
        SqlValue::Number(s, _) => {
            if let Ok(n) = s.parse::<i64>() {
                Ok(Value::Int64(n))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Err(err(format!("invalid number: {}", s)))
            }
        }
        SqlValue::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
        SqlValue::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(err(format!("unsupported value: {:?}", other))),
    }
}
