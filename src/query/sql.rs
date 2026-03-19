use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, GroupByExpr, JoinConstraint, JoinOperator,
    OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::query::expr::{Expr, Schema, Value};
use crate::query::plan::{AggFunc, JoinType, LogicalPlan, SortOrder};

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

/// Parse a SQL SELECT statement with multi-table support.
///
/// Uses a schema resolver closure to look up schemas by table name,
/// enabling JOIN queries across multiple tables.
pub fn sql_to_plan_multi(
    sql: &str,
    schema_resolver: impl Fn(&str) -> Option<Schema>,
) -> Result<LogicalPlan, SqlError> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| err(format!("parse error: {}", e)))?;

    if stmts.len() != 1 {
        return Err(err("expected exactly one statement"));
    }

    match stmts.remove(0) {
        Statement::Query(q) => translate_query_multi(*q, &schema_resolver),
        other => Err(err(format!("unsupported statement: {:?}", other))),
    }
}

fn translate_query_multi(
    query: Query,
    schema_resolver: &impl Fn(&str) -> Option<Schema>,
) -> Result<LogicalPlan, SqlError> {
    let Query {
        body,
        order_by,
        limit,
        ..
    } = query;

    let mut plan = match *body {
        SetExpr::Select(sel) => translate_select_multi(*sel, schema_resolver)?,
        other => return Err(err(format!("unsupported query body: {:?}", other))),
    };

    // ORDER BY
    let mut sort_keys = Vec::new();
    if let Some(ob) = order_by {
        for OrderByExpr { expr, asc, .. } in &ob.exprs {
            let col = expr_to_col_name_qualified(expr)?;
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

fn translate_select_multi(
    select: Select,
    schema_resolver: &impl Fn(&str) -> Option<Schema>,
) -> Result<LogicalPlan, SqlError> {
    if select.from.is_empty() {
        return Err(err("expected at least one FROM table"));
    }

    let from = &select.from[0];

    // Get the primary table name and schema.
    let primary_table = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        other => return Err(err(format!("unsupported FROM: {:?}", other))),
    };
    let primary_schema = schema_resolver(&primary_table)
        .ok_or_else(|| err(format!("unknown table '{}'", primary_table)))?;

    let mut plan: LogicalPlan = LogicalPlan::Scan {
        schema: primary_schema.clone(),
    };

    // Process JOINs from from[0].joins
    for join in &from.joins {
        let right_table = match &join.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            other => return Err(err(format!("unsupported JOIN table: {:?}", other))),
        };
        let right_schema = schema_resolver(&right_table)
            .ok_or_else(|| err(format!("unknown table '{}'", right_table)))?;

        let right_plan = LogicalPlan::Scan {
            schema: right_schema,
        };

        let (join_type, on_expr) = parse_join_operator(&join.join_operator)?;

        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            join_type,
            on: on_expr,
        };
    }

    // WHERE clause.
    if let Some(where_expr) = select.selection {
        let predicate = sql_expr_to_expr_qualified(&where_expr)?;
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
                    let col = expr_to_col_name_qualified(expr)?;
                    projections.push(col);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some((_, agg)) = try_agg_func(expr)? {
                    agg_funcs.push((alias.value.clone(), agg));
                } else {
                    let _col = expr_to_col_name_qualified(expr)?;
                    projections.push(alias.value.clone());
                }
            }
            other => return Err(err(format!("unsupported SELECT item: {:?}", other))),
        }
    }

    // GROUP BY
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                group_by_cols.push(expr_to_col_name_qualified(e)?);
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

fn parse_join_operator(op: &JoinOperator) -> Result<(JoinType, Option<Expr>), SqlError> {
    match op {
        JoinOperator::Inner(constraint) => {
            let on_expr = parse_join_constraint(constraint)?;
            Ok((JoinType::Inner, on_expr))
        }
        JoinOperator::LeftOuter(constraint) => {
            let on_expr = parse_join_constraint(constraint)?;
            Ok((JoinType::Left, on_expr))
        }
        JoinOperator::RightOuter(constraint) => {
            let on_expr = parse_join_constraint(constraint)?;
            Ok((JoinType::Right, on_expr))
        }
        JoinOperator::CrossJoin => Ok((JoinType::Cross, None)),
        other => Err(err(format!("unsupported JOIN type: {:?}", other))),
    }
}

fn parse_join_constraint(constraint: &JoinConstraint) -> Result<Option<Expr>, SqlError> {
    match constraint {
        JoinConstraint::On(expr) => {
            let e = sql_expr_to_expr_qualified(expr)?;
            Ok(Some(e))
        }
        JoinConstraint::None => Ok(None),
        other => Err(err(format!("unsupported JOIN constraint: {:?}", other))),
    }
}

/// Like expr_to_col_name but joins compound identifiers with "." for qualified names.
fn expr_to_col_name_qualified(expr: &SqlExpr) -> Result<String, SqlError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            Ok(parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join("."))
        }
        other => Err(err(format!("expected column name, got: {:?}", other))),
    }
}

/// Like sql_expr_to_expr but handles compound identifiers with "." joining.
fn sql_expr_to_expr_qualified(expr: &SqlExpr) -> Result<Expr, SqlError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(Expr::col(ident.value.clone())),
        SqlExpr::CompoundIdentifier(parts) => Ok(Expr::col(
            parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join(".")
        )),
        SqlExpr::Value(v) => Ok(Expr::lit(sql_value_to_value(v)?)),
        SqlExpr::BinaryOp { left, op, right } => {
            let l = sql_expr_to_expr_qualified(left)?;
            let r = sql_expr_to_expr_qualified(right)?;
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
            let e = sql_expr_to_expr_qualified(expr)?;
            match op {
                UnaryOperator::Not => Ok(Expr::not(e)),
                other => Err(err(format!("unsupported unary op: {:?}", other))),
            }
        }
        SqlExpr::Nested(inner) => sql_expr_to_expr_qualified(inner),
        other => Err(err(format!("unsupported expression: {:?}", other))),
    }
}

fn translate_query(query: Query, schema: Schema) -> Result<LogicalPlan, SqlError> {
    let Query {
        body,
        order_by,
        limit,
        offset,
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

    // OFFSET
    if let Some(offset_clause) = offset {
        let n = match offset_clause.value {
            SqlExpr::Value(SqlValue::Number(s, _)) => s
                .parse::<usize>()
                .map_err(|_| err(format!("invalid OFFSET: {}", s)))?,
            other => return Err(err(format!("unsupported OFFSET expr: {:?}", other))),
        };
        plan = LogicalPlan::Offset {
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

    // HAVING — applied after aggregation.
    if let Some(having_expr) = select.having {
        let predicate = sql_expr_to_expr(&having_expr)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // DISTINCT
    if select.distinct.is_some() {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
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
                BinaryOperator::Plus => Ok(Expr::add(l, r)),
                BinaryOperator::Minus => Ok(Expr::sub(l, r)),
                BinaryOperator::Multiply => Ok(Expr::mul(l, r)),
                BinaryOperator::Divide => Ok(Expr::div(l, r)),
                BinaryOperator::Modulo => Ok(Expr::modulo(l, r)),
                other => Err(err(format!("unsupported operator: {:?}", other))),
            }
        }
        SqlExpr::UnaryOp { op, expr } => {
            let e = sql_expr_to_expr(expr)?;
            match op {
                UnaryOperator::Not => Ok(Expr::not(e)),
                UnaryOperator::Minus => Ok(Expr::negate(e)),
                other => Err(err(format!("unsupported unary op: {:?}", other))),
            }
        }
        SqlExpr::Nested(inner) => sql_expr_to_expr(inner),
        SqlExpr::IsNull(inner) => {
            let e = sql_expr_to_expr(inner)?;
            Ok(Expr::is_null(e))
        }
        SqlExpr::IsNotNull(inner) => {
            let e = sql_expr_to_expr(inner)?;
            Ok(Expr::is_not_null(e))
        }
        SqlExpr::Like { negated, expr, pattern, .. } => {
            let val = sql_expr_to_expr(expr)?;
            let pat = sql_expr_to_expr(pattern)?;
            let like_expr = Expr::like(val, pat);
            if *negated {
                Ok(Expr::not(like_expr))
            } else {
                Ok(like_expr)
            }
        }
        SqlExpr::InList { expr, list, negated } => {
            let val = sql_expr_to_expr(expr)?;
            let items: Vec<Expr> = list.iter().map(|e| sql_expr_to_expr(e)).collect::<Result<_, _>>()?;
            let in_expr = Expr::in_list(val, items);
            if *negated {
                Ok(Expr::not(in_expr))
            } else {
                Ok(in_expr)
            }
        }
        SqlExpr::Between { expr, negated, low, high } => {
            let val = sql_expr_to_expr(expr)?;
            let lo = sql_expr_to_expr(low)?;
            let hi = sql_expr_to_expr(high)?;
            let between_expr = Expr::between(val, lo, hi);
            if *negated {
                Ok(Expr::not(between_expr))
            } else {
                Ok(between_expr)
            }
        }
        SqlExpr::Case { operand, conditions, results, else_result } => {
            let op = match operand {
                Some(e) => Some(sql_expr_to_expr(e)?),
                None => None,
            };
            let when_clauses: Vec<(Expr, Expr)> = conditions
                .iter()
                .zip(results.iter())
                .map(|(c, r)| Ok((sql_expr_to_expr(c)?, sql_expr_to_expr(r)?)))
                .collect::<Result<_, SqlError>>()?;
            let else_r = match else_result {
                Some(e) => Some(sql_expr_to_expr(e)?),
                None => None,
            };
            Ok(Expr::case(op, when_clauses, else_r))
        }
        SqlExpr::TypedString { data_type, value } => {
            use crate::query::expr::{parse_timestamp_str, parse_date_str, parse_uuid_str};
            match data_type {
                sqlparser::ast::DataType::Timestamp(_, _) => {
                    let us = parse_timestamp_str(value)
                        .ok_or_else(|| err(format!("invalid timestamp: '{}'", value)))?;
                    Ok(Expr::lit(Value::Timestamp(us)))
                }
                sqlparser::ast::DataType::Date => {
                    let days = parse_date_str(value)
                        .ok_or_else(|| err(format!("invalid date: '{}'", value)))?;
                    Ok(Expr::lit(Value::Date(days)))
                }
                sqlparser::ast::DataType::Uuid => {
                    let bytes = parse_uuid_str(value)
                        .ok_or_else(|| err(format!("invalid UUID: '{}'", value)))?;
                    Ok(Expr::lit(Value::Uuid(bytes)))
                }
                _ => Ok(Expr::lit(Value::Text(value.clone()))),
            }
        }
        SqlExpr::Cast { expr, data_type, .. } => {
            use crate::query::expr::{parse_timestamp_str, parse_date_str, parse_uuid_str};
            // For SQL expressions in WHERE/SELECT, we evaluate CASTs at plan time if the inner is a literal.
            let inner = sql_expr_to_expr(expr)?;
            match data_type {
                sqlparser::ast::DataType::Uuid => {
                    // If it's a literal text, convert now.
                    if let Expr::Lit(Value::Text(s)) = &inner {
                        let bytes = parse_uuid_str(s)
                            .ok_or_else(|| err(format!("invalid UUID: '{}'", s)))?;
                        Ok(Expr::lit(Value::Uuid(bytes)))
                    } else {
                        Ok(inner)
                    }
                }
                sqlparser::ast::DataType::Timestamp(_, _) => {
                    if let Expr::Lit(Value::Text(s)) = &inner {
                        let us = parse_timestamp_str(s)
                            .ok_or_else(|| err(format!("invalid timestamp: '{}'", s)))?;
                        Ok(Expr::lit(Value::Timestamp(us)))
                    } else {
                        Ok(inner)
                    }
                }
                sqlparser::ast::DataType::Date => {
                    if let Expr::Lit(Value::Text(s)) = &inner {
                        let days = parse_date_str(s)
                            .ok_or_else(|| err(format!("invalid date: '{}'", s)))?;
                        Ok(Expr::lit(Value::Date(days)))
                    } else {
                        Ok(inner)
                    }
                }
                _ => Ok(inner),
            }
        }
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
