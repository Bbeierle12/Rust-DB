use crate::query::expr::{Expr, Schema};

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Aggregate function.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum(String),  // column name
    Min(String),
    Max(String),
    Avg(String),
}

/// A logical query plan.
///
/// Built by the QueryBuilder, evaluated by the Executor.
/// Uses Box<LogicalPlan> for recursive plans (not trait objects).
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Full table scan with schema.
    Scan {
        schema: Schema,
    },
    /// Filter rows matching a predicate.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Project a subset of columns.
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    /// Sort rows by one or more columns.
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<(String, SortOrder)>,
    },
    /// Limit the number of output rows.
    Limit {
        input: Box<LogicalPlan>,
        n: usize,
    },
    /// Compute aggregate functions, optionally grouped.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        agg_funcs: Vec<(String, AggFunc)>, // (output_col_name, func)
    },
}

impl LogicalPlan {
    /// Return the schema (table name) at the root of this plan, if any.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            LogicalPlan::Scan { schema } => Some(&schema.table),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. } => input.table_name(),
        }
    }

    /// Return the root schema if this plan starts with a Scan.
    pub fn root_schema(&self) -> Option<&Schema> {
        match self {
            LogicalPlan::Scan { schema } => Some(schema),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. } => input.root_schema(),
        }
    }
}
