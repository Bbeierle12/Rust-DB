use crate::query::expr::{Expr, Schema, Value};

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Join type for multi-table queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
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
    /// Remove duplicate rows.
    Distinct {
        input: Box<LogicalPlan>,
    },
    /// Skip the first n rows.
    Offset {
        input: Box<LogicalPlan>,
        n: usize,
    },
    /// Join two plans together.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        on: Option<Expr>,
    },
    /// Index scan: look up rows via a secondary index.
    IndexScan {
        schema: Schema,
        index_name: String,
        lookup_values: Vec<Value>,
    },
}

impl LogicalPlan {
    /// Return the schema (table name) at the root of this plan, if any.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            LogicalPlan::Scan { schema } => Some(&schema.table),
            LogicalPlan::IndexScan { schema, .. } => Some(&schema.table),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Offset { input, .. } => input.table_name(),
            LogicalPlan::Join { left, .. } => left.table_name(),
        }
    }

    /// Return the root schema if this plan starts with a Scan.
    pub fn root_schema(&self) -> Option<&Schema> {
        match self {
            LogicalPlan::Scan { schema } => Some(schema),
            LogicalPlan::IndexScan { schema, .. } => Some(schema),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Offset { input, .. } => input.root_schema(),
            LogicalPlan::Join { left, .. } => left.root_schema(),
        }
    }

    /// Extract the projected column names if the outermost node is a Project.
    /// Returns `None` for SELECT * (no explicit projection) or non-project plans.
    pub fn project_columns(&self) -> Option<Vec<String>> {
        match self {
            LogicalPlan::Project { columns, .. } => Some(columns.clone()),
            // Sort/Limit/Offset/Distinct wrap a Project — look inside.
            LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Offset { input, .. }
            | LogicalPlan::Distinct { input, .. } => input.project_columns(),
            _ => None,
        }
    }

    /// Collect all table names referenced in this plan.
    pub fn collect_table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        self.collect_table_names_inner(&mut names);
        names
    }

    fn collect_table_names_inner(&self, names: &mut Vec<String>) {
        match self {
            LogicalPlan::Scan { schema } => {
                if !names.contains(&schema.table) {
                    names.push(schema.table.clone());
                }
            }
            LogicalPlan::IndexScan { schema, .. } => {
                if !names.contains(&schema.table) {
                    names.push(schema.table.clone());
                }
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Offset { input, .. } => input.collect_table_names_inner(names),
            LogicalPlan::Join { left, right, .. } => {
                left.collect_table_names_inner(names);
                right.collect_table_names_inner(names);
            }
        }
    }
}
