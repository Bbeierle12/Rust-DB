use crate::query::expr::{Expr, Schema};
use crate::query::plan::{AggFunc, LogicalPlan, SortOrder};

/// Composable query builder — the Rust-native API differentiator.
///
/// Models after Ecto's pipe-based composition: each method returns a new
/// builder that wraps the previous plan. Compiles to a LogicalPlan that
/// bypasses SQL parsing entirely.
///
/// Build a plan with: `from(schema).filter(expr).select(cols).order_by(col, order).limit(n).build()`
pub struct QueryBuilder {
    plan: LogicalPlan,
}

impl QueryBuilder {
    /// Start a query against a table described by a schema.
    pub fn from(schema: Schema) -> Self {
        Self {
            plan: LogicalPlan::Scan { schema },
        }
    }

    /// Build from an existing LogicalPlan (for sub-query composition).
    pub fn from_plan(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    /// Filter rows matching the given predicate.
    pub fn filter(self, predicate: Expr) -> Self {
        Self {
            plan: LogicalPlan::Filter {
                input: Box::new(self.plan),
                predicate,
            },
        }
    }

    /// Project (keep) only the listed columns.
    pub fn select(self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            plan: LogicalPlan::Project {
                input: Box::new(self.plan),
                columns: columns.into_iter().map(|c| c.into()).collect(),
            },
        }
    }

    /// Sort by a column with the given order.
    pub fn order_by(self, column: impl Into<String>, order: SortOrder) -> Self {
        let column = column.into();
        match self.plan {
            // Merge multiple order_by calls into a single Sort node.
            LogicalPlan::Sort { input, mut keys } => {
                keys.push((column, order));
                Self {
                    plan: LogicalPlan::Sort { input, keys },
                }
            }
            other => Self {
                plan: LogicalPlan::Sort {
                    input: Box::new(other),
                    keys: vec![(column, order)],
                },
            },
        }
    }

    /// Limit the number of output rows.
    pub fn limit(self, n: usize) -> Self {
        Self {
            plan: LogicalPlan::Limit {
                input: Box::new(self.plan),
                n,
            },
        }
    }

    /// Aggregate with optional grouping.
    pub fn aggregate(
        self,
        group_by: impl IntoIterator<Item = impl Into<String>>,
        agg_funcs: impl IntoIterator<Item = (impl Into<String>, AggFunc)>,
    ) -> Self {
        Self {
            plan: LogicalPlan::Aggregate {
                input: Box::new(self.plan),
                group_by: group_by.into_iter().map(|c| c.into()).collect(),
                agg_funcs: agg_funcs.into_iter().map(|(n, f)| (n.into(), f)).collect(),
            },
        }
    }

    /// Consume the builder and return the final LogicalPlan.
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}
