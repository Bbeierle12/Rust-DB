use std::collections::BTreeMap;

use crate::query::expr::{Row, Value};
use crate::query::plan::{AggFunc, LogicalPlan, SortOrder};

/// Execute a LogicalPlan against an input set of rows.
///
/// Pure function — no state, no messages. The caller is responsible for
/// supplying the rows (e.g. from a TxnScan followed by schema decoding).
pub fn execute(plan: &LogicalPlan, input: Vec<Row>) -> Vec<Row> {
    match plan {
        LogicalPlan::Scan { .. } => {
            // Scan is resolved externally; by the time execute() is called,
            // the input rows are already provided.
            input
        }

        LogicalPlan::Filter { input: child, predicate } => {
            let rows = execute(child, input);
            rows.into_iter().filter(|row| predicate.is_true(row)).collect()
        }

        LogicalPlan::Project { input: child, columns } => {
            let rows = execute(child, input);
            rows.into_iter()
                .map(|row| {
                    let mut projected = BTreeMap::new();
                    for col in columns {
                        if let Some(val) = row.get(col) {
                            projected.insert(col.clone(), val.clone());
                        } else {
                            projected.insert(col.clone(), Value::Null);
                        }
                    }
                    projected
                })
                .collect()
        }

        LogicalPlan::Sort { input: child, keys } => {
            let mut rows = execute(child, input);
            rows.sort_by(|a, b| {
                for (col, order) in keys {
                    let av = a.get(col).unwrap_or(&Value::Null);
                    let bv = b.get(col).unwrap_or(&Value::Null);
                    let cmp = value_cmp(av, bv);
                    let cmp = if matches!(order, SortOrder::Desc) {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
            rows
        }

        LogicalPlan::Limit { input: child, n } => {
            let rows = execute(child, input);
            rows.into_iter().take(*n).collect()
        }

        LogicalPlan::Aggregate {
            input: child,
            group_by,
            agg_funcs,
        } => {
            let rows = execute(child, input);

            if group_by.is_empty() {
                // Global aggregation — single output row.
                let mut result_row: BTreeMap<String, Value> = BTreeMap::new();
                for (out_col, func) in agg_funcs {
                    let val = apply_agg(func, &rows);
                    result_row.insert(out_col.clone(), val);
                }
                if result_row.is_empty() {
                    vec![]
                } else {
                    vec![result_row]
                }
            } else {
                // Grouped aggregation.
                // Group rows by the group_by key (as a Vec<Value>).
                let mut groups: BTreeMap<Vec<u8>, (Row, Vec<Row>)> = BTreeMap::new();

                for row in rows {
                    // Build a deterministic group key from BTreeMap of group_by cols.
                    let mut group_key_vals: Vec<Value> = group_by
                        .iter()
                        .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                        .collect();
                    let group_key = encode_group_key(&group_key_vals);

                    let entry = groups.entry(group_key).or_insert_with(|| {
                        // Capture group-by columns for the output row.
                        let mut rep_row = BTreeMap::new();
                        for (col, val) in group_by.iter().zip(group_key_vals.drain(..)) {
                            rep_row.insert(col.clone(), val);
                        }
                        (rep_row, Vec::new())
                    });
                    entry.1.push(row);
                }

                groups
                    .into_values()
                    .map(|(mut rep_row, group_rows)| {
                        for (out_col, func) in agg_funcs {
                            let val = apply_agg(func, &group_rows);
                            rep_row.insert(out_col.clone(), val);
                        }
                        rep_row
                    })
                    .collect()
            }
        }
    }
}

/// Apply an aggregate function over a set of rows.
fn apply_agg(func: &AggFunc, rows: &[Row]) -> Value {
    match func {
        AggFunc::Count => Value::Int64(rows.len() as i64),

        AggFunc::Sum(col) => {
            let mut sum_i = 0i64;
            let mut sum_f = 0.0f64;
            let mut is_float = false;
            for row in rows {
                match row.get(col) {
                    Some(Value::Int64(n)) => sum_i += n,
                    Some(Value::Float64(f)) => {
                        is_float = true;
                        sum_f += f;
                    }
                    _ => {}
                }
            }
            if is_float {
                Value::Float64(sum_f + sum_i as f64)
            } else {
                Value::Int64(sum_i)
            }
        }

        AggFunc::Min(col) => {
            rows.iter()
                .filter_map(|r| r.get(col))
                .filter(|v| !v.is_null())
                .min_by(|a, b| value_cmp(a, b))
                .cloned()
                .unwrap_or(Value::Null)
        }

        AggFunc::Max(col) => {
            rows.iter()
                .filter_map(|r| r.get(col))
                .filter(|v| !v.is_null())
                .max_by(|a, b| value_cmp(a, b))
                .cloned()
                .unwrap_or(Value::Null)
        }

        AggFunc::Avg(col) => {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for row in rows {
                match row.get(col) {
                    Some(Value::Int64(n)) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Some(Value::Float64(f)) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                Value::Null
            } else {
                Value::Float64(sum / count as f64)
            }
        }
    }
}

/// Total order for Value (for sorting/min/max).
fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use Value::*;
    match (a, b) {
        (Null, Null) => std::cmp::Ordering::Equal,
        (Null, _) => std::cmp::Ordering::Less,
        (_, Null) => std::cmp::Ordering::Greater,
        (Int64(x), Int64(y)) => x.cmp(y),
        (Float64(x), Float64(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Int64(x), Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Float64(x), Int64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Text(x), Text(y)) => x.cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        // Cross-type: arbitrary but stable ordering by type tag.
        _ => type_tag(a).cmp(&type_tag(b)),
    }
}

fn type_tag(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int64(_) => 2,
        Value::Float64(_) => 3,
        Value::Text(_) => 4,
        Value::Bytes(_) => 5,
    }
}

fn encode_group_key(vals: &[Value]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.encode()).collect()
}
