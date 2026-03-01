//! Stage 4 tests: query type system, builder, executor, SQL parser.

use std::collections::BTreeMap;

use rust_dst_db::query::builder::QueryBuilder;
use rust_dst_db::query::executor::execute;
use rust_dst_db::query::expr::{Column, Expr, Row, Schema, Value, ValueType};
use rust_dst_db::query::plan::{AggFunc, LogicalPlan, SortOrder};
use rust_dst_db::query::sql::sql_to_plan;

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

fn make_schema() -> Schema {
    Schema::new(
        "users",
        vec![
            Column::new("id", ValueType::Int64).not_null(),
            Column::new("name", ValueType::Text),
            Column::new("age", ValueType::Int64),
            Column::new("active", ValueType::Bool),
        ],
    )
}

fn make_row(id: i64, name: &str, age: i64, active: bool) -> Row {
    let mut row = BTreeMap::new();
    row.insert("id".into(), Value::Int64(id));
    row.insert("name".into(), Value::Text(name.into()));
    row.insert("age".into(), Value::Int64(age));
    row.insert("active".into(), Value::Bool(active));
    row
}

fn sample_rows() -> Vec<Row> {
    vec![
        make_row(1, "Alice", 30, true),
        make_row(2, "Bob", 25, false),
        make_row(3, "Carol", 35, true),
        make_row(4, "Dave", 22, true),
        make_row(5, "Eve", 28, false),
    ]
}

// ═══════════════════════════════════════════════════════════════════════
// Value type system
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn value_encode_decode_roundtrip() {
    let values = vec![
        Value::Null,
        Value::Bool(true),
        Value::Bool(false),
        Value::Int64(42),
        Value::Int64(-1),
        Value::Float64(3.14),
        Value::Text("hello world".into()),
        Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    ];

    for v in &values {
        let encoded = v.encode();
        let (decoded, consumed) = Value::decode(&encoded).expect("decode failed");
        assert_eq!(&decoded, v, "roundtrip failed for {:?}", v);
        assert_eq!(consumed, encoded.len());
    }
}

#[test]
fn schema_encode_decode_row() {
    let schema = make_schema();
    let row = make_row(42, "Alice", 30, true);

    let encoded = schema.encode_row(&row);
    let decoded = schema.decode_row(&encoded).expect("decode failed");

    assert_eq!(decoded["id"], Value::Int64(42));
    assert_eq!(decoded["name"], Value::Text("Alice".into()));
    assert_eq!(decoded["age"], Value::Int64(30));
    assert_eq!(decoded["active"], Value::Bool(true));
}

#[test]
fn schema_make_key() {
    let schema = make_schema();
    let key_a = schema.make_key(&Value::Int64(1));
    let key_b = schema.make_key(&Value::Int64(2));
    assert_ne!(key_a, key_b);
    // Key should contain the table name as prefix.
    assert!(key_a.starts_with(b"users\x00"));
}

// ═══════════════════════════════════════════════════════════════════════
// Expression evaluation
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn expr_col_lookup() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::col("age").eval(&row), Value::Int64(30));
    assert_eq!(Expr::col("missing").eval(&row), Value::Null);
}

#[test]
fn expr_comparisons() {
    let row = make_row(1, "Alice", 30, true);

    assert_eq!(
        Expr::eq(Expr::col("age"), Expr::lit(30i64)).eval(&row),
        Value::Bool(true)
    );
    assert_eq!(
        Expr::gt(Expr::col("age"), Expr::lit(25i64)).eval(&row),
        Value::Bool(true)
    );
    assert_eq!(
        Expr::lt(Expr::col("age"), Expr::lit(25i64)).eval(&row),
        Value::Bool(false)
    );
    assert_eq!(
        Expr::ne(Expr::col("name"), Expr::lit("Bob")).eval(&row),
        Value::Bool(true)
    );
}

#[test]
fn expr_logical_operators() {
    let row = make_row(1, "Alice", 30, true);

    let both_true = Expr::and(
        Expr::gt(Expr::col("age"), Expr::lit(20i64)),
        Expr::eq(Expr::col("active"), Expr::lit(true)),
    );
    assert_eq!(both_true.eval(&row), Value::Bool(true));

    let one_false = Expr::and(
        Expr::gt(Expr::col("age"), Expr::lit(20i64)),
        Expr::eq(Expr::col("active"), Expr::lit(false)),
    );
    assert_eq!(one_false.eval(&row), Value::Bool(false));

    let or_expr = Expr::or(
        Expr::eq(Expr::col("name"), Expr::lit("Bob")),
        Expr::eq(Expr::col("name"), Expr::lit("Alice")),
    );
    assert_eq!(or_expr.eval(&row), Value::Bool(true));

    assert_eq!(
        Expr::not(Expr::lit(false)).eval(&row),
        Value::Bool(true)
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Executor: Filter
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn executor_filter() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan { schema }),
        predicate: Expr::gt(Expr::col("age"), Expr::lit(27i64)),
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 3); // Alice(30), Carol(35), Eve(28)

    let ages: Vec<i64> = result
        .iter()
        .map(|r| match r["age"] {
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert!(ages.iter().all(|&a| a > 27));
}

#[test]
fn executor_filter_none_match() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan { schema }),
        predicate: Expr::gt(Expr::col("age"), Expr::lit(100i64)),
    };

    let result = execute(&plan, rows);
    assert!(result.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════
// Executor: Project
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn executor_project() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan { schema }),
        columns: vec!["id".into(), "name".into()],
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 5);
    for row in &result {
        assert!(row.contains_key("id"));
        assert!(row.contains_key("name"));
        assert!(!row.contains_key("age"));
        assert!(!row.contains_key("active"));
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Executor: Sort
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn executor_sort_asc() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Sort {
        input: Box::new(LogicalPlan::Scan { schema }),
        keys: vec![("age".into(), SortOrder::Asc)],
    };

    let result = execute(&plan, rows);
    let ages: Vec<i64> = result
        .iter()
        .map(|r| match r["age"] {
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ages, vec![22, 25, 28, 30, 35]);
}

#[test]
fn executor_sort_desc() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Sort {
        input: Box::new(LogicalPlan::Scan { schema }),
        keys: vec![("age".into(), SortOrder::Desc)],
    };

    let result = execute(&plan, rows);
    let ages: Vec<i64> = result
        .iter()
        .map(|r| match r["age"] {
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ages, vec![35, 30, 28, 25, 22]);
}

// ═══════════════════════════════════════════════════════════════════════
// Executor: Limit
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn executor_limit() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Limit {
        input: Box::new(LogicalPlan::Scan { schema }),
        n: 3,
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════
// Executor: Aggregates
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn executor_aggregate_count() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan { schema }),
        group_by: vec![],
        agg_funcs: vec![("count".into(), AggFunc::Count)],
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["count"], Value::Int64(5));
}

#[test]
fn executor_aggregate_sum() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan { schema }),
        group_by: vec![],
        agg_funcs: vec![("total_age".into(), AggFunc::Sum("age".into()))],
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 1);
    // 30 + 25 + 35 + 22 + 28 = 140
    assert_eq!(result[0]["total_age"], Value::Int64(140));
}

#[test]
fn executor_aggregate_min_max() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan { schema }),
        group_by: vec![],
        agg_funcs: vec![
            ("min_age".into(), AggFunc::Min("age".into())),
            ("max_age".into(), AggFunc::Max("age".into())),
        ],
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["min_age"], Value::Int64(22));
    assert_eq!(result[0]["max_age"], Value::Int64(35));
}

#[test]
fn executor_aggregate_avg() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan { schema }),
        group_by: vec![],
        agg_funcs: vec![("avg_age".into(), AggFunc::Avg("age".into()))],
    };

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 1);
    // 140 / 5 = 28.0
    assert_eq!(result[0]["avg_age"], Value::Float64(28.0));
}

#[test]
fn executor_group_by() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan { schema }),
        group_by: vec!["active".into()],
        agg_funcs: vec![("count".into(), AggFunc::Count)],
    };

    let mut result = execute(&plan, rows);
    // Sort by active for deterministic assertion.
    result.sort_by_key(|r| match r.get("active") {
        Some(Value::Bool(b)) => *b as i64,
        _ => -1,
    });

    assert_eq!(result.len(), 2);
    // active=false: Bob, Eve → 2
    assert_eq!(result[0]["active"], Value::Bool(false));
    assert_eq!(result[0]["count"], Value::Int64(2));
    // active=true: Alice, Carol, Dave → 3
    assert_eq!(result[1]["active"], Value::Bool(true));
    assert_eq!(result[1]["count"], Value::Int64(3));
}

// ═══════════════════════════════════════════════════════════════════════
// QueryBuilder — fluent API
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn query_builder_filter_project_sort_limit() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = QueryBuilder::from(schema)
        .filter(Expr::eq(Expr::col("active"), Expr::lit(true)))
        .select(["name", "age"])
        .order_by("age", SortOrder::Desc)
        .limit(2)
        .build();

    let result = execute(&plan, rows);

    assert_eq!(result.len(), 2);
    // active=true: Alice(30), Carol(35), Dave(22) → sorted desc → Carol(35), Alice(30) → top 2
    assert_eq!(result[0]["name"], Value::Text("Carol".into()));
    assert_eq!(result[1]["name"], Value::Text("Alice".into()));
    // Projected: only name + age.
    assert!(!result[0].contains_key("id"));
}

#[test]
fn query_builder_produces_correct_plan_structure() {
    let schema = make_schema();

    let plan = QueryBuilder::from(schema.clone())
        .filter(Expr::gt(Expr::col("age"), Expr::lit(20i64)))
        .select(["id", "name"])
        .build();

    // Verify the plan tree structure.
    match &plan {
        LogicalPlan::Project { input, columns } => {
            assert_eq!(columns, &["id", "name"]);
            match input.as_ref() {
                LogicalPlan::Filter { input: scan, .. } => {
                    assert!(matches!(scan.as_ref(), LogicalPlan::Scan { .. }));
                }
                other => panic!("Expected Filter, got {:?}", other),
            }
        }
        other => panic!("Expected Project, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// SQL parser → LogicalPlan
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn sql_select_star() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT * FROM users", schema).expect("parse failed");
    let result = execute(&plan, rows);
    assert_eq!(result.len(), 5);
}

#[test]
fn sql_select_with_where() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan(
        "SELECT * FROM users WHERE age > 27",
        schema,
    )
    .expect("parse failed");

    let result = execute(&plan, rows);
    assert_eq!(result.len(), 3); // Alice(30), Carol(35), Eve(28)
}

#[test]
fn sql_select_columns() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT name, age FROM users", schema).expect("parse failed");
    let result = execute(&plan, rows);

    assert_eq!(result.len(), 5);
    for row in &result {
        assert!(row.contains_key("name"));
        assert!(row.contains_key("age"));
        assert!(!row.contains_key("id"));
    }
}

#[test]
fn sql_select_order_by() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT * FROM users ORDER BY age ASC", schema)
        .expect("parse failed");
    let result = execute(&plan, rows);

    let ages: Vec<i64> = result
        .iter()
        .map(|r| match r["age"] {
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ages, vec![22, 25, 28, 30, 35]);
}

#[test]
fn sql_select_limit() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT * FROM users LIMIT 2", schema).expect("parse failed");
    let result = execute(&plan, rows);
    assert_eq!(result.len(), 2);
}

#[test]
fn sql_select_count_star() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT COUNT(*) FROM users", schema).expect("parse failed");
    let result = execute(&plan, rows);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["count"], Value::Int64(5));
}

#[test]
fn sql_select_sum() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan("SELECT SUM(age) AS total FROM users", schema)
        .expect("parse failed");
    let result = execute(&plan, rows);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["total"], Value::Int64(140));
}

#[test]
fn sql_compound_where() {
    let schema = make_schema();
    let rows = sample_rows();

    let plan = sql_to_plan(
        "SELECT * FROM users WHERE age > 25 AND active = true",
        schema,
    )
    .expect("parse failed");

    let result = execute(&plan, rows);
    // Alice(30,true), Carol(35,true): age>25 AND active=true
    assert_eq!(result.len(), 2);
}

#[test]
fn sql_parse_error_wrong_table() {
    let schema = make_schema();
    let result = sql_to_plan("SELECT * FROM wrong_table", schema);
    assert!(result.is_err());
}
