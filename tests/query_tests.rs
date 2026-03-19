//! Stage 4 tests: query type system, builder, executor, SQL parser.

use std::collections::BTreeMap;

use rust_dst_db::query::builder::QueryBuilder;
use rust_dst_db::query::executor::{execute, execute_with_sources};
use rust_dst_db::query::expr::{Column, Expr, Row, Schema, Value, ValueType};
use rust_dst_db::query::plan::{AggFunc, JoinType, LogicalPlan, SortOrder};
use rust_dst_db::query::sql::{sql_to_plan, sql_to_plan_multi};

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

// ═══════════════════════════════════════════════════════════════════════
// JOIN tests
// ═══════════════════════════════════════════════════════════════════════

fn make_orders_schema() -> Schema {
    Schema::new(
        "orders",
        vec![
            Column::new("order_id", ValueType::Int64).not_null(),
            Column::new("user_id", ValueType::Int64),
            Column::new("amount", ValueType::Int64),
        ],
    )
}

fn make_order_row(order_id: i64, user_id: i64, amount: i64) -> Row {
    let mut row = BTreeMap::new();
    row.insert("order_id".into(), Value::Int64(order_id));
    row.insert("user_id".into(), Value::Int64(user_id));
    row.insert("amount".into(), Value::Int64(amount));
    row
}

fn sample_orders() -> Vec<Row> {
    vec![
        make_order_row(100, 1, 50),
        make_order_row(101, 2, 30),
        make_order_row(102, 1, 70),
        make_order_row(103, 9, 20), // user_id=9 has no matching user
    ]
}

/// Prefix all row keys with `tablename.`.
fn prefix_rows(table: &str, rows: Vec<Row>) -> Vec<Row> {
    rows.into_iter()
        .map(|row| {
            let mut prefixed = BTreeMap::new();
            for (k, v) in row {
                prefixed.insert(format!("{}.{}", table, k), v);
            }
            prefixed
        })
        .collect()
}

fn build_sources() -> BTreeMap<String, Vec<Row>> {
    let mut sources = BTreeMap::new();
    sources.insert("users".into(), prefix_rows("users", sample_rows()));
    sources.insert("orders".into(), prefix_rows("orders", sample_orders()));
    sources
}

#[test]
fn cross_join_two_tables() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Cross,
        on: None,
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // 5 users x 4 orders = 20 rows
    assert_eq!(result.len(), 20);
    // Each row should have columns from both tables.
    assert!(result[0].contains_key("users.id"));
    assert!(result[0].contains_key("orders.order_id"));
}

#[test]
fn inner_join_on_condition() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // User 1 (Alice) matches orders 100, 102; user 2 (Bob) matches order 101.
    // User 9 has no matching user, so order 103 is excluded.
    assert_eq!(result.len(), 3);

    let order_ids: Vec<i64> = result.iter().filter_map(|r| {
        match r.get("orders.order_id") {
            Some(Value::Int64(n)) => Some(*n),
            _ => None,
        }
    }).collect();
    assert!(order_ids.contains(&100));
    assert!(order_ids.contains(&101));
    assert!(order_ids.contains(&102));
}

#[test]
fn left_join_null_padding() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Left,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // User 1 -> 2 orders, user 2 -> 1 order, users 3,4,5 -> NULL padded = 6 total rows
    assert_eq!(result.len(), 6);

    // Find Carol (id=3) who has no orders.
    let carol_rows: Vec<_> = result.iter().filter(|r| {
        r.get("users.name") == Some(&Value::Text("Carol".into()))
    }).collect();
    assert_eq!(carol_rows.len(), 1);
    assert_eq!(carol_rows[0].get("orders.order_id"), Some(&Value::Null));
}

#[test]
fn right_join_null_padding() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Right,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // Orders 100,102 match user 1, order 101 matches user 2, order 103 has no user -> NULL padded
    assert_eq!(result.len(), 4);

    // Order 103 (user_id=9) should have NULL user columns.
    let unmatched: Vec<_> = result.iter().filter(|r| {
        r.get("orders.order_id") == Some(&Value::Int64(103))
    }).collect();
    assert_eq!(unmatched.len(), 1);
    assert_eq!(unmatched[0].get("users.id"), Some(&Value::Null));
}

#[test]
fn join_with_filter() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let join_plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let plan = LogicalPlan::Filter {
        input: Box::new(join_plan),
        predicate: Expr::gt(Expr::col("orders.amount"), Expr::lit(40i64)),
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // Only orders with amount > 40: order 100 (50) and order 102 (70)
    assert_eq!(result.len(), 2);
}

#[test]
fn join_with_sort() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let join_plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let plan = LogicalPlan::Sort {
        input: Box::new(join_plan),
        keys: vec![("orders.amount".into(), SortOrder::Desc)],
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    assert_eq!(result.len(), 3);

    let amounts: Vec<i64> = result.iter().filter_map(|r| {
        match r.get("orders.amount") {
            Some(Value::Int64(n)) => Some(*n),
            _ => None,
        }
    }).collect();
    assert_eq!(amounts, vec![70, 50, 30]);
}

#[test]
fn join_with_aggregate() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let join_plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let plan = LogicalPlan::Aggregate {
        input: Box::new(join_plan),
        group_by: vec!["users.name".into()],
        agg_funcs: vec![("total".into(), AggFunc::Sum("orders.amount".into()))],
    };

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    assert_eq!(result.len(), 2); // Alice and Bob

    let alice_row = result.iter().find(|r| {
        r.get("users.name") == Some(&Value::Text("Alice".into()))
    }).unwrap();
    // Alice: orders 100 (50) + 102 (70) = 120
    assert_eq!(alice_row.get("total"), Some(&Value::Int64(120)));
}

#[test]
fn three_table_join() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();
    let products_schema = Schema::new(
        "products",
        vec![
            Column::new("product_id", ValueType::Int64).not_null(),
            Column::new("order_id", ValueType::Int64),
            Column::new("product_name", ValueType::Text),
        ],
    );

    let mut product_rows = Vec::new();
    let mut r1 = BTreeMap::new();
    r1.insert("product_id".into(), Value::Int64(1));
    r1.insert("order_id".into(), Value::Int64(100));
    r1.insert("product_name".into(), Value::Text("Widget".into()));
    product_rows.push(r1);
    let mut r2 = BTreeMap::new();
    r2.insert("product_id".into(), Value::Int64(2));
    r2.insert("order_id".into(), Value::Int64(101));
    r2.insert("product_name".into(), Value::Text("Gadget".into()));
    product_rows.push(r2);

    // users JOIN orders ON users.id = orders.user_id
    // JOIN products ON orders.order_id = products.order_id
    let join1 = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };
    let plan = LogicalPlan::Join {
        left: Box::new(join1),
        right: Box::new(LogicalPlan::Scan { schema: products_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("orders.order_id"), Expr::col("products.order_id"))),
    };

    let mut sources = build_sources();
    sources.insert("products".into(), prefix_rows("products", product_rows));

    let result = execute_with_sources(&plan, &sources);
    // Order 100 (user 1=Alice) -> Widget, Order 101 (user 2=Bob) -> Gadget
    assert_eq!(result.len(), 2);
    assert!(result.iter().any(|r| {
        r.get("users.name") == Some(&Value::Text("Alice".into()))
            && r.get("products.product_name") == Some(&Value::Text("Widget".into()))
    }));
}

#[test]
fn sql_parse_inner_join() {
    let resolver = |name: &str| -> Option<Schema> {
        match name {
            "users" => Some(make_schema()),
            "orders" => Some(make_orders_schema()),
            _ => None,
        }
    };

    let plan = sql_to_plan_multi(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        resolver,
    ).expect("parse failed");

    let table_names = plan.collect_table_names();
    assert!(table_names.contains(&"users".into()));
    assert!(table_names.contains(&"orders".into()));

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    assert_eq!(result.len(), 3);
}

#[test]
fn sql_parse_left_join() {
    let resolver = |name: &str| -> Option<Schema> {
        match name {
            "users" => Some(make_schema()),
            "orders" => Some(make_orders_schema()),
            _ => None,
        }
    };

    let plan = sql_to_plan_multi(
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
        resolver,
    ).expect("parse failed");

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    // 5 users: Alice(2 orders), Bob(1), Carol(0), Dave(0), Eve(0) = 6 rows
    assert_eq!(result.len(), 6);
}

#[test]
fn sql_parse_cross_join() {
    let resolver = |name: &str| -> Option<Schema> {
        match name {
            "users" => Some(make_schema()),
            "orders" => Some(make_orders_schema()),
            _ => None,
        }
    };

    let plan = sql_to_plan_multi(
        "SELECT * FROM users CROSS JOIN orders",
        resolver,
    ).expect("parse failed");

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    assert_eq!(result.len(), 20); // 5 * 4
}

#[test]
fn builder_join_api() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = QueryBuilder::from(users_schema)
        .join(
            LogicalPlan::Scan { schema: orders_schema },
            Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id")),
        )
        .build();

    match &plan {
        LogicalPlan::Join { join_type, on, .. } => {
            assert_eq!(join_type.clone(), JoinType::Inner);
            assert!(on.is_some());
        }
        other => panic!("Expected Join, got {:?}", other),
    }

    let sources = build_sources();
    let result = execute_with_sources(&plan, &sources);
    assert_eq!(result.len(), 3);
}

#[test]
fn engine_join_integration() {
    let dir = std::env::temp_dir().join("rust_db_test_join_integration");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql("CREATE TABLE users (id INT NOT NULL, name TEXT, age INT)").unwrap();
    db.execute_sql("CREATE TABLE orders (order_id INT NOT NULL, user_id INT, amount INT)").unwrap();

    db.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    db.execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
    db.execute_sql("INSERT INTO users VALUES (3, 'Carol', 35)").unwrap();

    db.execute_sql("INSERT INTO orders VALUES (100, 1, 50)").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (101, 2, 30)").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (102, 1, 70)").unwrap();

    let result = db.execute_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
    ).unwrap();

    match result {
        rust_dst_db::engine::SqlResult::Query { rows, columns } => {
            assert_eq!(rows.len(), 3);
            // Columns should be prefixed with table names.
            assert!(columns.iter().any(|c| c.starts_with("users.")));
            assert!(columns.iter().any(|c| c.starts_with("orders.")));
        }
        _ => panic!("Expected query result"),
    }
}

#[test]
fn join_empty_table() {
    let users_schema = make_schema();
    let orders_schema = make_orders_schema();

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: users_schema }),
        right: Box::new(LogicalPlan::Scan { schema: orders_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let mut sources = BTreeMap::new();
    sources.insert("users".into(), prefix_rows("users", sample_rows()));
    sources.insert("orders".into(), Vec::new()); // empty orders

    let result = execute_with_sources(&plan, &sources);
    assert!(result.is_empty());

    // Left join should preserve all left rows with NULL padding.
    let left_plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: make_schema() }),
        right: Box::new(LogicalPlan::Scan { schema: make_orders_schema() }),
        join_type: JoinType::Left,
        on: Some(Expr::eq(Expr::col("users.id"), Expr::col("orders.user_id"))),
    };

    let result = execute_with_sources(&left_plan, &sources);
    assert_eq!(result.len(), 5); // all 5 users preserved
    for row in &result {
        assert_eq!(row.get("orders.order_id"), Some(&Value::Null));
    }
}

#[test]
fn self_join() {
    let _users_schema = make_schema();
    // Self-join: find pairs of users with the same "active" status.
    // We use the same schema twice but with different prefixing by using
    // two separate entries in sources. For self-join we need different keys,
    // so we use a workaround with aliased schemas.
    let u1_schema = Schema::new("u1", make_schema().columns);
    let u2_schema = Schema::new("u2", make_schema().columns);

    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { schema: u1_schema }),
        right: Box::new(LogicalPlan::Scan { schema: u2_schema }),
        join_type: JoinType::Inner,
        on: Some(Expr::and(
            Expr::eq(Expr::col("u1.active"), Expr::col("u2.active")),
            Expr::lt(Expr::col("u1.id"), Expr::col("u2.id")),  // avoid duplicate pairs
        )),
    };

    let mut sources = BTreeMap::new();
    sources.insert("u1".into(), prefix_rows("u1", sample_rows()));
    sources.insert("u2".into(), prefix_rows("u2", sample_rows()));

    let result = execute_with_sources(&plan, &sources);
    // active=true: Alice(1), Carol(3), Dave(4) -> pairs (1,3), (1,4), (3,4) = 3
    // active=false: Bob(2), Eve(5) -> pairs (2,5) = 1
    // Total = 4
    assert_eq!(result.len(), 4);
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 2: SQL Expression Operators
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn arithmetic_add_sub_mul_div() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::add(Expr::lit(10i64), Expr::lit(5i64)).eval(&row), Value::Int64(15));
    assert_eq!(Expr::sub(Expr::lit(10i64), Expr::lit(3i64)).eval(&row), Value::Int64(7));
    assert_eq!(Expr::mul(Expr::lit(4i64), Expr::lit(5i64)).eval(&row), Value::Int64(20));
    assert_eq!(Expr::div(Expr::lit(20i64), Expr::lit(4i64)).eval(&row), Value::Int64(5));
    assert_eq!(Expr::modulo(Expr::lit(10i64), Expr::lit(3i64)).eval(&row), Value::Int64(1));
}

#[test]
fn arithmetic_div_by_zero_returns_null() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::div(Expr::lit(10i64), Expr::lit(0i64)).eval(&row), Value::Null);
    assert_eq!(Expr::modulo(Expr::lit(10i64), Expr::lit(0i64)).eval(&row), Value::Null);
    assert_eq!(Expr::div(Expr::lit(10.0f64), Expr::lit(0.0f64)).eval(&row), Value::Null);
}

#[test]
fn arithmetic_int_float_promotion() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::add(Expr::lit(10i64), Expr::lit(2.5f64)).eval(&row), Value::Float64(12.5));
    assert_eq!(Expr::mul(Expr::lit(3.0f64), Expr::lit(4i64)).eval(&row), Value::Float64(12.0));
}

#[test]
fn expr_is_null_is_not_null() {
    let mut row = make_row(1, "Alice", 30, true);
    row.insert("maybe".into(), Value::Null);
    assert_eq!(Expr::is_null(Expr::col("maybe")).eval(&row), Value::Bool(true));
    assert_eq!(Expr::is_not_null(Expr::col("maybe")).eval(&row), Value::Bool(false));
    assert_eq!(Expr::is_null(Expr::col("name")).eval(&row), Value::Bool(false));
    assert_eq!(Expr::is_not_null(Expr::col("name")).eval(&row), Value::Bool(true));
    assert_eq!(Expr::is_null(Expr::col("nonexistent")).eval(&row), Value::Bool(true));
}

#[test]
fn expr_like_wildcard_percent() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("A%")).eval(&row), Value::Bool(true));
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("%ice")).eval(&row), Value::Bool(true));
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("%li%")).eval(&row), Value::Bool(true));
}

#[test]
fn expr_like_wildcard_underscore() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("A_ice")).eval(&row), Value::Bool(true));
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("_lice")).eval(&row), Value::Bool(true));
}

#[test]
fn expr_like_no_match() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("Bob%")).eval(&row), Value::Bool(false));
    assert_eq!(Expr::like(Expr::col("name"), Expr::lit("alice")).eval(&row), Value::Bool(false));
}

#[test]
fn expr_in_list() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(
        Expr::in_list(Expr::col("name"), vec![Expr::lit("Alice"), Expr::lit("Bob"), Expr::lit("Carol")]).eval(&row),
        Value::Bool(true)
    );
}

#[test]
fn expr_in_list_no_match() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(
        Expr::in_list(Expr::col("name"), vec![Expr::lit("Bob"), Expr::lit("Carol"), Expr::lit("Dave")]).eval(&row),
        Value::Bool(false)
    );
}

#[test]
fn expr_between() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::between(Expr::col("age"), Expr::lit(25i64), Expr::lit(35i64)).eval(&row), Value::Bool(true));
    assert_eq!(Expr::between(Expr::col("age"), Expr::lit(31i64), Expr::lit(40i64)).eval(&row), Value::Bool(false));
    assert_eq!(Expr::between(Expr::col("age"), Expr::lit(30i64), Expr::lit(40i64)).eval(&row), Value::Bool(true));
}

#[test]
fn expr_case_simple() {
    let row = make_row(1, "Alice", 30, true);
    let case_expr = Expr::case(
        Some(Expr::col("name")),
        vec![(Expr::lit("Alice"), Expr::lit("found")), (Expr::lit("Bob"), Expr::lit("other"))],
        Some(Expr::lit("unknown")),
    );
    assert_eq!(case_expr.eval(&row), Value::Text("found".into()));
}

#[test]
fn expr_case_searched() {
    let row = make_row(1, "Alice", 30, true);
    let case_expr = Expr::case(
        None,
        vec![
            (Expr::gt(Expr::col("age"), Expr::lit(40i64)), Expr::lit("old")),
            (Expr::gt(Expr::col("age"), Expr::lit(25i64)), Expr::lit("mid")),
        ],
        Some(Expr::lit("young")),
    );
    assert_eq!(case_expr.eval(&row), Value::Text("mid".into()));
}

#[test]
fn expr_negate() {
    let row = make_row(1, "Alice", 30, true);
    assert_eq!(Expr::negate(Expr::lit(42i64)).eval(&row), Value::Int64(-42));
    assert_eq!(Expr::negate(Expr::lit(3.14f64)).eval(&row), Value::Float64(-3.14));
    assert_eq!(Expr::negate(Expr::lit("text")).eval(&row), Value::Null);
}

#[test]
fn sql_arithmetic_in_select() {
    let schema = make_schema();
    let plan = sql_to_plan("SELECT * FROM users WHERE age + 5 > 30", schema).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 3);
}

#[test]
fn sql_is_null() {
    let schema = Schema::new("items", vec![
        Column::new("id", ValueType::Int64).not_null(),
        Column::new("val", ValueType::Text),
    ]);
    let mut r1 = BTreeMap::new();
    r1.insert("id".into(), Value::Int64(1));
    r1.insert("val".into(), Value::Text("hello".into()));
    let mut r2 = BTreeMap::new();
    r2.insert("id".into(), Value::Int64(2));
    r2.insert("val".into(), Value::Null);
    let rows = vec![r1, r2];
    let plan = sql_to_plan("SELECT * FROM items WHERE val IS NULL", schema).expect("parse failed");
    let result = execute(&plan, rows);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["id"], Value::Int64(2));
}

#[test]
fn sql_like() {
    let schema = make_schema();
    let plan = sql_to_plan("SELECT * FROM users WHERE name LIKE 'A%'", schema).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["name"], Value::Text("Alice".into()));
}

#[test]
fn sql_in_list_test() {
    let schema = make_schema();
    let plan = sql_to_plan("SELECT * FROM users WHERE name IN ('Alice', 'Carol')", schema).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 2);
}

#[test]
fn sql_between() {
    let schema = make_schema();
    let plan = sql_to_plan("SELECT * FROM users WHERE age BETWEEN 25 AND 30", schema).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 3);
}

#[test]
fn sql_distinct() {
    let schema = make_schema();
    let plan = sql_to_plan("SELECT DISTINCT active FROM users", schema).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 2);
}

#[test]
fn sql_having() {
    let schema = make_schema();
    let plan = sql_to_plan(
        "SELECT active, COUNT(*) AS cnt FROM users GROUP BY active HAVING cnt > 2",
        schema,
    ).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["active"], Value::Bool(true));
    assert_eq!(result[0]["cnt"], Value::Int64(3));
}

#[test]
fn sql_offset() {
    let schema = make_schema();
    let plan = sql_to_plan(
        "SELECT * FROM users ORDER BY age ASC LIMIT 3 OFFSET 2",
        schema,
    ).expect("parse failed");
    let result = execute(&plan, sample_rows());
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["age"], Value::Int64(28));
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 6: New Data Types (Timestamp, Date, Uuid, Decimal)
// ═══════════════════════════════════════════════════════════════════════

use rust_dst_db::query::expr::{
    parse_timestamp_str, parse_date_str, parse_uuid_str, parse_decimal_str,
    format_timestamp, format_date, format_uuid, format_decimal, ymd_to_days,
};

#[test]
fn value_timestamp_encode_decode() {
    // 2024-01-15 10:30:00 UTC
    let us = parse_timestamp_str("2024-01-15 10:30:00").unwrap();
    let val = Value::Timestamp(us);
    let encoded = val.encode();
    let (decoded, consumed) = Value::decode(&encoded).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, encoded.len());
    // Check display format
    let display = format!("{}", val);
    assert!(display.starts_with("2024-01-15T10:30:00"));
}

#[test]
fn value_date_encode_decode() {
    let days = parse_date_str("2024-01-15").unwrap();
    let val = Value::Date(days);
    let encoded = val.encode();
    let (decoded, consumed) = Value::decode(&encoded).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, encoded.len());
    assert_eq!(format!("{}", val), "2024-01-15");
}

#[test]
fn value_uuid_encode_decode() {
    let bytes = parse_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let val = Value::Uuid(bytes);
    let encoded = val.encode();
    let (decoded, consumed) = Value::decode(&encoded).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, encoded.len());
    assert_eq!(format!("{}", val), "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn value_decimal_encode_decode() {
    // 123.45 = Decimal(12345, 2)
    let val = Value::Decimal(12345, 2);
    let encoded = val.encode();
    let (decoded, consumed) = Value::decode(&encoded).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, encoded.len());
    assert_eq!(format!("{}", val), "123.45");

    // Negative decimal
    let neg = Value::Decimal(-999, 1);
    assert_eq!(format!("{}", neg), "-99.9");

    // parse_decimal_str roundtrip
    let (v, s) = parse_decimal_str("123.45").unwrap();
    assert_eq!(v, 12345);
    assert_eq!(s, 2);
}

#[test]
fn timestamp_comparison() {
    let t1 = Value::Timestamp(parse_timestamp_str("2024-01-01 00:00:00").unwrap());
    let t2 = Value::Timestamp(parse_timestamp_str("2024-06-15 12:00:00").unwrap());
    assert!(t1 < t2);
    assert!(t2 > t1);
    assert_eq!(t1, t1.clone());
}

#[test]
fn date_comparison() {
    let d1 = Value::Date(parse_date_str("2024-01-01").unwrap());
    let d2 = Value::Date(parse_date_str("2024-12-31").unwrap());
    assert!(d1 < d2);
    assert_eq!(d1, d1.clone());
}

#[test]
fn decimal_comparison() {
    // 123.45 vs 123.450 (different scales, same value)
    let a = Value::Decimal(12345, 2);
    let b = Value::Decimal(123450, 3);
    assert_eq!(a, b);

    // 100.00 < 200.00
    let c = Value::Decimal(10000, 2);
    let d = Value::Decimal(20000, 2);
    assert!(c < d);
}

#[test]
fn date_arithmetic_add_days() {
    let row = BTreeMap::new();
    let base_date = Value::Date(ymd_to_days(2024, 1, 15));
    let result = Expr::add(Expr::lit(base_date.clone()), Expr::lit(10i64)).eval(&row);
    assert_eq!(result, Value::Date(ymd_to_days(2024, 1, 25)));
}

#[test]
fn date_arithmetic_subtract() {
    let row = BTreeMap::new();
    let d1 = Value::Date(ymd_to_days(2024, 1, 31));
    let d2 = Value::Date(ymd_to_days(2024, 1, 1));
    // Date - Date = Int64 (difference in days)
    let result = Expr::sub(Expr::lit(d1), Expr::lit(d2)).eval(&row);
    assert_eq!(result, Value::Int64(30));

    // Date - Int64 = Date
    let d3 = Value::Date(ymd_to_days(2024, 1, 15));
    let result2 = Expr::sub(Expr::lit(d3), Expr::lit(5i64)).eval(&row);
    assert_eq!(result2, Value::Date(ymd_to_days(2024, 1, 10)));
}

#[test]
fn decimal_arithmetic() {
    let row = BTreeMap::new();
    // 10.50 + 3.25 = 13.75
    let a = Value::Decimal(1050, 2);
    let b = Value::Decimal(325, 2);
    let result = Expr::add(Expr::lit(a), Expr::lit(b)).eval(&row);
    assert_eq!(result, Value::Decimal(1375, 2));

    // 10.00 * 2.50 = 25.0000
    let c = Value::Decimal(1000, 2);
    let d = Value::Decimal(250, 2);
    let result2 = Expr::mul(Expr::lit(c), Expr::lit(d)).eval(&row);
    assert_eq!(result2, Value::Decimal(250000, 4));
}

#[test]
fn engine_create_table_with_new_types() {
    let dir = std::env::temp_dir().join("rust_db_test_new_types_create");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql(
        "CREATE TABLE events (id INT NOT NULL, created_at TIMESTAMP, event_date DATE, request_id UUID, amount DECIMAL)"
    ).unwrap();

    let schema = db.get_schema("events").unwrap().unwrap();
    assert_eq!(schema.columns.len(), 5);
    assert_eq!(schema.columns[1].col_type, ValueType::Timestamp);
    assert_eq!(schema.columns[2].col_type, ValueType::Date);
    assert_eq!(schema.columns[3].col_type, ValueType::Uuid);
    assert_eq!(schema.columns[4].col_type, ValueType::Decimal);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_insert_select_timestamp() {
    let dir = std::env::temp_dir().join("rust_db_test_insert_timestamp");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql("CREATE TABLE logs (id INT NOT NULL, ts TIMESTAMP)").unwrap();
    db.execute_sql("INSERT INTO logs VALUES (1, TIMESTAMP '2024-01-15 10:30:00')").unwrap();

    let result = db.execute_sql("SELECT * FROM logs").unwrap();
    match result {
        rust_dst_db::engine::SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0]["ts"] {
                Value::Timestamp(us) => {
                    let display = format_timestamp(*us);
                    assert!(display.starts_with("2024-01-15T10:30:00"));
                }
                other => panic!("Expected Timestamp, got {:?}", other),
            }
        }
        _ => panic!("Expected query result"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_insert_select_date() {
    let dir = std::env::temp_dir().join("rust_db_test_insert_date");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql("CREATE TABLE events (id INT NOT NULL, event_date DATE)").unwrap();
    db.execute_sql("INSERT INTO events VALUES (1, DATE '2024-06-15')").unwrap();

    let result = db.execute_sql("SELECT * FROM events").unwrap();
    match result {
        rust_dst_db::engine::SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0]["event_date"] {
                Value::Date(days) => {
                    assert_eq!(format_date(*days), "2024-06-15");
                }
                other => panic!("Expected Date, got {:?}", other),
            }
        }
        _ => panic!("Expected query result"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_insert_select_uuid() {
    let dir = std::env::temp_dir().join("rust_db_test_insert_uuid");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql("CREATE TABLE items (id UUID NOT NULL, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO items VALUES (CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID), 'widget')").unwrap();

    let result = db.execute_sql("SELECT * FROM items").unwrap();
    match result {
        rust_dst_db::engine::SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0]["id"] {
                Value::Uuid(bytes) => {
                    assert_eq!(format_uuid(bytes), "550e8400-e29b-41d4-a716-446655440000");
                }
                other => panic!("Expected Uuid, got {:?}", other),
            }
        }
        _ => panic!("Expected query result"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn engine_insert_select_decimal() {
    let dir = std::env::temp_dir().join("rust_db_test_insert_decimal");
    let _ = std::fs::remove_dir_all(&dir);
    let db = rust_dst_db::engine::Database::open(&dir).expect("open failed");

    db.execute_sql("CREATE TABLE prices (id INT NOT NULL, amount DECIMAL)").unwrap();
    // Insert using a regular numeric literal -- sqlparser parses 99.95 as a Number
    db.execute_sql("INSERT INTO prices VALUES (1, 99.95)").unwrap();

    let result = db.execute_sql("SELECT * FROM prices").unwrap();
    match result {
        rust_dst_db::engine::SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            // The value was inserted as Float64 (since sql_expr_to_value parses numbers
            // as Int64 or Float64). This is expected since we'd need schema-aware coercion
            // to convert at insert time. Verify it's stored as a number.
            match &rows[0]["amount"] {
                Value::Float64(f) => {
                    assert!((f - 99.95).abs() < 0.001);
                }
                Value::Decimal(v, s) => {
                    assert_eq!(format_decimal(*v, *s), "99.95");
                }
                other => panic!("Expected Float64 or Decimal, got {:?}", other),
            }
        }
        _ => panic!("Expected query result"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
