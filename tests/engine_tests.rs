//! Integration tests for the production Database engine.
//!
//! Tests real file I/O, WAL durability, SQL DDL/DML, crash recovery,
//! and concurrent transaction behavior.

use rust_dst_db::engine::{Database, DbError, SqlResult};
use rust_dst_db::query::expr::Value;
use std::collections::BTreeMap;

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust_db_test_{}", name));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

// ─── DDL Tests ───────────────────────────────────────────────────────────────

#[test]
fn create_table_and_list() {
    let dir = tmp_dir("create_table");
    let db = Database::open(&dir).unwrap();

    match db
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT, active BOOLEAN)")
        .unwrap()
    {
        SqlResult::Execute(_) => {}
        _ => panic!("expected Execute result"),
    }

    assert_eq!(db.table_count().unwrap(), 1);
}

#[test]
fn create_duplicate_table_fails() {
    let dir = tmp_dir("dup_table");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT)").unwrap();
    let result = db.execute_sql("CREATE TABLE users (id BIGINT)");

    assert!(result.is_err());
}

#[test]
fn drop_table() {
    let dir = tmp_dir("drop_table");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE temp (id BIGINT)").unwrap();
    assert_eq!(db.table_count().unwrap(), 1);

    db.execute_sql("DROP TABLE temp").unwrap();
    assert_eq!(db.table_count().unwrap(), 0);
}

// ─── INSERT Tests ────────────────────────────────────────────────────────────

#[test]
fn insert_and_select() {
    let dir = tmp_dir("insert_select");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();

    match db.execute_sql("SELECT * FROM users").unwrap() {
        SqlResult::Query { columns, rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert!(columns.contains(&"id".to_string()));
            assert!(columns.contains(&"name".to_string()));
        }
        _ => panic!("expected Query result"),
    }
}

#[test]
fn insert_multiple_rows() {
    let dir = tmp_dir("insert_multi");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();

    match db.execute_sql("SELECT * FROM items").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("expected Query result"),
    }
}

#[test]
fn insert_not_null_violation() {
    let dir = tmp_dir("not_null");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    let result = db.execute_sql("INSERT INTO users (id, name) VALUES (NULL, 'Alice')");

    assert!(result.is_err());
}

// ─── SELECT with WHERE ──────────────────────────────────────────────────────

#[test]
fn select_with_filter() {
    let dir = tmp_dir("select_filter");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
        .unwrap();

    match db.execute_sql("SELECT * FROM users WHERE id > 1").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected Query result"),
    }
}

#[test]
fn select_with_order_and_limit() {
    let dir = tmp_dir("order_limit");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE nums (id BIGINT, val BIGINT)")
        .unwrap();
    for i in 1..=5 {
        db.execute_sql(&format!(
            "INSERT INTO nums (id, val) VALUES ({}, {})",
            i,
            i * 10
        ))
        .unwrap();
    }

    match db
        .execute_sql("SELECT * FROM nums ORDER BY val DESC LIMIT 3")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // First row should be val=50 (highest).
            assert_eq!(rows[0].get("val"), Some(&Value::Int64(50)));
        }
        _ => panic!("expected Query result"),
    }
}

#[test]
fn select_aggregate() {
    let dir = tmp_dir("aggregate");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE scores (id BIGINT, player TEXT, score BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (1, 'A', 100)")
        .unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (2, 'B', 200)")
        .unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (3, 'A', 300)")
        .unwrap();

    match db
        .execute_sql("SELECT COUNT(*), SUM(score), AVG(score) FROM scores")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count"), Some(&Value::Int64(3)));
            assert_eq!(rows[0].get("sum"), Some(&Value::Int64(600)));
        }
        _ => panic!("expected Query result"),
    }
}

// ─── UPDATE Tests ────────────────────────────────────────────────────────────

#[test]
fn update_rows() {
    let dir = tmp_dir("update");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();

    match db
        .execute_sql("UPDATE users SET name = 'Alicia' WHERE id = 1")
        .unwrap()
    {
        SqlResult::Execute(count) => assert_eq!(count, 1),
        _ => panic!("expected Execute result"),
    }

    match db.execute_sql("SELECT * FROM users WHERE id = 1").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("name"),
                Some(&Value::Text("Alicia".to_string()))
            );
        }
        _ => panic!("expected Query result"),
    }
}

// ─── DELETE Tests ────────────────────────────────────────────────────────────

#[test]
fn delete_rows() {
    let dir = tmp_dir("delete");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a')")
        .unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (2, 'b')")
        .unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (3, 'c')")
        .unwrap();

    match db.execute_sql("DELETE FROM items WHERE id = 2").unwrap() {
        SqlResult::Execute(count) => assert_eq!(count, 1),
        _ => panic!("expected Execute result"),
    }

    match db.execute_sql("SELECT * FROM items").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected Query result"),
    }
}

// ─── WAL Crash Recovery ─────────────────────────────────────────────────────

#[test]
fn wal_recovery_persists_data() {
    let dir = tmp_dir("wal_recovery");

    // Phase 1: insert data and close.
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();
    }
    // Database dropped — file handles closed.

    // Phase 2: reopen and verify data survived.
    {
        let db = Database::open(&dir).unwrap();
        assert_eq!(db.table_count().unwrap(), 1);

        match db.execute_sql("SELECT * FROM users").unwrap() {
            SqlResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected Query result"),
        }
    }
}

#[test]
fn wal_recovery_after_updates_and_deletes() {
    let dir = tmp_dir("wal_recovery_dml");

    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a')")
            .unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (2, 'b')")
            .unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (3, 'c')")
            .unwrap();
        db.execute_sql("UPDATE items SET name = 'x' WHERE id = 2")
            .unwrap();
        db.execute_sql("DELETE FROM items WHERE id = 3").unwrap();
    }

    {
        let db = Database::open(&dir).unwrap();
        match db.execute_sql("SELECT * FROM items ORDER BY id").unwrap() {
            SqlResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("name"), Some(&Value::Text("a".to_string())));
                assert_eq!(rows[1].get("name"), Some(&Value::Text("x".to_string())));
            }
            _ => panic!("expected Query result"),
        }
    }
}

// ─── Transaction API Tests ──────────────────────────────────────────────────

#[test]
fn explicit_transaction_api() {
    let dir = tmp_dir("txn_api");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE kv (id BIGINT, val TEXT)")
        .unwrap();

    let schema = db.get_schema("kv").unwrap().unwrap();

    let txn_id = db.begin().unwrap();

    let key = schema.make_key(&Value::Int64(1));
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), Value::Int64(1));
    row.insert("val".to_string(), Value::Text("hello".to_string()));
    let value = schema.encode_row(&row);

    db.put(txn_id, key.clone(), value).unwrap();
    db.commit(txn_id).unwrap();

    // Verify via SQL.
    match db.execute_sql("SELECT * FROM kv").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("val"), Some(&Value::Text("hello".to_string())));
        }
        _ => panic!("expected Query result"),
    }
}

// ─── Transaction Isolation Tests ─────────────────────────────────────────────

#[test]
fn concurrent_transactions_conflict() {
    let dir = tmp_dir("txn_conflict");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE counter (id BIGINT, val BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO counter (id, val) VALUES (1, 0)")
        .unwrap();

    // Start two transactions.
    let txn1 = db.begin().unwrap();
    let txn2 = db.begin().unwrap();

    // Both write to the same key.
    let schema = db.get_schema("counter").unwrap().unwrap();

    let key = schema.make_key(&Value::Int64(1));
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), Value::Int64(1));
    row.insert("val".to_string(), Value::Int64(10));
    let value = schema.encode_row(&row);

    db.put(txn1, key.clone(), value.clone()).unwrap();

    let mut row2 = BTreeMap::new();
    row2.insert("id".to_string(), Value::Int64(1));
    row2.insert("val".to_string(), Value::Int64(20));
    let value2 = schema.encode_row(&row2);

    db.put(txn2, key.clone(), value2).unwrap();

    // First commit wins.
    db.commit(txn1).unwrap();

    // Second commit should fail with conflict.
    let result = db.commit(txn2);
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::Conflict(_) => {}
        other => panic!("expected Conflict, got: {:?}", other),
    }
}

// ─── Multiple Tables ─────────────────────────────────────────────────────────

#[test]
fn multiple_tables_independent() {
    let dir = tmp_dir("multi_table");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DOUBLE PRECISION)")
        .unwrap();

    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 99.99)")
        .unwrap();

    match db.execute_sql("SELECT * FROM users").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("expected Query"),
    }

    match db.execute_sql("SELECT * FROM orders").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("expected Query"),
    }
}

// ─── Data Types ──────────────────────────────────────────────────────────────

#[test]
fn various_data_types() {
    let dir = tmp_dir("data_types");
    let db = Database::open(&dir).unwrap();

    db.execute_sql(
        "CREATE TABLE mixed (id BIGINT, flag BOOLEAN, score DOUBLE PRECISION, name TEXT)",
    )
    .unwrap();

    db.execute_sql("INSERT INTO mixed (id, flag, score, name) VALUES (1, true, 3.14, 'test')")
        .unwrap();

    match db.execute_sql("SELECT * FROM mixed").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("flag"), Some(&Value::Bool(true)));
            assert_eq!(rows[0].get("score"), Some(&Value::Float64(3.14)));
            assert_eq!(rows[0].get("name"), Some(&Value::Text("test".to_string())));
        }
        _ => panic!("expected Query"),
    }
}

// ─── Phase A regression tests ────────────────────────────────────────────────

#[test]
fn a1_leading_line_comments_before_ddl() {
    let dir = tmp_dir("a1_comments");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("-- migration 001\nCREATE TABLE t (id BIGINT)")
        .unwrap();
    assert_eq!(db.table_count().unwrap(), 1);

    db.execute_sql("-- top comment\n-- another comment\nINSERT INTO t (id) VALUES (1)")
        .unwrap();

    match db.execute_sql("-- pre-select\nSELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("expected Query"),
    }
}

#[test]
fn a1_inline_comments_preserve_string_literals() {
    let dir = tmp_dir("a1_strings");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE notes (id BIGINT, body TEXT)")
        .unwrap();
    // The `--` inside the string literal must NOT be treated as a comment.
    db.execute_sql("INSERT INTO notes (id, body) VALUES (1, 'has -- two dashes')")
        .unwrap();

    match db.execute_sql("SELECT * FROM notes").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("body"),
                Some(&Value::Text("has -- two dashes".to_string()))
            );
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn a2_like_with_escape_clause() {
    let dir = tmp_dir("a2_like_escape");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE q (id BIGINT, label TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO q (id, label) VALUES (1, '50% off')")
        .unwrap();
    db.execute_sql("INSERT INTO q (id, label) VALUES (2, 'half off')")
        .unwrap();
    db.execute_sql("INSERT INTO q (id, label) VALUES (3, 'a_b')")
        .unwrap();

    // With ESCAPE '\\', '\%' is a literal '%' so only row 1 ('50% off') matches.
    match db
        .execute_sql(r"SELECT * FROM q WHERE label LIKE '%\%%' ESCAPE '\'")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1, "expected only the literal-% row");
            assert_eq!(
                rows[0].get("label"),
                Some(&Value::Text("50% off".to_string()))
            );
        }
        _ => panic!("expected Query"),
    }

    // Underscore literal via escape: 'a\_b' matches only 'a_b', not 'a' + any + 'b'.
    match db
        .execute_sql(r"SELECT * FROM q WHERE label LIKE 'a\_b' ESCAPE '\'")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("label"), Some(&Value::Text("a_b".to_string())));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn a3_comma_join_is_cross_join() {
    let dir = tmp_dir("a3_comma_join");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE a (id BIGINT)").unwrap();
    db.execute_sql("CREATE TABLE b (id BIGINT)").unwrap();
    db.execute_sql("INSERT INTO a (id) VALUES (1)").unwrap();
    db.execute_sql("INSERT INTO a (id) VALUES (2)").unwrap();
    db.execute_sql("INSERT INTO b (id) VALUES (10)").unwrap();
    db.execute_sql("INSERT INTO b (id) VALUES (20)").unwrap();
    db.execute_sql("INSERT INTO b (id) VALUES (30)").unwrap();

    match db.execute_sql("SELECT * FROM a, b").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 6),
        _ => panic!("expected Query"),
    }
}

#[test]
fn a3_comma_join_with_where_filter() {
    let dir = tmp_dir("a3_comma_join_where");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE u (uid BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("CREATE TABLE p (pid BIGINT, owner BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO u (uid, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO u (uid, name) VALUES (2, 'Bob')")
        .unwrap();
    db.execute_sql("INSERT INTO p (pid, owner) VALUES (10, 1)")
        .unwrap();
    db.execute_sql("INSERT INTO p (pid, owner) VALUES (11, 2)")
        .unwrap();

    match db
        .execute_sql("SELECT * FROM u, p WHERE u.uid = p.owner")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected Query"),
    }
}

#[test]
fn a4_in_list_in_multitable_where() {
    let dir = tmp_dir("a4_in_multi");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE u (uid BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("CREATE TABLE p (pid BIGINT, owner BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO u (uid, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO u (uid, name) VALUES (2, 'Bob')")
        .unwrap();
    db.execute_sql("INSERT INTO u (uid, name) VALUES (3, 'Carol')")
        .unwrap();
    db.execute_sql("INSERT INTO p (pid, owner) VALUES (10, 1)")
        .unwrap();
    db.execute_sql("INSERT INTO p (pid, owner) VALUES (11, 2)")
        .unwrap();
    db.execute_sql("INSERT INTO p (pid, owner) VALUES (12, 3)")
        .unwrap();

    match db
        .execute_sql("SELECT * FROM u JOIN p ON u.uid = p.owner WHERE u.uid IN (1, 3)")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected Query"),
    }
}

#[test]
fn a5_create_index_if_not_exists_is_idempotent() {
    let dir = tmp_dir("a5_idx_ine");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("CREATE INDEX ix_name ON t (name)").unwrap();

    // Re-issuing without IF NOT EXISTS must error.
    assert!(db.execute_sql("CREATE INDEX ix_name ON t (name)").is_err());

    // With IF NOT EXISTS, must be a no-op success.
    db.execute_sql("CREATE INDEX IF NOT EXISTS ix_name ON t (name)")
        .unwrap();
}

// ─── Phase B: Vector primitives ──────────────────────────────────────────────

#[test]
fn b_vector_create_insert_select_roundtrip() {
    let dir = tmp_dir("b_vec_roundtrip");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE docs (id BIGINT, embedding VECTOR(3))")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, embedding) VALUES (1, '[1.0, 2.0, 3.0]')")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, embedding) VALUES (2, '[4.0, 5.0, 6.0]')")
        .unwrap();

    match db.execute_sql("SELECT * FROM docs").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Rows come back ordered by primary key.
            assert_eq!(
                rows[0].get("embedding"),
                Some(&Value::Vector(vec![1.0, 2.0, 3.0]))
            );
            assert_eq!(
                rows[1].get("embedding"),
                Some(&Value::Vector(vec![4.0, 5.0, 6.0]))
            );
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn b_vector_dim_mismatch_rejected() {
    let dir = tmp_dir("b_vec_dim");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE docs (id BIGINT, embedding VECTOR(3))")
        .unwrap();
    match db.execute_sql("INSERT INTO docs (id, embedding) VALUES (1, '[1.0, 2.0]')") {
        Err(DbError::Constraint(_)) => {}
        Ok(_) => panic!("expected Constraint error, got Ok"),
        Err(other) => panic!("expected Constraint, got {:?}", other),
    }
}

#[test]
fn b_vector_persists_across_reopen() {
    let dir = tmp_dir("b_vec_persist");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE docs (id BIGINT, embedding VECTOR(2))")
            .unwrap();
        db.execute_sql("INSERT INTO docs (id, embedding) VALUES (7, '[0.1, 0.2]')")
            .unwrap();
    }
    let db2 = Database::open(&dir).unwrap();
    let schema = db2.get_schema("docs").unwrap().expect("schema must reload");
    use rust_dst_db::query::expr::ValueType;
    assert_eq!(schema.columns[1].col_type, ValueType::Vector { dim: 2 });

    match db2.execute_sql("SELECT * FROM docs").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("embedding"),
                Some(&Value::Vector(vec![0.1, 0.2]))
            );
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn b_cosine_distance_function() {
    let dir = tmp_dir("b_cosine");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE docs (id BIGINT, e VECTOR(3))")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (1, '[1.0, 0.0, 0.0]')")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (2, '[0.0, 1.0, 0.0]')")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (3, '[1.0, 0.0, 0.0]')")
        .unwrap();

    match db
        .execute_sql("SELECT id, COSINE_DISTANCE(e, '[1.0, 0.0, 0.0]') AS d FROM docs")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Identical vectors: distance ~ 0.
            let r1 = rows
                .iter()
                .find(|r| r.get("id") == Some(&Value::Int64(1)))
                .unwrap();
            if let Some(Value::Float64(d)) = r1.get("d") {
                assert!(d.abs() < 1e-6, "expected ~0, got {}", d);
            } else {
                panic!("expected Float64 distance, got {:?}", r1.get("d"));
            }
            // Orthogonal: distance ~ 1.
            let r2 = rows
                .iter()
                .find(|r| r.get("id") == Some(&Value::Int64(2)))
                .unwrap();
            if let Some(Value::Float64(d)) = r2.get("d") {
                assert!((d - 1.0).abs() < 1e-6, "expected ~1, got {}", d);
            } else {
                panic!("expected Float64 distance");
            }
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn b_l2_and_dot_product_functions() {
    let dir = tmp_dir("b_l2_dot");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE v (id BIGINT, e VECTOR(2))")
        .unwrap();
    db.execute_sql("INSERT INTO v (id, e) VALUES (1, '[3.0, 4.0]')")
        .unwrap();

    match db
        .execute_sql("SELECT L2_DISTANCE(e, '[0.0, 0.0]') AS d FROM v")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            // sqrt(9 + 16) = 5
            if let Some(Value::Float64(d)) = rows[0].get("d") {
                assert!((d - 5.0).abs() < 1e-6, "expected 5.0, got {}", d);
            } else {
                panic!("expected Float64 distance, got {:?}", rows[0].get("d"));
            }
        }
        _ => panic!("expected Query"),
    }

    match db
        .execute_sql("SELECT DOT_PRODUCT(e, '[1.0, 1.0]') AS d FROM v")
        .unwrap()
    {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            if let Some(Value::Float64(d)) = rows[0].get("d") {
                assert!((d - 7.0).abs() < 1e-6, "expected 7.0, got {}", d);
            } else {
                panic!("expected Float64 dot product");
            }
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn b_brute_force_knn_via_order_by_limit() {
    let dir = tmp_dir("b_knn");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE docs (id BIGINT, e VECTOR(2))")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (1, '[1.0, 0.0]')")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (2, '[0.9, 0.1]')")
        .unwrap(); // closest to query
    db.execute_sql("INSERT INTO docs (id, e) VALUES (3, '[0.0, 1.0]')")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (4, '[-1.0, 0.0]')")
        .unwrap();

    let sql = "SELECT id, L2_DISTANCE(e, '[1.0, 0.0]') AS d \
               FROM docs ORDER BY d LIMIT 2";
    match db.execute_sql(sql).unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Nearest two must be id=1 (distance 0) then id=2 (~0.1414).
            assert_eq!(rows[0].get("id"), Some(&Value::Int64(1)));
            assert_eq!(rows[1].get("id"), Some(&Value::Int64(2)));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn b_vector_update_and_dim_check() {
    let dir = tmp_dir("b_vec_update");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE docs (id BIGINT, e VECTOR(3))")
        .unwrap();
    db.execute_sql("INSERT INTO docs (id, e) VALUES (1, '[1.0, 2.0, 3.0]')")
        .unwrap();
    db.execute_sql("UPDATE docs SET e = '[9.0, 8.0, 7.0]' WHERE id = 1")
        .unwrap();

    match db.execute_sql("SELECT * FROM docs").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows[0].get("e"), Some(&Value::Vector(vec![9.0, 8.0, 7.0])));
        }
        _ => panic!("expected Query"),
    }

    // Wrong dim on UPDATE must fail.
    match db.execute_sql("UPDATE docs SET e = '[1.0, 2.0]' WHERE id = 1") {
        Err(DbError::Constraint(_)) => {}
        Ok(_) => panic!("expected Constraint error, got Ok"),
        Err(other) => panic!("expected Constraint, got {:?}", other),
    }
}

// ─── Phase C: Snapshot / checkpoint ──────────────────────────────────────────

#[test]
fn c_checkpoint_truncates_wal_and_preserves_data() {
    let dir = tmp_dir("c_checkpoint_basic");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();

    let wal_before = std::fs::metadata(dir.join("wal.log")).unwrap().len();
    assert!(wal_before > 0, "WAL should have content before checkpoint");

    db.checkpoint().unwrap();

    let wal_after = std::fs::metadata(dir.join("wal.log")).unwrap().len();
    assert_eq!(wal_after, 0, "WAL should be empty after checkpoint");
    assert!(
        dir.join("snapshot.current").exists(),
        "snapshot.current should exist"
    );
    assert!(
        !dir.join("snapshot.new").exists(),
        "snapshot.new should be cleaned up"
    );

    // Data still reachable in-process.
    match db.execute_sql("SELECT * FROM users").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected Query"),
    }
}

#[test]
fn c_reopen_from_snapshot_only() {
    let dir = tmp_dir("c_reopen_snapshot_only");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
            .unwrap();
        for i in 1..=50 {
            db.execute_sql(&format!(
                "INSERT INTO t (id, name) VALUES ({}, 'row-{}')",
                i, i
            ))
            .unwrap();
        }
        db.checkpoint().unwrap();
        assert_eq!(std::fs::metadata(dir.join("wal.log")).unwrap().len(), 0);
    }
    let db2 = Database::open(&dir).unwrap();
    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 50),
        _ => panic!("expected Query"),
    }
    // Schema also survives.
    let schema = db2
        .get_schema("t")
        .unwrap()
        .expect("schema must reload from snapshot");
    assert_eq!(schema.columns.len(), 2);
}

#[test]
fn c_snapshot_plus_wal_replay_on_reopen() {
    let dir = tmp_dir("c_snapshot_plus_wal");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO t (id, name) VALUES (1, 'pre')")
            .unwrap();
        db.execute_sql("INSERT INTO t (id, name) VALUES (2, 'pre')")
            .unwrap();
        db.checkpoint().unwrap();
        // Post-snapshot writes — must replay from WAL on reopen.
        db.execute_sql("INSERT INTO t (id, name) VALUES (3, 'post')")
            .unwrap();
        db.execute_sql("UPDATE t SET name = 'pre-updated' WHERE id = 1")
            .unwrap();
        db.execute_sql("DELETE FROM t WHERE id = 2").unwrap();
    }
    let db2 = Database::open(&dir).unwrap();
    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            // id=1 updated, id=2 deleted, id=3 added.
            assert_eq!(rows.len(), 2);
            let row1 = rows
                .iter()
                .find(|r| r.get("id") == Some(&Value::Int64(1)))
                .unwrap();
            assert_eq!(row1.get("name"), Some(&Value::Text("pre-updated".into())));
            assert!(rows.iter().any(|r| r.get("id") == Some(&Value::Int64(3))));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn c_snapshot_preserves_tombstones() {
    // If a key was deleted before snapshot, the snapshot must carry the
    // tombstone so reopen doesn't resurrect the row from earlier WAL replay.
    // (Post-checkpoint the WAL is empty, but defensive check that the snapshot
    // itself encodes the delete.)
    let dir = tmp_dir("c_tombstones");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO t (id, name) VALUES (1, 'keep')")
            .unwrap();
        db.execute_sql("INSERT INTO t (id, name) VALUES (2, 'remove')")
            .unwrap();
        db.execute_sql("DELETE FROM t WHERE id = 2").unwrap();
        db.checkpoint().unwrap();
    }
    let db2 = Database::open(&dir).unwrap();
    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id"), Some(&Value::Int64(1)));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn c_partial_snapshot_new_is_ignored_on_open() {
    let dir = tmp_dir("c_partial_snapshot");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT)").unwrap();
        db.execute_sql("INSERT INTO t (id) VALUES (42)").unwrap();
    }
    // Simulate a crashed in-progress snapshot: garbage snapshot.new file.
    std::fs::write(dir.join("snapshot.new"), b"garbage partial write").unwrap();

    let db2 = Database::open(&dir).unwrap();
    // The stale snapshot.new was ignored AND cleaned up.
    assert!(
        !dir.join("snapshot.new").exists(),
        "stale snapshot.new must be removed on open"
    );
    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id"), Some(&Value::Int64(42)));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn c_auto_checkpoint_fires_at_threshold() {
    let dir = tmp_dir("c_auto_checkpoint");
    // Very small threshold so a few inserts trigger it.
    let db = Database::open_with_checkpoint_bytes(&dir, 1024).unwrap();
    db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    for i in 1..=30 {
        db.execute_sql(&format!(
            "INSERT INTO t (id, name) VALUES ({}, 'some payload long enough to cross the threshold')",
            i
        )).unwrap();
    }
    // At some point the threshold fired and the WAL got truncated. Final state:
    // WAL is smaller than total bytes written, or zero. Snapshot exists.
    assert!(
        dir.join("snapshot.current").exists(),
        "auto-checkpoint must have produced a snapshot"
    );

    // Data integrity preserved across auto-checkpoint.
    match db.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 30),
        _ => panic!("expected Query"),
    }
}

#[test]
fn c_multiple_checkpoints_stack_correctly() {
    let dir = tmp_dir("c_multi_checkpoint");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE t (id BIGINT, v BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO t (id, v) VALUES (1, 100)")
        .unwrap();
    db.checkpoint().unwrap();

    db.execute_sql("UPDATE t SET v = 200 WHERE id = 1").unwrap();
    db.checkpoint().unwrap();

    db.execute_sql("UPDATE t SET v = 300 WHERE id = 1").unwrap();
    db.checkpoint().unwrap();

    drop(db);
    let db2 = Database::open(&dir).unwrap();
    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("v"), Some(&Value::Int64(300)));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn d_checkpoint_count_increments() {
    let dir = tmp_dir("d_ckpt_count");
    let db = Database::open(&dir).unwrap();
    assert_eq!(db.checkpoint_count().unwrap(), 0);
    db.execute_sql("CREATE TABLE t (id BIGINT)").unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.checkpoint_count().unwrap(), 1);
    db.checkpoint().unwrap();
    assert_eq!(db.checkpoint_count().unwrap(), 2);
}

#[test]
fn d_auto_checkpoint_bumps_engine_counter() {
    let dir = tmp_dir("d_auto_ckpt_count");
    // Aggressive threshold so one INSERT crosses it.
    let db = Database::open_with_checkpoint_bytes(&dir, 256).unwrap();
    db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    for i in 1..=20 {
        db.execute_sql(&format!(
            "INSERT INTO t (id, name) VALUES ({}, 'padding padding padding padding')",
            i
        ))
        .unwrap();
    }
    // Some number of auto-checkpoints must have fired.
    assert!(
        db.checkpoint_count().unwrap() >= 1,
        "auto-checkpoint must bump the counter"
    );
}

#[test]
fn c_next_ts_survives_snapshot_roundtrip() {
    let dir = tmp_dir("c_next_ts");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT)").unwrap();
        for i in 1..=5 {
            db.execute_sql(&format!("INSERT INTO t (id) VALUES ({})", i))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    // After reopen, new commits must not conflict with old timestamps.
    let db2 = Database::open(&dir).unwrap();
    db2.execute_sql("INSERT INTO t (id) VALUES (99)").unwrap();
    db2.execute_sql("DELETE FROM t WHERE id = 1").unwrap();

    match db2.execute_sql("SELECT * FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5); // 5 - 1 deleted + 1 new = 5
            assert!(!rows.iter().any(|r| r.get("id") == Some(&Value::Int64(1))));
            assert!(rows.iter().any(|r| r.get("id") == Some(&Value::Int64(99))));
        }
        _ => panic!("expected Query"),
    }
}
