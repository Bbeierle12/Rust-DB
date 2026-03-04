//! Integration tests for the production Database engine.
//!
//! Tests real file I/O, WAL durability, SQL DDL/DML, crash recovery,
//! and concurrent transaction behavior.

use std::collections::BTreeMap;
use rust_dst_db::engine::{Database, DbError, SqlResult};
use rust_dst_db::query::expr::Value;

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

    match db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT, active BOOLEAN)").unwrap() {
        SqlResult::Execute(_) => {}
        _ => panic!("expected Execute result"),
    }

    assert_eq!(db.table_count(), 1);
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
    assert_eq!(db.table_count(), 1);

    db.execute_sql("DROP TABLE temp").unwrap();
    assert_eq!(db.table_count(), 0);
}

// ─── INSERT Tests ────────────────────────────────────────────────────────────

#[test]
fn insert_and_select() {
    let dir = tmp_dir("insert_select");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    match db.execute_sql("SELECT * FROM users").unwrap() {
        SqlResult::Query { columns, rows } => {
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

    db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();

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

    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    let result = db.execute_sql("INSERT INTO users (id, name) VALUES (NULL, 'Alice')");

    assert!(result.is_err());
}

// ─── SELECT with WHERE ──────────────────────────────────────────────────────

#[test]
fn select_with_filter() {
    let dir = tmp_dir("select_filter");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (3, 'Charlie')").unwrap();

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

    db.execute_sql("CREATE TABLE nums (id BIGINT, val BIGINT)").unwrap();
    for i in 1..=5 {
        db.execute_sql(&format!("INSERT INTO nums (id, val) VALUES ({}, {})", i, i * 10)).unwrap();
    }

    match db.execute_sql("SELECT * FROM nums ORDER BY val DESC LIMIT 3").unwrap() {
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

    db.execute_sql("CREATE TABLE scores (id BIGINT, player TEXT, score BIGINT)").unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (1, 'A', 100)").unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (2, 'B', 200)").unwrap();
    db.execute_sql("INSERT INTO scores (id, player, score) VALUES (3, 'A', 300)").unwrap();

    match db.execute_sql("SELECT COUNT(*), SUM(score), AVG(score) FROM scores").unwrap() {
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

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    match db.execute_sql("UPDATE users SET name = 'Alicia' WHERE id = 1").unwrap() {
        SqlResult::Execute(count) => assert_eq!(count, 1),
        _ => panic!("expected Execute result"),
    }

    match db.execute_sql("SELECT * FROM users WHERE id = 1").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Text("Alicia".to_string())));
        }
        _ => panic!("expected Query result"),
    }
}

// ─── DELETE Tests ────────────────────────────────────────────────────────────

#[test]
fn delete_rows() {
    let dir = tmp_dir("delete");
    let db = Database::open(&dir).unwrap();

    db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a')").unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (2, 'b')").unwrap();
    db.execute_sql("INSERT INTO items (id, name) VALUES (3, 'c')").unwrap();

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
        db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)").unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();
    }
    // Database dropped — file handles closed.

    // Phase 2: reopen and verify data survived.
    {
        let db = Database::open(&dir).unwrap();
        assert_eq!(db.table_count(), 1);

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
        db.execute_sql("CREATE TABLE items (id BIGINT, name TEXT)").unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'a')").unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (2, 'b')").unwrap();
        db.execute_sql("INSERT INTO items (id, name) VALUES (3, 'c')").unwrap();
        db.execute_sql("UPDATE items SET name = 'x' WHERE id = 2").unwrap();
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

    db.execute_sql("CREATE TABLE kv (id BIGINT, val TEXT)").unwrap();

    let schema = db.get_schema("kv").unwrap();

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

    db.execute_sql("CREATE TABLE counter (id BIGINT, val BIGINT)").unwrap();
    db.execute_sql("INSERT INTO counter (id, val) VALUES (1, 0)").unwrap();

    // Start two transactions.
    let txn1 = db.begin().unwrap();
    let txn2 = db.begin().unwrap();

    // Both write to the same key.
    let schema = db.get_schema("counter").unwrap();

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

    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DOUBLE PRECISION)").unwrap();

    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 99.99)").unwrap();

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
        "CREATE TABLE mixed (id BIGINT, flag BOOLEAN, score DOUBLE PRECISION, name TEXT)"
    ).unwrap();

    db.execute_sql(
        "INSERT INTO mixed (id, flag, score, name) VALUES (1, true, 3.14, 'test')"
    ).unwrap();

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
