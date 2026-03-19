//! Integration tests for secondary indexes.

use rust_dst_db::engine::{Database, DbError, SqlResult};
use rust_dst_db::query::expr::Value;

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust_db_idx_test_{}", name));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn create_index_basic() {
    let dir = tmp_dir("create_idx_basic");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT, age BIGINT)").unwrap();
    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();

    let idx = db.get_index("idx_users_name").unwrap().unwrap();
    assert_eq!(idx.table, "users");
    assert_eq!(idx.columns, vec!["name".to_string()]);
    assert!(!idx.unique);
}

#[test]
fn create_unique_index() {
    let dir = tmp_dir("create_unique_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, email TEXT)").unwrap();
    db.execute_sql("CREATE UNIQUE INDEX idx_users_email ON users (email)").unwrap();

    let idx = db.get_index("idx_users_email").unwrap().unwrap();
    assert!(idx.unique);
}

#[test]
fn drop_index() {
    let dir = tmp_dir("drop_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();
    assert!(db.get_index("idx_users_name").unwrap().is_some());

    db.execute_sql("DROP INDEX idx_users_name").unwrap();
    assert!(db.get_index("idx_users_name").unwrap().is_none());
}

#[test]
fn insert_maintains_index() {
    let dir = tmp_dir("insert_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();

    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    let schema = db.get_schema("users").unwrap().unwrap();
    let idx = db.get_index("idx_users_name").unwrap().unwrap();
    let rows = db.index_lookup(&schema, &idx, &[Value::Text("Alice".to_string())]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Int64(1)));
}

#[test]
fn update_maintains_index() {
    let dir = tmp_dir("update_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();

    db.execute_sql("UPDATE users SET name = 'Alicia' WHERE id = 1").unwrap();

    let schema = db.get_schema("users").unwrap().unwrap();
    let idx = db.get_index("idx_users_name").unwrap().unwrap();

    let old_rows = db.index_lookup(&schema, &idx, &[Value::Text("Alice".to_string())]).unwrap();
    assert_eq!(old_rows.len(), 0);

    let new_rows = db.index_lookup(&schema, &idx, &[Value::Text("Alicia".to_string())]).unwrap();
    assert_eq!(new_rows.len(), 1);
    assert_eq!(new_rows[0].get("id"), Some(&Value::Int64(1)));
}

#[test]
fn delete_maintains_index() {
    let dir = tmp_dir("delete_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    db.execute_sql("DELETE FROM users WHERE id = 1").unwrap();

    let schema = db.get_schema("users").unwrap().unwrap();
    let idx = db.get_index("idx_users_name").unwrap().unwrap();

    let rows = db.index_lookup(&schema, &idx, &[Value::Text("Alice".to_string())]).unwrap();
    assert_eq!(rows.len(), 0);

    let rows = db.index_lookup(&schema, &idx, &[Value::Text("Bob".to_string())]).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn unique_index_rejects_duplicate() {
    let dir = tmp_dir("unique_idx_dup");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, email TEXT)").unwrap();
    db.execute_sql("CREATE UNIQUE INDEX idx_users_email ON users (email)").unwrap();

    db.execute_sql("INSERT INTO users (id, email) VALUES (1, 'alice@test.com')").unwrap();
    let result = db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'alice@test.com')");

    assert!(result.is_err());
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    match err {
        DbError::Constraint(msg) => {
            assert!(msg.contains("unique index"), "expected unique index error, got: {}", msg);
        }
        other => panic!("expected Constraint error, got: {:?}", other),
    }
}

#[test]
fn index_lookup_returns_correct_rows() {
    let dir = tmp_dir("idx_lookup");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE products (id BIGINT NOT NULL, category TEXT, name TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_products_category ON products (category)").unwrap();

    db.execute_sql("INSERT INTO products (id, category, name) VALUES (1, 'electronics', 'laptop')").unwrap();
    db.execute_sql("INSERT INTO products (id, category, name) VALUES (2, 'electronics', 'phone')").unwrap();
    db.execute_sql("INSERT INTO products (id, category, name) VALUES (3, 'books', 'novel')").unwrap();

    let schema = db.get_schema("products").unwrap().unwrap();
    let idx = db.get_index("idx_products_category").unwrap().unwrap();

    let electronics = db.index_lookup(&schema, &idx, &[Value::Text("electronics".to_string())]).unwrap();
    assert_eq!(electronics.len(), 2);

    let books = db.index_lookup(&schema, &idx, &[Value::Text("books".to_string())]).unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].get("name"), Some(&Value::Text("novel".to_string())));
}

#[test]
fn multi_column_index() {
    let dir = tmp_dir("multi_col_idx");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE orders (id BIGINT NOT NULL, customer TEXT, status TEXT)").unwrap();
    db.execute_sql("CREATE INDEX idx_orders_cust_status ON orders (customer, status)").unwrap();

    db.execute_sql("INSERT INTO orders (id, customer, status) VALUES (1, 'Alice', 'shipped')").unwrap();
    db.execute_sql("INSERT INTO orders (id, customer, status) VALUES (2, 'Alice', 'pending')").unwrap();
    db.execute_sql("INSERT INTO orders (id, customer, status) VALUES (3, 'Bob', 'shipped')").unwrap();

    let schema = db.get_schema("orders").unwrap().unwrap();
    let idx = db.get_index("idx_orders_cust_status").unwrap().unwrap();

    let rows = db.index_lookup(&schema, &idx, &[
        Value::Text("Alice".to_string()),
        Value::Text("shipped".to_string()),
    ]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Int64(1)));
}

#[test]
fn index_survives_wal_recovery() {
    let dir = tmp_dir("idx_wal_recovery");

    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
        db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();
    }

    {
        let db = Database::open(&dir).unwrap();
        let idx = db.get_index("idx_users_name").unwrap();
        assert!(idx.is_some(), "index definition should survive WAL recovery");
        let idx = idx.unwrap();
        assert_eq!(idx.table, "users");
        assert_eq!(idx.columns, vec!["name".to_string()]);

        match db.execute_sql("SELECT * FROM users").unwrap() {
            SqlResult::Query { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected Query result"),
        }
    }
}

#[test]
fn list_indexes_for_table() {
    let dir = tmp_dir("list_indexes");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT, name TEXT, email TEXT)").unwrap();
    db.execute_sql("CREATE TABLE orders (id BIGINT, total BIGINT)").unwrap();

    db.execute_sql("CREATE INDEX idx_users_name ON users (name)").unwrap();
    db.execute_sql("CREATE UNIQUE INDEX idx_users_email ON users (email)").unwrap();
    db.execute_sql("CREATE INDEX idx_orders_total ON orders (total)").unwrap();

    let user_indexes = db.list_indexes("users").unwrap();
    assert_eq!(user_indexes.len(), 2);

    let order_indexes = db.list_indexes("orders").unwrap();
    assert_eq!(order_indexes.len(), 1);
}

#[test]
fn create_index_on_nonexistent_table_fails() {
    let dir = tmp_dir("idx_no_table");
    let db = Database::open(&dir).unwrap();

    let result = db.execute_sql("CREATE INDEX idx_foo_bar ON foo (bar)");
    assert!(result.is_err());
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    match err {
        DbError::NoSuchTable(t) => assert_eq!(t, "foo"),
        other => panic!("expected NoSuchTable, got: {:?}", other),
    }
}
