//! Stage 5 pgwire adapter tests.
//!
//! Run with: `cargo test --test pgwire_tests --features pgwire`

#![cfg(feature = "pgwire")]

use std::collections::BTreeMap;

use rust_dst_db::net::pgwire_adapter::{poll_external, ClientRequest, ClientResponse};
use rust_dst_db::query::expr::{Column, Row, Schema, Value, ValueType};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn users_schema() -> Schema {
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
    ]
}

/// Issue a single SQL request through `poll_external` and return the response.
fn query(sql: &str, schema: &Schema, rows: &[Row]) -> ClientResponse {
    use std::sync::mpsc;

    let (req_tx, req_rx) = mpsc::sync_channel(1);
    let (resp_tx, resp_rx) = mpsc::sync_channel(1);

    req_tx
        .send(ClientRequest {
            conn_id: 1,
            sql: sql.to_string(),
            reply_tx: resp_tx,
        })
        .unwrap();

    poll_external(&req_rx, schema, rows);

    resp_rx.recv().unwrap()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// SELECT * returns all rows without error.
#[test]
fn pgwire_simple_select() {
    let schema = users_schema();
    let rows = sample_rows();

    let resp = query("SELECT * FROM users", &schema, &rows);

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    assert_eq!(resp.rows.len(), 3, "expected all 3 rows");
}

/// SELECT with WHERE clause filters rows correctly.
#[test]
fn pgwire_where_clause() {
    let schema = users_schema();
    let rows = sample_rows();

    let resp = query("SELECT * FROM users WHERE age > 25", &schema, &rows);

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    // Alice(30) and Carol(35) qualify; Bob(25) does not.
    assert_eq!(resp.rows.len(), 2, "expected 2 rows with age > 25");
    for row in &resp.rows {
        if let Some(Value::Int64(age)) = row.get("age") {
            assert!(*age > 25, "row with age {} should not pass filter", age);
        }
    }
}

/// BEGIN and COMMIT are accepted as no-ops (transaction control stubs).
#[test]
fn pgwire_begin_commit() {
    let schema = users_schema();
    let rows = sample_rows();

    let begin_resp = query("BEGIN", &schema, &rows);
    assert!(begin_resp.error.is_none(), "BEGIN should not error");
    assert!(begin_resp.rows.is_empty(), "BEGIN returns no rows");

    let commit_resp = query("COMMIT", &schema, &rows);
    assert!(commit_resp.error.is_none(), "COMMIT should not error");
    assert!(commit_resp.rows.is_empty(), "COMMIT returns no rows");
}

/// ROLLBACK is accepted as a no-op.
#[test]
fn pgwire_rollback() {
    let schema = users_schema();
    let rows = sample_rows();

    let resp = query("ROLLBACK", &schema, &rows);
    assert!(resp.error.is_none(), "ROLLBACK should not error");
    assert!(resp.rows.is_empty(), "ROLLBACK returns no rows");
}

/// Multiple concurrent clients get isolated responses (sequential under the
/// poll_external model; each query runs against a consistent snapshot).
#[test]
fn pgwire_multiple_clients() {
    use std::sync::mpsc;

    let schema = users_schema();
    let rows = sample_rows();

    // Enqueue two requests simultaneously.
    let (req_tx, req_rx) = mpsc::sync_channel(2);
    let (resp_tx1, resp_rx1) = mpsc::sync_channel(1);
    let (resp_tx2, resp_rx2) = mpsc::sync_channel(1);

    req_tx
        .send(ClientRequest {
            conn_id: 1,
            sql: "SELECT * FROM users WHERE id = 1".to_string(),
            reply_tx: resp_tx1,
        })
        .unwrap();
    req_tx
        .send(ClientRequest {
            conn_id: 2,
            sql: "SELECT * FROM users WHERE id = 2".to_string(),
            reply_tx: resp_tx2,
        })
        .unwrap();

    poll_external(&req_rx, &schema, &rows);

    let resp1 = resp_rx1.recv().unwrap();
    let resp2 = resp_rx2.recv().unwrap();

    assert!(resp1.error.is_none());
    assert_eq!(resp1.rows.len(), 1);
    assert_eq!(resp1.rows[0]["id"], Value::Int64(1));

    assert!(resp2.error.is_none());
    assert_eq!(resp2.rows.len(), 1);
    assert_eq!(resp2.rows[0]["id"], Value::Int64(2));
}
