//! Integration tests for the web surface: /metrics, /health, CORS.
//!
//! Only compiled with `--features server` (where axum etc. are available).

#![cfg(feature = "server")]

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use rust_dst_db::engine::Database;
use rust_dst_db::web::metrics::{Metrics, QueryKind};
use rust_dst_db::web::{self, AppState};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust_db_web_{}", name));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn new_state(name: &str) -> AppState {
    let dir = tmp_dir(name);
    let db = Database::open(&dir).unwrap();
    AppState {
        db,
        auth: None,
        sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        metrics: Metrics::new(),
    }
}

#[tokio::test]
async fn metrics_endpoint_renders_prometheus() {
    let state = new_state("metrics_basic");
    state.metrics.record_query(QueryKind::Select);
    state.metrics.record_query(QueryKind::Insert);
    state.metrics.connection_opened();

    let router = web::router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(content_type.starts_with("text/plain"));

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(text.contains("rustdb_queries_total{kind=\"select\"} 1"));
    assert!(text.contains("rustdb_queries_total{kind=\"insert\"} 1"));
    assert!(text.contains("rustdb_active_connections 1"));
}

#[tokio::test]
async fn health_endpoint_returns_ok_json() {
    let state = new_state("health_ok");
    state.db.execute_sql("CREATE TABLE t (id BIGINT)").unwrap();

    let router = web::router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["tables"], 1);
}

#[tokio::test]
async fn metrics_reflects_engine_checkpoint_count() {
    let state = new_state("metrics_checkpoint_sync");
    state.db.execute_sql("CREATE TABLE t (id BIGINT)").unwrap();
    state.db.checkpoint().unwrap();
    state.db.checkpoint().unwrap();

    let router = web::router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(
        text.contains("rustdb_checkpoints_total 2"),
        "expected checkpoint counter to be 2, got:\n{}",
        text
    );
}
