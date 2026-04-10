//! Web frontend REST API for the database engine.
//!
//! Provides JSON endpoints for authentication, table browsing, SQL execution,
//! and user management. Serves an embedded SPA frontend.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use crate::auth::AuthManager;
use crate::engine::Database;
use crate::query::expr::Value;

// ---------------------------------------------------------------------------
// Static frontend files (embedded at compile time)
// ---------------------------------------------------------------------------

const INDEX_HTML: &str = include_str!("../../frontend/index.html");
const STYLE_CSS: &str = include_str!("../../frontend/style.css");
const APP_JS: &str = include_str!("../../frontend/app.js");

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub auth: Option<Arc<Mutex<AuthManager>>>,
    pub sessions: Arc<Mutex<BTreeMap<String, Session>>>,
}

pub struct Session {
    pub username: String,
}

// ---------------------------------------------------------------------------
// JSON types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    token: String,
    username: String,
}

#[derive(Serialize)]
struct MeResponse {
    username: String,
    auth_enabled: bool,
}

#[derive(Serialize)]
struct TableInfo {
    name: String,
    columns: Vec<ColumnInfo>,
    row_count: usize,
}

#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    col_type: String,
    nullable: bool,
}

#[derive(Serialize)]
struct TableDetail {
    name: String,
    columns: Vec<ColumnInfo>,
    indexes: Vec<IndexInfo>,
    row_count: usize,
}

#[derive(Serialize)]
struct IndexInfo {
    name: String,
    columns: Vec<String>,
    unique: bool,
}

#[derive(Deserialize)]
struct PaginationParams {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Serialize)]
struct DataResponse {
    columns: Vec<String>,
    column_types: Vec<String>,
    rows: Vec<BTreeMap<String, serde_json::Value>>,
    total: usize,
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    #[serde(rename = "type")]
    result_type: String,
    columns: Option<Vec<String>>,
    column_types: Option<Vec<String>>,
    rows: Option<Vec<BTreeMap<String, serde_json::Value>>>,
    affected: Option<u64>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct UserInfo {
    username: String,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    Router::new()
        // Static files
        .route("/", get(serve_index))
        .route("/style.css", get(serve_css))
        .route("/app.js", get(serve_js))
        // Auth
        .route("/api/auth/status", get(auth_status))
        .route("/api/auth/login", post(login))
        .route("/api/auth/register", post(register))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/me", get(me))
        // Tables
        .route("/api/tables", get(list_tables))
        .route("/api/tables/{name}", get(get_table))
        .route("/api/tables/{name}/data", get(get_table_data))
        // Query
        .route("/api/query", post(execute_query))
        // Users
        .route("/api/users", get(list_users))
        .route("/api/users", post(create_user))
        .route("/api/users/{name}", delete(delete_user))
        // Permissive CORS: API is intended to be reached from the Tauri
        // desktop client (origin `tauri://localhost` / `http://tauri.localhost`)
        // as well as the embedded browser UI. Bearer tokens travel in headers,
        // not cookies, so credentialed CORS is not required.
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_session(headers: &HeaderMap, state: &AppState) -> Option<String> {
    // If auth is disabled, allow all requests
    if state.auth.is_none() {
        return Some("anonymous".to_string());
    }
    let auth_header = headers.get("authorization")?.to_str().ok()?;
    let token = auth_header.strip_prefix("Bearer ")?;
    let sessions = state.sessions.lock().ok()?;
    sessions.get(token).map(|s| s.username.clone())
}

fn require_auth(headers: &HeaderMap, state: &AppState) -> Result<String, Response> {
    extract_session(headers, state).ok_or_else(|| {
        (StatusCode::UNAUTHORIZED, Json(ErrorBody { error: "unauthorized".into() })).into_response()
    })
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(n) => serde_json::json!(*n),
        Value::Float64(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(format!("\\x{}", b.iter().map(|b| format!("{:02x}", b)).collect::<String>())),
        Value::Timestamp(us) => serde_json::Value::String(crate::query::expr::format_timestamp(*us)),
        Value::Date(days) => serde_json::Value::String(crate::query::expr::format_date(*days)),
        Value::Uuid(bytes) => serde_json::Value::String(crate::query::expr::format_uuid(bytes)),
        Value::Decimal(val, scale) => serde_json::Value::String(crate::query::expr::format_decimal(*val, *scale)),
    }
}

fn value_type_name(vt: &crate::query::expr::ValueType) -> &'static str {
    use crate::query::expr::ValueType;
    match vt {
        ValueType::Null => "null",
        ValueType::Bool => "bool",
        ValueType::Int64 => "int64",
        ValueType::Float64 => "float64",
        ValueType::Text => "text",
        ValueType::Bytes => "bytes",
        ValueType::Timestamp => "timestamp",
        ValueType::Date => "date",
        ValueType::Uuid => "uuid",
        ValueType::Decimal => "decimal",
    }
}

// ---------------------------------------------------------------------------
// Static file handlers
// ---------------------------------------------------------------------------

async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn serve_css() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/css")],
        STYLE_CSS,
    ).into_response()
}

async fn serve_js() -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/javascript")],
        APP_JS,
    ).into_response()
}

// ---------------------------------------------------------------------------
// Auth endpoints
// ---------------------------------------------------------------------------

async fn auth_status(State(state): State<AppState>) -> Response {
    #[derive(Serialize)]
    struct StatusResp {
        auth_enabled: bool,
    }
    (StatusCode::OK, Json(StatusResp { auth_enabled: state.auth.is_some() })).into_response()
}

async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Response {
    let auth = match &state.auth {
        Some(a) => a,
        None => {
            // Auth disabled — auto-login
            let token = uuid::Uuid::new_v4().to_string();
            let mut sessions = state.sessions.lock().unwrap();
            sessions.insert(token.clone(), Session { username: "anonymous".into() });
            return (StatusCode::OK, Json(LoginResponse { token, username: "anonymous".into() })).into_response();
        }
    };

    let auth_guard = auth.lock().unwrap();
    if auth_guard.authenticate(&req.username, &req.password) {
        drop(auth_guard);
        let token = uuid::Uuid::new_v4().to_string();
        let mut sessions = state.sessions.lock().unwrap();
        sessions.insert(token.clone(), Session { username: req.username.clone() });
        (StatusCode::OK, Json(LoginResponse { token, username: req.username })).into_response()
    } else {
        (StatusCode::UNAUTHORIZED, Json(ErrorBody { error: "invalid credentials".into() })).into_response()
    }
}

async fn register(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Response {
    let auth = match &state.auth {
        Some(a) => a,
        None => {
            return (StatusCode::BAD_REQUEST, Json(ErrorBody { error: "authentication not enabled".into() })).into_response();
        }
    };

    let mut guard = auth.lock().unwrap();
    if guard.user_exists(&req.username) {
        return (StatusCode::CONFLICT, Json(ErrorBody { error: "username already taken".into() })).into_response();
    }
    guard.add_user(&req.username, &req.password);
    drop(guard);

    let token = uuid::Uuid::new_v4().to_string();
    let mut sessions = state.sessions.lock().unwrap();
    sessions.insert(token.clone(), Session { username: req.username.clone() });
    (StatusCode::CREATED, Json(LoginResponse { token, username: req.username })).into_response()
}

async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Some(auth_header) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.remove(token);
        }
    }
    StatusCode::OK.into_response()
}

async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    match require_auth(&headers, &state) {
        Err(r) => r,
        Ok(username) => {
            (StatusCode::OK, Json(MeResponse {
                username,
                auth_enabled: state.auth.is_some(),
            })).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Table endpoints
// ---------------------------------------------------------------------------

async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    match state.db.list_tables() {
        Ok(schemas) => {
            let tables: Vec<TableInfo> = schemas.iter().map(|s| {
                let row_count = state.db.execute_sql(&format!("SELECT COUNT(*) AS c FROM {}", s.table))
                    .ok()
                    .and_then(|r| match r {
                        crate::engine::SqlResult::Query { rows, .. } => {
                            rows.first().and_then(|row| row.get("c")).and_then(|v| match v {
                                Value::Int64(n) => Some(*n as usize),
                                _ => None,
                            })
                        }
                        _ => None,
                    })
                    .unwrap_or(0);
                TableInfo {
                    name: s.table.clone(),
                    columns: s.columns.iter().map(|c| ColumnInfo {
                        name: c.name.clone(),
                        col_type: format!("{:?}", c.col_type),
                        nullable: c.nullable,
                    }).collect(),
                    row_count,
                }
            }).collect();
            (StatusCode::OK, Json(tables)).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: e.to_string() })).into_response()
        }
    }
}

async fn get_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    let schema = match state.db.get_schema(&name) {
        Ok(Some(s)) => s,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(ErrorBody { error: format!("table '{}' not found", name) })).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: e.to_string() })).into_response(),
    };

    let indexes = state.db.list_indexes(&name).unwrap_or_default();
    let row_count = state.db.execute_sql(&format!("SELECT COUNT(*) AS c FROM {}", name))
        .ok()
        .and_then(|r| match r {
            crate::engine::SqlResult::Query { rows, .. } => {
                rows.first().and_then(|row| row.get("c")).and_then(|v| match v {
                    Value::Int64(n) => Some(*n as usize),
                    _ => None,
                })
            }
            _ => None,
        })
        .unwrap_or(0);

    let detail = TableDetail {
        name: schema.table.clone(),
        columns: schema.columns.iter().map(|c| ColumnInfo {
            name: c.name.clone(),
            col_type: format!("{:?}", c.col_type),
            nullable: c.nullable,
        }).collect(),
        indexes: indexes.iter().map(|idx| IndexInfo {
            name: idx.name.clone(),
            columns: idx.columns.clone(),
            unique: idx.unique,
        }).collect(),
        row_count,
    };

    (StatusCode::OK, Json(detail)).into_response()
}

async fn get_table_data(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);

    // Get total count
    let total = state.db.execute_sql(&format!("SELECT COUNT(*) AS c FROM {}", name))
        .ok()
        .and_then(|r| match r {
            crate::engine::SqlResult::Query { rows, .. } => {
                rows.first().and_then(|row| row.get("c")).and_then(|v| match v {
                    Value::Int64(n) => Some(*n as usize),
                    _ => None,
                })
            }
            _ => None,
        })
        .unwrap_or(0);

    let sql = format!("SELECT * FROM {} LIMIT {} OFFSET {}", name, limit, offset);
    match state.db.execute_sql(&sql) {
        Ok(crate::engine::SqlResult::Query { columns, column_types, rows }) => {
            let json_rows: Vec<BTreeMap<String, serde_json::Value>> = rows.iter().map(|row| {
                let mut map = BTreeMap::new();
                for col in &columns {
                    let val = row.get(col).unwrap_or(&Value::Null);
                    map.insert(col.clone(), value_to_json(val));
                }
                map
            }).collect();

            (StatusCode::OK, Json(DataResponse {
                columns,
                column_types: column_types.iter().map(|t| value_type_name(t).to_string()).collect(),
                rows: json_rows,
                total,
            })).into_response()
        }
        Ok(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: "unexpected result".into() })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: e.to_string() })).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Query endpoint
// ---------------------------------------------------------------------------

async fn execute_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    match state.db.execute_sql(&req.sql) {
        Ok(result) => {
            let resp = match result {
                crate::engine::SqlResult::Query { columns, column_types, rows } => {
                    let json_rows: Vec<BTreeMap<String, serde_json::Value>> = rows.iter().map(|row| {
                        let mut map = BTreeMap::new();
                        for col in &columns {
                            let val = row.get(col).unwrap_or(&Value::Null);
                            map.insert(col.clone(), value_to_json(val));
                        }
                        map
                    }).collect();
                    QueryResponse {
                        result_type: "query".into(),
                        columns: Some(columns),
                        column_types: Some(column_types.iter().map(|t| value_type_name(t).to_string()).collect()),
                        rows: Some(json_rows),
                        affected: None,
                        message: None,
                    }
                }
                crate::engine::SqlResult::Execute(count) => {
                    QueryResponse {
                        result_type: "execute".into(),
                        columns: None,
                        column_types: None,
                        rows: None,
                        affected: Some(count),
                        message: Some(format!("{} row(s) affected", count)),
                    }
                }
                crate::engine::SqlResult::Begin(txn_id) => {
                    QueryResponse {
                        result_type: "begin".into(),
                        columns: None,
                        column_types: None,
                        rows: None,
                        affected: None,
                        message: Some(format!("transaction {} started", txn_id)),
                    }
                }
                crate::engine::SqlResult::Commit => {
                    QueryResponse {
                        result_type: "commit".into(),
                        columns: None,
                        column_types: None,
                        rows: None,
                        affected: None,
                        message: Some("committed".into()),
                    }
                }
                crate::engine::SqlResult::Rollback => {
                    QueryResponse {
                        result_type: "rollback".into(),
                        columns: None,
                        column_types: None,
                        rows: None,
                        affected: None,
                        message: Some("rolled back".into()),
                    }
                }
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            (StatusCode::BAD_REQUEST, Json(ErrorBody { error: e.to_string() })).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// User management endpoints
// ---------------------------------------------------------------------------

async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    match &state.auth {
        Some(auth) => {
            let guard = auth.lock().unwrap();
            let users: Vec<UserInfo> = guard.list_users().iter().map(|u| UserInfo { username: u.to_string() }).collect();
            (StatusCode::OK, Json(users)).into_response()
        }
        None => {
            (StatusCode::OK, Json(Vec::<UserInfo>::new())).into_response()
        }
    }
}

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    match &state.auth {
        Some(auth) => {
            let mut guard = auth.lock().unwrap();
            if guard.user_exists(&req.username) {
                return (StatusCode::CONFLICT, Json(ErrorBody { error: "user already exists".into() })).into_response();
            }
            guard.add_user(&req.username, &req.password);
            (StatusCode::CREATED, Json(UserInfo { username: req.username })).into_response()
        }
        None => {
            (StatusCode::BAD_REQUEST, Json(ErrorBody { error: "authentication not enabled".into() })).into_response()
        }
    }
}

async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = require_auth(&headers, &state) { return r; }

    match &state.auth {
        Some(auth) => {
            let mut guard = auth.lock().unwrap();
            if guard.remove_user(&name) {
                StatusCode::NO_CONTENT.into_response()
            } else {
                (StatusCode::NOT_FOUND, Json(ErrorBody { error: "user not found".into() })).into_response()
            }
        }
        None => {
            (StatusCode::BAD_REQUEST, Json(ErrorBody { error: "authentication not enabled".into() })).into_response()
        }
    }
}
