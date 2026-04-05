//! rust-db-server — PostgreSQL-compatible database server.
//!
//! Accepts connections via the PostgreSQL wire protocol, executes SQL against
//! the real `Database` engine with MVCC transactions, WAL durability, and
//! file-backed persistence.
//!
//! Usage:
//!   cargo run --features server --bin rust-db-server -- --data-dir ./mydb --port 5433
//!
//! With authentication:
//!   cargo run --features server --bin rust-db-server -- --port 5433 --user admin --password secret
//!
//! Connect with:
//!   psql -h 127.0.0.1 -p 5433
//!   psql -h 127.0.0.1 -p 5433 -U admin   (when auth enabled)

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use futures::{stream, Sink, SinkExt};
use tokio::net::TcpListener;

use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::ClientInfo;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse,
    FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::ClientPortalStore;
use pgwire::api::{PgWireConnectionState, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

use rust_dst_db::auth::AuthManager;
use rust_dst_db::engine::{Database, SqlResult};
use rust_dst_db::query::expr::Value;
use rust_dst_db::web;

use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "rust-db-server", about = "Rust-DB PostgreSQL-compatible server")]
struct Args {
    /// Data directory for WAL and storage files.
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    /// Port to listen on.
    #[arg(short, long, default_value_t = 5433)]
    port: u16,

    /// Bind address.
    #[arg(short, long, default_value = "127.0.0.1")]
    bind: String,

    /// Port for the web UI.
    #[arg(short, long, default_value_t = 8080)]
    web_port: u16,

    /// Username for authentication. If provided, authentication is enabled.
    #[arg(short, long)]
    user: Option<String>,

    /// Password for the authenticated user.
    #[arg(long)]
    password: Option<String>,
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/// Startup handler that supports optional cleartext password authentication.
///
/// When `auth` is `Some`, the handler requests a cleartext password from the
/// client and verifies it against the `AuthManager`. When `auth` is `None`,
/// the handler behaves like the noop handler (no auth required).
struct DbStartupHandler {
    auth: Option<Arc<Mutex<AuthManager>>>,
    params: Arc<DefaultServerParameterProvider>,
}

#[async_trait]
impl StartupHandler for DbStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match (&self.auth, message) {
            // --- Auth disabled: noop startup ---
            (None, PgWireFrontendMessage::Startup(ref startup)) => {
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                pgwire::api::auth::finish_authentication(client, self.params.as_ref()).await?;
            }

            // --- Auth enabled: request password on Startup ---
            (Some(_), PgWireFrontendMessage::Startup(ref startup)) => {
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }

            // --- Auth enabled: verify password ---
            (Some(auth), PgWireFrontendMessage::PasswordMessageFamily(pwd)) => {
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);
                let username = login_info.user().unwrap_or("");

                if auth.lock().unwrap().authenticate(username, &pwd.password) {
                    info!(user = username, "authentication successful");
                    pgwire::api::auth::finish_authentication(client, self.params.as_ref()).await?;
                } else {
                    warn!(user = username, "authentication failed");
                    let error_info = ErrorInfo::new(
                        "FATAL".to_string(),
                        "28P01".to_string(),
                        "password authentication failed".to_string(),
                    );
                    let error = ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }

            _ => {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Query handling (unchanged logic, separated from startup handler)
// ---------------------------------------------------------------------------

/// Per-connection state tracking active transactions.
struct ConnState {
    active_txn: Option<u64>,
}

struct DbQueryHandler {
    db: Database,
    conn_id: u64,
    state: Arc<Mutex<ConnState>>,
}

impl DbQueryHandler {
    fn new(db: Database, conn_id: u64) -> Self {
        Self {
            db,
            conn_id,
            state: Arc::new(Mutex::new(ConnState { active_txn: None })),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DbQueryHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!(conn_id = self.conn_id, sql = query, "query");

        let statements: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for stmt in statements {
            match self.execute_one(stmt) {
                Ok(resp) => responses.push(resp),
                Err(e) => {
                    error!(conn_id = self.conn_id, error = %e, "query error");
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "42000".to_string(),
                        e.to_string(),
                    ))));
                }
            }
        }

        Ok(responses)
    }
}

impl DbQueryHandler {
    fn execute_one<'a>(&self, sql: &str) -> Result<Response<'a>, Box<dyn std::error::Error>> {
        let preprocessed = preprocess_sql(sql);
        if let Some(intercepted) = intercept_system_query(&preprocessed, &self.db) {
            return Ok(intercepted);
        }
        let result = self.db.execute_sql(&preprocessed)?;
        execute_sql_result(result, &self.state, &self.db)
    }
}

fn value_type_to_pg_type(vt: &rust_dst_db::query::expr::ValueType) -> Type {
    use rust_dst_db::query::expr::ValueType;
    match vt {
        ValueType::Bool => Type::BOOL,
        ValueType::Int64 => Type::INT8,
        ValueType::Float64 => Type::FLOAT8,
        ValueType::Text => Type::TEXT,
        ValueType::Bytes => Type::BYTEA,
        ValueType::Timestamp => Type::TIMESTAMP,
        ValueType::Date => Type::DATE,
        ValueType::Uuid => Type::UUID,
        ValueType::Decimal => Type::NUMERIC,
        ValueType::Null => Type::TEXT,
    }
}

fn execute_sql_result<'a>(
    result: SqlResult,
    state: &Arc<Mutex<ConnState>>,
    db: &Database,
) -> Result<Response<'a>, Box<dyn std::error::Error>> {
    match result {
        SqlResult::Begin(txn_id) => {
            let mut state = state.lock().unwrap();
            state.active_txn = Some(txn_id);
            Ok(Response::Execution(Tag::new("BEGIN")))
        }
        SqlResult::Commit => {
            let mut state = state.lock().unwrap();
            if let Some(txn_id) = state.active_txn.take() {
                db.commit(txn_id)?;
            }
            Ok(Response::Execution(Tag::new("COMMIT")))
        }
        SqlResult::Rollback => {
            let mut state = state.lock().unwrap();
            if let Some(txn_id) = state.active_txn.take() {
                db.abort(txn_id)?;
            }
            Ok(Response::Execution(Tag::new("ROLLBACK")))
        }
        SqlResult::Execute(count) => {
            Ok(Response::Execution(Tag::new(&format!("OK {}", count))))
        }
        SqlResult::Query { columns, column_types, rows } => {
            // Strip table prefix (e.g. "devices.id" → "id") to match PostgreSQL convention.
            // The engine keeps qualified names internally; clients expect unqualified names.
            let display_names: Vec<String> = columns.iter().map(|name| {
                if let Some(pos) = name.rfind('.') { name[pos+1..].to_string() } else { name.clone() }
            }).collect();

            let fields: Vec<FieldInfo> = display_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let pg_type = column_types.get(i).map(value_type_to_pg_type).unwrap_or(Type::TEXT);
                    FieldInfo::new(name.clone(), None, None, pg_type, FieldFormat::Text)
                })
                .collect();
            let schema = Arc::new(fields);

            let rows_data: Vec<_> = rows
                .iter()
                .map(|row| {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    for (i, col) in columns.iter().enumerate() {
                        // Try qualified name first, then unqualified fallback.
                        let val = row.get(col)
                            .or_else(|| row.get(display_names[i].as_str()))
                            .unwrap_or(&Value::Null);
                        if val.is_null() {
                            encoder.encode_field(&None::<&str>)?;
                        } else {
                            let text = value_to_text(val);
                            encoder.encode_field(&Some(&text))?;
                        }
                    }
                    encoder.finish()
                })
                .collect();

            Ok(Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows_data),
            )))
        }
    }
}

fn value_to_text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => format!("{:?}", b),
        Value::Timestamp(us) => rust_dst_db::query::expr::format_timestamp(*us),
        Value::Date(days) => rust_dst_db::query::expr::format_date(*days),
        Value::Uuid(bytes) => rust_dst_db::query::expr::format_uuid(bytes),
        Value::Decimal(val, scale) => rust_dst_db::query::expr::format_decimal(*val, *scale),
    }
}

// ---------------------------------------------------------------------------
// SQL preprocessing — strip PostgreSQL ::type casts before execution
// ---------------------------------------------------------------------------

/// Strip `::typename` casts so Rust-DB's engine can parse SQLAlchemy/asyncpg SQL.
/// Handles `$1::jsonb`, `'foo'::text`, `NULL::integer`, and compound types like
/// `::double precision` and `::character varying`.
/// Ignores `::` that appear inside single-quoted string literals.
fn preprocess_sql(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut result = String::with_capacity(n);
    let mut i = 0;
    while i < n {
        // Consume a single-quoted string literal verbatim.
        if chars[i] == '\'' {
            result.push('\'');
            i += 1;
            while i < n {
                if chars[i] == '\'' {
                    result.push('\'');
                    i += 1;
                    if i < n && chars[i] == '\'' {
                        result.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    result.push(chars[i]);
                    i += 1;
                }
            }
            continue;
        }
        // Detect and strip :: cast operator.
        if i + 1 < n && chars[i] == ':' && chars[i + 1] == ':' {
            i += 2;
            // Consume the first identifier word (e.g. "timestamp", "character", "double").
            while i < n && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            // Skip [] for array types.
            while i < n && (chars[i] == '[' || chars[i] == ']') {
                i += 1;
            }
            // Consume multi-word compound type suffixes (case-insensitive).
            // Build lowercase slice of remaining chars to match against known patterns.
            let rest_lower: String = chars[i..].iter().collect::<String>().to_lowercase();
            let suffixes = [
                " without time zone",
                " with time zone",
                " time zone",
                " precision",
                " varying",
                " zone",
            ];
            for suffix in suffixes {
                if rest_lower.starts_with(suffix) {
                    i += suffix.len();
                    break;
                }
            }
            // Skip [] after compound types too.
            while i < n && (chars[i] == '[' || chars[i] == ']') {
                i += 1;
            }
            continue;
        }
        result.push(chars[i]);
        i += 1;
    }
    result
}

// ---------------------------------------------------------------------------
// System / catalog query interception helpers
// ---------------------------------------------------------------------------

fn empty_query_response<'a>() -> Response<'a> {
    Response::Query(QueryResponse::new(Arc::new(vec![]), stream::iter(vec![])))
}

fn single_text_row<'a>(col_name: &str, value: &str) -> Response<'a> {
    let fields = vec![FieldInfo::new(col_name.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
    let schema = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let s = value.to_string();
    let _ = encoder.encode_field(&Some(s.as_str()));
    let row = encoder.finish();
    Response::Query(QueryResponse::new(schema, stream::iter(vec![row])))
}

fn single_bool_row<'a>(col_name: &str, value: bool) -> Response<'a> {
    // Use TEXT type to avoid binary-format decoding issues with asyncpg.
    let fields = vec![FieldInfo::new(col_name.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
    let schema = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&Some(if value { "t" } else { "f" }));
    let row = encoder.finish();
    Response::Query(QueryResponse::new(schema, stream::iter(vec![row])))
}

/// Extract the value from `col = 'value'` in a normalised (lowercase) SQL string.
fn extract_eq_filter(norm: &str, col: &str) -> Option<String> {
    // Search all occurrences of `col` until we find one followed by `= 'value'`
    let mut search_from = 0;
    while let Some(pos) = norm[search_from..].find(col) {
        let abs_pos = search_from + pos;
        let rest = norm[abs_pos + col.len()..].trim_start();
        if let Some(rest) = rest.strip_prefix('=') {
            let rest = rest.trim_start();
            if let Some(rest) = rest.strip_prefix('\'') {
                if let Some(end) = rest.find('\'') {
                    return Some(rest[..end].to_string());
                }
            }
        }
        search_from = abs_pos + col.len();
    }
    None
}

fn value_type_to_sql_type(vt: &rust_dst_db::query::expr::ValueType) -> &'static str {
    use rust_dst_db::query::expr::ValueType;
    match vt {
        ValueType::Bool => "boolean",
        ValueType::Int64 => "bigint",
        ValueType::Float64 => "double precision",
        ValueType::Text => "text",
        ValueType::Bytes => "bytea",
        ValueType::Timestamp => "timestamp without time zone",
        ValueType::Date => "date",
        ValueType::Uuid => "uuid",
        ValueType::Decimal => "numeric",
        ValueType::Null => "text",
    }
}

fn information_schema_tables_response<'a>(norm: &str, db: &Database) -> Response<'a> {
    let fields = vec![
        FieldInfo::new("table_catalog".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("table_schema".into(),  None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("table_name".into(),    None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("table_type".into(),    None, None, Type::TEXT, FieldFormat::Text),
    ];
    let schema = Arc::new(fields);

    let tables = db.list_tables().unwrap_or_default();
    let is_exists = norm.trim_start().starts_with("select exists");
    let name_filter = extract_eq_filter(norm, "table_name");

    let filtered: Vec<_> = tables.iter()
        .filter(|t| name_filter.as_deref().map_or(true, |f| t.table == f))
        .collect();

    if is_exists {
        return single_bool_row("exists", !filtered.is_empty());
    }

    let rows: Vec<_> = filtered.iter().map(|t| {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&Some("rustdb"));
        let _ = encoder.encode_field(&Some("public"));
        let _ = encoder.encode_field(&Some(t.table.as_str()));
        let _ = encoder.encode_field(&Some("BASE TABLE"));
        encoder.finish()
    }).collect();

    Response::Query(QueryResponse::new(schema, stream::iter(rows)))
}

fn information_schema_columns_response<'a>(norm: &str, db: &Database) -> Response<'a> {
    // All TEXT to avoid binary-format decoding issues with asyncpg.
    let fields = vec![
        FieldInfo::new("table_name".into(),               None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("column_name".into(),              None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("ordinal_position".into(),         None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("column_default".into(),           None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("is_nullable".into(),              None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("data_type".into(),                None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("character_maximum_length".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("numeric_precision".into(),        None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("numeric_scale".into(),            None, None, Type::TEXT, FieldFormat::Text),
    ];
    let schema = Arc::new(fields);

    let tables = db.list_tables().unwrap_or_default();
    let table_filter = extract_eq_filter(norm, "table_name");

    let mut rows = Vec::new();
    for tbl in &tables {
        if table_filter.as_deref().map_or(false, |f| tbl.table != f) {
            continue;
        }
        for (i, col) in tbl.columns.iter().enumerate() {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let pos = (i as i64 + 1).to_string();
            let _ = encoder.encode_field(&Some(tbl.table.as_str()));
            let _ = encoder.encode_field(&Some(col.name.as_str()));
            let _ = encoder.encode_field(&Some(pos.as_str()));
            let _ = encoder.encode_field(&None::<&str>);
            let _ = encoder.encode_field(&Some(if col.nullable { "YES" } else { "NO" }));
            let _ = encoder.encode_field(&Some(value_type_to_sql_type(&col.col_type)));
            let _ = encoder.encode_field(&None::<&str>);
            let _ = encoder.encode_field(&None::<&str>);
            let _ = encoder.encode_field(&None::<&str>);
            rows.push(encoder.finish());
        }
    }

    Response::Query(QueryResponse::new(schema, stream::iter(rows)))
}

/// Intercept PostgreSQL system/catalog queries that Rust-DB cannot execute natively.
/// Returns `Some(Response)` if intercepted; `None` to let the engine handle it.
fn intercept_system_query<'a>(sql: &str, db: &Database) -> Option<Response<'a>> {
    let norm = sql.trim().to_lowercase();

    // ── DDL / connection-control commands ─────────────────────────────────
    if norm.starts_with("set ") || norm == "set"
        || norm.starts_with("reset ") || norm == "reset"
        || norm.starts_with("discard ") || norm == "discard"
        || norm.starts_with("deallocate ") || norm == "deallocate"
    {
        return Some(Response::Execution(Tag::new("SET")));
    }

    if norm.starts_with("show ") {
        let param = norm["show ".len()..].trim();
        let value = match param {
            "transaction_isolation" => "read committed",
            "timezone" | "time zone" => "UTC",
            "server_encoding" | "client_encoding" => "UTF8",
            "server_version" => "14.0",
            "search_path" => "public",
            "max_connections" => "100",
            _ => "",
        };
        return Some(single_text_row(param, value));
    }

    // ── version() — intercept before the broad pg_catalog catch-all ───────
    if norm.contains("version()") {
        return Some(single_text_row("version", "PostgreSQL 14.0 on Rust-DB (custom engine)"));
    }

    // ── pg_catalog.pg_class — SQLAlchemy table-existence check ───────────
    // Intercept: SELECT pg_catalog.pg_class.relname FROM pg_catalog.pg_class ...
    // WHERE pg_catalog.pg_class.relname = 'tablename' ...
    if (norm.contains("pg_class") || norm.contains("pg_catalog."))
        && norm.contains("relname")
        && norm.starts_with("select")
    {
        // Extract the relname filter value ('devices', 'ports', etc.)
        if let Some(table_name) = extract_eq_filter(&norm, "relname") {
            let exists = db.get_schema(&table_name).ok().flatten().is_some();
            if exists {
                let schema = Arc::new(vec![
                    FieldInfo::new("relname".into(), None, None, Type::TEXT, FieldFormat::Text),
                ]);
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field_with_type_and_format(&table_name, &Type::TEXT, FieldFormat::Text).ok();
                return Some(Response::Query(QueryResponse::new(schema, stream::iter(vec![encoder.finish()]))));
            } else {
                return Some(empty_query_response());
            }
        }
        return Some(empty_query_response());
    }

    // ── pg_catalog / system catalog (everything else) ─────────────────────
    if norm.contains("pg_catalog.")
        || norm.contains("pg_type")
        || norm.contains("pg_namespace")
        || norm.contains("pg_class")
        || norm.contains("pg_attribute")
        || norm.contains("pg_index")
        || norm.contains("pg_constraint")
        || norm.contains("pg_description")
        || norm.contains("pg_statistic")
        || norm.contains("pg_available")
        || norm.contains("pg_is_in_recovery")
        || norm.contains("pg_encoding_to_char")
    {
        return Some(empty_query_response());
    }

    // ── information_schema virtual tables ─────────────────────────────────
    if norm.contains("information_schema.tables") {
        return Some(information_schema_tables_response(&norm, db));
    }
    if norm.contains("information_schema.columns") {
        return Some(information_schema_columns_response(&norm, db));
    }
    if norm.contains("information_schema.") {
        return Some(empty_query_response());
    }

    // ── scalar connection functions ────────────────────────────────────────
    if norm.contains("current_schema()") || norm == "select current_schema" {
        return Some(single_text_row("current_schema", "public"));
    }
    if norm.contains("current_database()") {
        return Some(single_text_row("current_database", "rustdb"));
    }
    if norm.contains("current_setting(") {
        return Some(single_text_row("current_setting", ""));
    }

    // ── SELECT without FROM (scalar query) ────────────────────────────────
    // Rust-DB's engine requires at least one FROM table. Intercept any
    // SELECT that has no FROM clause (function calls already handled above).
    // Use word-boundary check to handle newlines/tabs before FROM keyword.
    let has_from_kw = norm.split_whitespace().any(|t| t == "from");
    if norm.starts_with("select ") && !has_from_kw {
        return Some(evaluate_scalar_select(&norm));
    }

    None
}

/// Evaluate a scalar SELECT with no FROM clause.
/// Handles: `SELECT 1`, `SELECT 1 AS n`, `SELECT 'foo'`, `SELECT true`, `SELECT NULL`.
fn evaluate_scalar_select<'a>(norm: &str) -> Response<'a> {
    let rest = norm["select ".len()..].trim();

    // Parse: <expr> [AS <alias>]
    let (expr_raw, alias) = if let Some(pos) = rest.find(" as ") {
        (rest[..pos].trim(), rest[pos + 4..].trim())
    } else {
        (rest.trim(), "?column?")
    };

    // Integer literal — encode as TEXT to avoid binary-format issues with asyncpg.
    if let Ok(n) = expr_raw.parse::<i64>() {
        let fields = vec![FieldInfo::new(alias.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
        let schema = Arc::new(fields);
        let ns = n.to_string();
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&Some(ns.as_str()));
        return Response::Query(QueryResponse::new(schema, stream::iter(vec![encoder.finish()])));
    }
    // Float literal
    if let Ok(f) = expr_raw.parse::<f64>() {
        let fields = vec![FieldInfo::new(alias.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
        let schema = Arc::new(fields);
        let fs = f.to_string();
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&Some(fs.as_str()));
        return Response::Query(QueryResponse::new(schema, stream::iter(vec![encoder.finish()])));
    }
    // Boolean
    if expr_raw == "true" { return single_bool_row(alias, true); }
    if expr_raw == "false" { return single_bool_row(alias, false); }
    // NULL
    if expr_raw == "null" {
        let fields = vec![FieldInfo::new(alias.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
        let schema = Arc::new(fields);
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&None::<&str>);
        return Response::Query(QueryResponse::new(schema, stream::iter(vec![encoder.finish()])));
    }
    // Quoted string: 'foo' or "foo"
    if (expr_raw.starts_with('\'') && expr_raw.ends_with('\''))
        || (expr_raw.starts_with('"') && expr_raw.ends_with('"'))
    {
        let val = &expr_raw[1..expr_raw.len() - 1];
        return single_text_row(alias, val);
    }
    // Unknown expression: return a NULL text row rather than erroring.
    let fields = vec![FieldInfo::new(alias.to_string(), None, None, Type::TEXT, FieldFormat::Text)];
    let schema = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&None::<&str>);
    Response::Query(QueryResponse::new(schema, stream::iter(vec![encoder.finish()])))
}

/// Return only the column schema for an intercepted system query — no rows.
/// Must be exactly consistent with what `intercept_system_query` produces.
/// Returns `Some(fields)` (possibly empty) when intercepted, `None` otherwise.
fn field_info_for_intercept(sql: &str, db: &Database) -> Option<Vec<FieldInfo>> {
    let norm = sql.trim().to_lowercase();

    // Control commands → no result columns.
    if norm.starts_with("set ") || norm == "set"
        || norm.starts_with("reset ") || norm == "reset"
        || norm.starts_with("discard ") || norm == "discard"
        || norm.starts_with("deallocate ") || norm == "deallocate"
    {
        return Some(vec![]);
    }

    // SHOW → 1 text column named after the parameter.
    if norm.starts_with("show ") {
        let param = norm["show ".len()..].trim().to_string();
        return Some(vec![FieldInfo::new(param, None, None, Type::TEXT, FieldFormat::Text)]);
    }

    if norm.contains("version()") {
        return Some(vec![FieldInfo::new("version".into(), None, None, Type::TEXT, FieldFormat::Text)]);
    }

    // pg_class relname query (table existence check) — returns 1 relname column.
    if (norm.contains("pg_class") || norm.contains("pg_catalog."))
        && norm.contains("relname")
        && norm.starts_with("select")
    {
        return Some(vec![FieldInfo::new("relname".into(), None, None, Type::TEXT, FieldFormat::Text)]);
    }

    if norm.contains("pg_catalog.")
        || norm.contains("pg_type")
        || norm.contains("pg_namespace")
        || norm.contains("pg_class")
        || norm.contains("pg_attribute")
        || norm.contains("pg_index")
        || norm.contains("pg_constraint")
        || norm.contains("pg_description")
        || norm.contains("pg_statistic")
        || norm.contains("pg_available")
        || norm.contains("pg_is_in_recovery")
        || norm.contains("pg_encoding_to_char")
    {
        return Some(vec![]);
    }

    if norm.contains("information_schema.tables") {
        if norm.trim_start().starts_with("select exists") {
            return Some(vec![FieldInfo::new("exists".into(), None, None, Type::TEXT, FieldFormat::Text)]);
        }
        return Some(vec![
            FieldInfo::new("table_catalog".into(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("table_schema".into(),  None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("table_name".into(),    None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("table_type".into(),    None, None, Type::TEXT, FieldFormat::Text),
        ]);
    }
    if norm.contains("information_schema.columns") {
        // All TEXT — avoids binary-format decoding issues with asyncpg.
        return Some(vec![
            FieldInfo::new("table_name".into(),               None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("column_name".into(),              None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("ordinal_position".into(),         None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("column_default".into(),           None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("is_nullable".into(),              None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("data_type".into(),                None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("character_maximum_length".into(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("numeric_precision".into(),        None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("numeric_scale".into(),            None, None, Type::TEXT, FieldFormat::Text),
        ]);
    }
    if norm.contains("information_schema.") {
        return Some(vec![]);
    }

    if norm.contains("current_schema()") || norm == "select current_schema" {
        return Some(vec![FieldInfo::new("current_schema".into(), None, None, Type::TEXT, FieldFormat::Text)]);
    }
    if norm.contains("current_database()") {
        return Some(vec![FieldInfo::new("current_database".into(), None, None, Type::TEXT, FieldFormat::Text)]);
    }
    if norm.contains("current_setting(") {
        return Some(vec![FieldInfo::new("current_setting".into(), None, None, Type::TEXT, FieldFormat::Text)]);
    }

    // Scalar SELECT without FROM — mirror evaluate_scalar_select's column logic.
    // Use word-boundary check to handle newlines/tabs before FROM.
    let has_from = norm.split_whitespace().any(|t| t == "from");
    if norm.starts_with("select ") && !has_from {
        let rest = norm["select ".len()..].trim().to_string();
        let (expr_raw, alias) = if let Some(pos) = rest.find(" as ") {
            (rest[..pos].trim().to_string(), rest[pos + 4..].trim().to_string())
        } else {
            (rest.trim().to_string(), "?column?".to_string())
        };
        // All TEXT — avoids binary-format decoding issues with asyncpg.
        return Some(vec![FieldInfo::new(alias, None, None, Type::TEXT, FieldFormat::Text)]);
    }

    None
}

/// Parse SQL to determine result columns for Describe responses.
/// Returns FieldInfo entries for SELECT / RETURNING queries, empty for DML without RETURNING.
fn describe_sql_columns(sql: &str, db: &Database) -> Vec<FieldInfo> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let trimmed = sql.trim();
    // Strip $N placeholders — we only need column structure, not values.
    // Replace $N with NULL so the parser can handle it.
    let sanitized = {
        let mut s = String::with_capacity(trimmed.len());
        let mut chars = trimmed.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '$' && chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                // Skip the digits
                while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                    chars.next();
                }
                s.push_str("NULL");
            } else {
                s.push(ch);
            }
        }
        s
    };

    // Strip ::type casts so the parser and engine accept SQLAlchemy/asyncpg SQL.
    let sanitized = preprocess_sql(&sanitized);

    // Check intercept layer first — before the SQL parser, so system catalog
    // queries that sqlparser can't parse still get the right column schema.
    if let Some(fields) = field_info_for_intercept(&sanitized, db) {
        return fields;
    }

    let stmts = match Parser::parse_sql(&GenericDialect {}, &sanitized) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    if stmts.is_empty() {
        return vec![];
    }

    match &stmts[0] {
        sqlparser::ast::Statement::Query(_) => {
            // For SELECT: try to execute against the engine to get real metadata.
            match db.execute_sql(&sanitized) {
                Ok(SqlResult::Query { columns, column_types, .. }) => {
                    columns
                        .iter()
                        .enumerate()
                        .map(|(i, name)| {
                            // Strip table prefix to match PostgreSQL RowDescription convention.
                            let display_name = if let Some(pos) = name.rfind('.') {
                                name[pos+1..].to_string()
                            } else {
                                name.clone()
                            };
                            let pg_type = column_types.get(i).map(value_type_to_pg_type).unwrap_or(Type::TEXT);
                            FieldInfo::new(display_name, None, None, pg_type, FieldFormat::Text)
                        })
                        .collect()
                }
                _ => vec![],
            }
        }
        sqlparser::ast::Statement::Insert(insert) => {
            describe_returning_columns(&insert.returning, &insert.table_name.to_string(), db)
        }
        sqlparser::ast::Statement::Update { returning, table, .. } => {
            describe_returning_columns(returning, &table.relation.to_string(), db)
        }
        sqlparser::ast::Statement::Delete(delete) => {
            let table_name = if !delete.tables.is_empty() {
                delete.tables[0].to_string()
            } else {
                match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        tables.first().map(|f| f.relation.to_string()).unwrap_or_default()
                    }
                }
            };
            describe_returning_columns(&delete.returning, &table_name, db)
        }
        _ => vec![],
    }
}

/// Build FieldInfo for a RETURNING clause given the table name.
fn describe_returning_columns(
    returning: &Option<Vec<sqlparser::ast::SelectItem>>,
    table_name: &str,
    db: &Database,
) -> Vec<FieldInfo> {
    let items = match returning {
        Some(items) if !items.is_empty() => items,
        _ => return vec![],
    };
    let schema = match db.get_schema(table_name) {
        Ok(Some(s)) => s,
        _ => return vec![],
    };
    let mut fields = Vec::new();
    for item in items {
        match item {
            sqlparser::ast::SelectItem::Wildcard(_) => {
                for col in &schema.columns {
                    let pg_type = value_type_to_pg_type(&col.col_type);
                    fields.push(FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text));
                }
                return fields;
            }
            sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(ident)) => {
                let col_type = schema.columns.iter()
                    .find(|c| c.name == ident.value)
                    .map(|c| value_type_to_pg_type(&c.col_type))
                    .unwrap_or(Type::TEXT);
                fields.push(FieldInfo::new(ident.value.clone(), None, None, col_type, FieldFormat::Text));
            }
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                fields.push(FieldInfo::new(alias.value.clone(), None, None, Type::TEXT, FieldFormat::Text));
            }
            _ => {}
        }
    }
    fields
}

// ---------------------------------------------------------------------------
// Extended query protocol (parameterized queries)
// ---------------------------------------------------------------------------

/// Map a SQL type name to a pgwire Type.
fn sql_type_name_to_pg_type(name: &str) -> Type {
    match name.trim().to_lowercase().as_str() {
        "varchar" | "character varying" | "text" | "char" | "bpchar" => Type::TEXT,
        "int" | "integer" | "int4" => Type::INT4,
        "bigint" | "int8" => Type::INT8,
        "smallint" | "int2" => Type::INT2,
        "float" | "float4" | "real" => Type::FLOAT4,
        "double precision" | "float8" => Type::FLOAT8,
        "bool" | "boolean" => Type::BOOL,
        "timestamp with time zone" | "timestamptz" => Type::TIMESTAMPTZ,
        "timestamp" | "timestamp without time zone" => Type::TIMESTAMP,
        "date" => Type::DATE,
        "uuid" => Type::UUID,
        "json" | "jsonb" => Type::TEXT,
        "bytea" => Type::BYTEA,
        "numeric" | "decimal" => Type::NUMERIC,
        _ => Type::TEXT,
    }
}

/// Scan SQL for `$N::TYPENAME` patterns and return a Vec<Type> indexed by parameter position.
/// Returns a Vec of length = max($N) with TEXT as default for untyped params.
fn infer_param_types(sql: &str) -> Vec<Type> {
    let mut types: std::collections::HashMap<usize, Type> = std::collections::HashMap::new();
    let lower = sql.to_lowercase();
    let bytes = lower.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                if bytes[i] == b'\\' { i += 1; }
                i += 1;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'$' {
            i += 1;
            let num_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > num_start {
                if let Ok(n) = lower[num_start..i].parse::<usize>() {
                    // Check for ::TYPENAME after the parameter number
                    let mut j = i;
                    while j < bytes.len() && bytes[j] == b' ' { j += 1; }
                    if j + 1 < bytes.len() && bytes[j] == b':' && bytes[j + 1] == b':' {
                        j += 2;
                        while j < bytes.len() && bytes[j] == b' ' { j += 1; }
                        let type_start = j;
                        // Read until a non-type character
                        while j < bytes.len() && !matches!(bytes[j], b',' | b')' | b' ' | b'\n' | b'\r' | b'\t' | b'[' | b'=' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/' | b'|' | b'&') {
                            j += 1;
                        }
                        // Also check for "double precision", "character varying", "timestamp with time zone"
                        let mut type_name = lower[type_start..j].to_string();
                        // Handle multi-word types by consuming extra words
                        if type_name == "timestamp" || type_name == "character" || type_name == "double" {
                            while j < bytes.len() && bytes[j] == b' ' { j += 1; }
                            let word2_start = j;
                            while j < bytes.len() && bytes[j].is_ascii_alphabetic() { j += 1; }
                            if j > word2_start {
                                let w2 = &lower[word2_start..j];
                                type_name = format!("{} {}", type_name, w2);
                                // For "timestamp with time zone"
                                if type_name == "timestamp with" {
                                    while j < bytes.len() && bytes[j] == b' ' { j += 1; }
                                    let w3s = j;
                                    while j < bytes.len() && bytes[j].is_ascii_alphabetic() { j += 1; }
                                    let w4s = j;
                                    while j < bytes.len() && bytes[j] == b' ' { j += 1; }
                                    let w4e = j;
                                    while j < bytes.len() && bytes[j].is_ascii_alphabetic() { j += 1; }
                                    let w3 = &lower[w3s..w4s.min(lower.len())];
                                    let w4 = if w4e < lower.len() { &lower[w4e..j] } else { "" };
                                    if !w3.is_empty() {
                                        type_name = format!("{} {} {}", type_name, w3, w4).trim().to_string();
                                    }
                                }
                            }
                        }
                        types.insert(n, sql_type_name_to_pg_type(&type_name));
                    } else {
                        types.entry(n).or_insert(Type::TEXT);
                    }
                }
            }
            continue;
        }
        i += 1;
    }
    if types.is_empty() {
        return vec![];
    }
    let max_n = *types.keys().max().unwrap_or(&0);
    (1..=max_n)
        .map(|n| types.get(&n).cloned().unwrap_or(Type::TEXT))
        .collect()
}

/// Query parser that stores SQL as a plain string.
struct DbQueryParser;

#[async_trait]
impl QueryParser for DbQueryParser {
    type Statement = String;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<String> {
        Ok(sql.to_owned())
    }
}

/// Extended query handler that substitutes $1, $2, ... parameters into SQL
/// and delegates to the same engine execution path as simple queries.
struct DbExtendedQueryHandler {
    db: Database,
    conn_id: u64,
    state: Arc<Mutex<ConnState>>,
    parser: Arc<DbQueryParser>,
}

impl DbExtendedQueryHandler {
    fn new(db: Database, conn_id: u64, state: Arc<Mutex<ConnState>>) -> Self {
        Self {
            db,
            conn_id,
            state,
            parser: Arc::new(DbQueryParser),
        }
    }

    fn execute_one<'a>(&self, sql: &str) -> Result<Response<'a>, Box<dyn std::error::Error>> {
        let preprocessed = preprocess_sql(sql);
        if let Some(intercepted) = intercept_system_query(&preprocessed, &self.db) {
            return Ok(intercepted);
        }
        let result = self.db.execute_sql(&preprocessed)?;
        execute_sql_result(result, &self.state, &self.db)
    }
}

/// PG epoch is 2000-01-01; Unix epoch is 1970-01-01.
/// Difference in microseconds.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
/// Difference in days.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// Decode a single binary parameter based on its PG type OID and format it as a SQL literal.
fn decode_param_to_sql(bytes: &[u8], pg_type: &Type, format: &FieldFormat) -> String {
    // If the client sent the parameter in text format, use the old text-based logic.
    if matches!(format, FieldFormat::Text) {
        let text = String::from_utf8_lossy(bytes);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return "''".to_string();
        } else if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
            return trimmed.to_uppercase();
        } else if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
            return trimmed.to_string();
        } else {
            return format!("'{}'", trimmed.replace('\'', "''"));
        }
    }

    // Binary format decoding based on type OID.
    match *pg_type {
        Type::BOOL => {
            if bytes.first() == Some(&1) { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        Type::INT2 => {
            if bytes.len() >= 2 {
                let v = i16::from_be_bytes(bytes[..2].try_into().unwrap());
                v.to_string()
            } else { "0".to_string() }
        }
        Type::INT4 => {
            if bytes.len() >= 4 {
                let v = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                v.to_string()
            } else { "0".to_string() }
        }
        Type::INT8 => {
            if bytes.len() >= 8 {
                let v = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                v.to_string()
            } else { "0".to_string() }
        }
        Type::FLOAT4 => {
            if bytes.len() >= 4 {
                let v = f32::from_be_bytes(bytes[..4].try_into().unwrap());
                v.to_string()
            } else { "0".to_string() }
        }
        Type::FLOAT8 => {
            if bytes.len() >= 8 {
                let v = f64::from_be_bytes(bytes[..8].try_into().unwrap());
                v.to_string()
            } else { "0".to_string() }
        }
        Type::UUID => {
            if bytes.len() >= 16 {
                let u = &bytes[..16];
                format!(
                    "'{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}'",
                    u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7],
                    u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]
                )
            } else {
                "NULL".to_string()
            }
        }
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            if bytes.len() >= 8 {
                let pg_us = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                let unix_us = pg_us + PG_EPOCH_OFFSET_US;
                let ts_str = rust_dst_db::query::expr::format_timestamp(unix_us);
                format!("TIMESTAMP '{}'", ts_str)
            } else { "NULL".to_string() }
        }
        Type::DATE => {
            if bytes.len() >= 4 {
                let pg_days = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                let unix_days = pg_days + PG_EPOCH_OFFSET_DAYS;
                let date_str = rust_dst_db::query::expr::format_date(unix_days);
                format!("DATE '{}'", date_str)
            } else { "NULL".to_string() }
        }
        Type::BYTEA => {
            // Encode as hex escape for BYTEA
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            format!("'\\x{}'", hex)
        }
        Type::NUMERIC => {
            // PG numeric binary format is complex; fall back to text interpretation.
            let text = String::from_utf8_lossy(bytes);
            let trimmed = text.trim();
            if trimmed.parse::<f64>().is_ok() {
                trimmed.to_string()
            } else {
                format!("'{}'", trimmed.replace('\'', "''"))
            }
        }
        // TEXT, VARCHAR, and anything else: treat as UTF-8 text.
        _ => {
            let text = String::from_utf8_lossy(bytes);
            let trimmed = text.trim();
            if trimmed.is_empty() {
                "''".to_string()
            } else {
                format!("'{}'", trimmed.replace('\'', "''"))
            }
        }
    }
}

/// Substitute $1, $2, ... placeholders with parameter values.
/// Uses type OIDs from the portal's statement to correctly decode binary parameters.
fn substitute_params(
    sql: &str,
    params: &[Option<bytes::Bytes>],
    param_types: &[Type],
    param_format: &pgwire::api::portal::Format,
) -> String {
    let mut result = sql.to_string();
    // Replace from highest index to lowest to avoid offset issues.
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match param {
            Some(bytes) => {
                let pg_type = param_types.get(i).unwrap_or(&Type::TEXT);
                let format = param_format.format_for(i);
                decode_param_to_sql(bytes, pg_type, &format)
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

#[async_trait]
impl ExtendedQueryHandler for DbExtendedQueryHandler {
    type Statement = String;
    type QueryParser = DbQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement;
        let resolved_sql = substitute_params(
            sql,
            &portal.parameters,
            &portal.statement.parameter_types,
            &portal.parameter_format,
        );


        info!(conn_id = self.conn_id, sql = %resolved_sql, "extended query");

        match self.execute_one(&resolved_sql) {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!(conn_id = self.conn_id, error = %e, "extended query error");
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42000".to_string(),
                    e.to_string(),
                ))))
            }
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = describe_sql_columns(&target.statement, &self.db);
        // If asyncpg sent no type OIDs in Parse, infer types from $N::TYPE casts.
        let params = if target.parameter_types.is_empty() {
            infer_param_types(&target.statement)
        } else {
            target.parameter_types.clone()
        };
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = describe_sql_columns(&target.statement.statement, &self.db);
        Ok(DescribePortalResponse::new(fields))
    }
}

// ---------------------------------------------------------------------------
// Handler factory
// ---------------------------------------------------------------------------

struct DbHandlerFactory {
    db: Database,
    next_conn_id: Arc<AtomicU64>,
    auth: Option<Arc<Mutex<AuthManager>>>,
}

impl PgWireHandlerFactory for DbHandlerFactory {
    type StartupHandler = DbStartupHandler;
    type SimpleQueryHandler = DbQueryHandler;
    type ExtendedQueryHandler = DbExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(DbStartupHandler {
            auth: self.auth.clone(),
            params: Arc::new(DefaultServerParameterProvider::default()),
        })
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        Arc::new(DbQueryHandler::new(self.db.clone(), conn_id))
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(Mutex::new(ConnState { active_txn: None }));
        Arc::new(DbExtendedQueryHandler::new(self.db.clone(), conn_id, state))
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    info!(data_dir = %args.data_dir, port = args.port, "starting Rust-DB server");

    // Build auth manager if credentials were provided.
    let auth = match (&args.user, &args.password) {
        (Some(user), Some(password)) => {
            let mut mgr = AuthManager::new();
            mgr.add_user(user, password);
            info!(user = %user, "authentication enabled");
            Some(Arc::new(Mutex::new(mgr)))
        }
        (Some(_), None) => {
            error!("--user requires --password");
            std::process::exit(1);
        }
        (None, Some(_)) => {
            error!("--password requires --user");
            std::process::exit(1);
        }
        (None, None) => {
            warn!("authentication disabled — any client can connect");
            None
        }
    };

    // Open database.
    let db = match Database::open(&args.data_dir) {
        Ok(db) => db,
        Err(e) => {
            error!(error = %e, "failed to open database");
            std::process::exit(1);
        }
    };

    info!(tables = db.table_count().unwrap_or(0), "database opened, WAL recovery complete");

    // Start web UI server.
    let web_state = web::AppState {
        db: db.clone(),
        auth: auth.clone(),
        sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
    };
    let web_addr = format!("{}:{}", args.bind, args.web_port);
    let web_router = web::router(web_state);
    let web_listener = tokio::net::TcpListener::bind(&web_addr)
        .await
        .expect("failed to bind web port");
    info!(addr = %web_addr, "web UI available");

    tokio::spawn(async move {
        axum::serve(web_listener, web_router).await.ok();
    });

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");

    info!(addr = %addr, "listening for PostgreSQL connections");
    info!("connect with: psql -h 127.0.0.1 -p {}", args.port);

    let next_conn_id = Arc::new(AtomicU64::new(1));

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                info!(peer = %peer_addr, "new connection");

                let factory = Arc::new(DbHandlerFactory {
                    db: db.clone(),
                    next_conn_id: Arc::clone(&next_conn_id),
                    auth: auth.clone(),
                });

                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, factory).await {
                        warn!(peer = %peer_addr, error = %e, "connection error");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "accept error");
            }
        }
    }
}
