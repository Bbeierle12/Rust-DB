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
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

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
    auth: Option<Arc<AuthManager>>,
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

                if auth.authenticate(username, &pwd.password) {
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
        let result = self.db.execute_sql(sql)?;
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
            let fields: Vec<FieldInfo> = columns
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
                    for col in &columns {
                        let val = row.get(col).unwrap_or(&Value::Null);
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
                            let pg_type = column_types.get(i).map(value_type_to_pg_type).unwrap_or(Type::TEXT);
                            FieldInfo::new(name.clone(), None, None, pg_type, FieldFormat::Text)
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
        let result = self.db.execute_sql(sql)?;
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
        Ok(DescribeStatementResponse::new(target.parameter_types.clone(), fields))
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
    auth: Option<Arc<AuthManager>>,
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
            Some(Arc::new(mgr))
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
