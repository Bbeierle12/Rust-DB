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

        match result {
            SqlResult::Begin(txn_id) => {
                let mut state = self.state.lock().unwrap();
                state.active_txn = Some(txn_id);
                Ok(Response::Execution(Tag::new("BEGIN")))
            }
            SqlResult::Commit => {
                let mut state = self.state.lock().unwrap();
                if let Some(txn_id) = state.active_txn.take() {
                    self.db.commit(txn_id)?;
                }
                Ok(Response::Execution(Tag::new("COMMIT")))
            }
            SqlResult::Rollback => {
                let mut state = self.state.lock().unwrap();
                if let Some(txn_id) = state.active_txn.take() {
                    self.db.abort(txn_id)?;
                }
                Ok(Response::Execution(Tag::new("ROLLBACK")))
            }
            SqlResult::Execute(count) => {
                Ok(Response::Execution(Tag::new(&format!("OK {}", count))))
            }
            SqlResult::Query { columns, rows } => {
                let fields: Vec<FieldInfo> = columns
                    .iter()
                    .map(|name| {
                        FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text)
                    })
                    .collect();
                let schema = Arc::new(fields);

                let rows_data: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        for col in &columns {
                            let text = value_to_text(row.get(col).unwrap_or(&Value::Null));
                            encoder.encode_field(&text)?;
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
}

fn value_to_text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
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

        match result {
            SqlResult::Begin(txn_id) => {
                let mut state = self.state.lock().unwrap();
                state.active_txn = Some(txn_id);
                Ok(Response::Execution(Tag::new("BEGIN")))
            }
            SqlResult::Commit => {
                let mut state = self.state.lock().unwrap();
                if let Some(txn_id) = state.active_txn.take() {
                    self.db.commit(txn_id)?;
                }
                Ok(Response::Execution(Tag::new("COMMIT")))
            }
            SqlResult::Rollback => {
                let mut state = self.state.lock().unwrap();
                if let Some(txn_id) = state.active_txn.take() {
                    self.db.abort(txn_id)?;
                }
                Ok(Response::Execution(Tag::new("ROLLBACK")))
            }
            SqlResult::Execute(count) => {
                Ok(Response::Execution(Tag::new(&format!("OK {}", count))))
            }
            SqlResult::Query { columns, rows } => {
                let fields: Vec<FieldInfo> = columns
                    .iter()
                    .map(|name| {
                        FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text)
                    })
                    .collect();
                let schema = Arc::new(fields);

                let rows_data: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        for col in &columns {
                            let text = value_to_text(row.get(col).unwrap_or(&Value::Null));
                            encoder.encode_field(&text)?;
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
}

/// Substitute $1, $2, ... placeholders with parameter values.
/// Parameters are text-format bytes from the pgwire Portal.
fn substitute_params(sql: &str, params: &[Option<bytes::Bytes>]) -> String {
    let mut result = sql.to_string();
    // Replace from highest index to lowest to avoid offset issues.
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match param {
            Some(bytes) => {
                let text = String::from_utf8_lossy(bytes);
                // Escape single quotes in the value and wrap in quotes.
                // Check if the value looks numeric to avoid quoting numbers.
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    "''".to_string()
                } else if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
                    trimmed.to_string()
                } else if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
                    trimmed.to_string()
                } else {
                    format!("'{}'", trimmed.replace('\'', "''"))
                }
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
        let resolved_sql = substitute_params(sql, &portal.parameters);

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
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Return empty description — we don't know column types until execution.
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(vec![]))
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
