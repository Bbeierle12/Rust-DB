//! rust-db-server — PostgreSQL-compatible database server.
//!
//! Accepts connections via the PostgreSQL wire protocol, executes SQL against
//! the real `Database` engine with MVCC transactions, WAL durability, and
//! file-backed persistence.
//!
//! Usage:
//!   cargo run --features server --bin rust-db-server -- --data-dir ./mydb --port 5433
//!
//! Connect with:
//!   psql -h 127.0.0.1 -p 5433

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use futures::{stream, Sink};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use rust_dst_db::engine::{Database, SqlResult};
use rust_dst_db::query::expr::Value;

use tracing::{info, error, warn};

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
}

/// Per-connection state tracking active transactions.
struct ConnState {
    active_txn: Option<u64>,
}

struct DbQueryHandler {
    db: Database,
    conn_id: u64,
    /// Per-connection state (tracks active transaction).
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

impl NoopStartupHandler for DbQueryHandler {}

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

        // Split on semicolons to handle multi-statement queries.
        let statements: Vec<&str> = query.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for stmt in statements {
            match self.execute_one(stmt) {
                Ok(resp) => responses.push(resp),
                Err(e) => {
                    error!(conn_id = self.conn_id, error = %e, "query error");
                    return Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            e.to_string(),
                        ),
                    )));
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
    }
}

struct DbHandlerFactory {
    db: Database,
    next_conn_id: Arc<AtomicU64>,
}

impl PgWireHandlerFactory for DbHandlerFactory {
    type StartupHandler = DbQueryHandler;
    type SimpleQueryHandler = DbQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        Arc::new(DbQueryHandler::new(self.db.clone(), conn_id))
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        Arc::new(DbQueryHandler::new(self.db.clone(), conn_id))
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let args = Args::parse();

    info!(data_dir = %args.data_dir, port = args.port, "starting Rust-DB server");

    // Open database.
    let db = match Database::open(&args.data_dir) {
        Ok(db) => db,
        Err(e) => {
            error!(error = %e, "failed to open database");
            std::process::exit(1);
        }
    };

    info!(tables = db.table_count(), "database opened, WAL recovery complete");

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
