/// pgwire adapter — async bridge between real PostgreSQL clients and the
/// synchronous message bus.
///
/// Enabled only when the `pgwire` feature flag is set. This keeps the core
/// library free of tokio/async dependencies.
///
/// # Architecture
///
/// ```text
/// PostgreSQL client
///     │  (TCP, pgwire protocol)
///     ▼
/// tokio Runtime (separate thread)
///   BusHandlerFactory  (implements PgWireHandlerFactory)
///     │  parses SQL → sql_to_plan()
///     │  (std::sync::mpsc channel pair)
///     ▼
/// MessageBus thread  —  poll_external() drains channel each tick
///     │  executes against in-memory rows (Stage 5 stub)
///     ▼
///   results via reply channel
///     ▼
/// BusQueryHandler → formats DataRow + CommandComplete
///     ▼
/// PostgreSQL client
/// ```

use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::{stream, Sink};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use crate::config::NET_DEFAULT_PORT;
use crate::query::executor::execute;
use crate::query::expr::{Row, Schema, Value};
use crate::query::sql::sql_to_plan;

// ─── Channel types ────────────────────────────────────────────────────────────

/// A query request forwarded from a pgwire client to the bus thread.
pub struct ClientRequest {
    pub conn_id: u64,
    pub sql: String,
    pub reply_tx: mpsc::SyncSender<ClientResponse>,
}

/// The response sent back from the bus thread to the pgwire handler.
pub struct ClientResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub error: Option<String>,
}

// ─── Bus-side helper ──────────────────────────────────────────────────────────

/// Drain pending [`ClientRequest`]s, execute each synchronously against the
/// provided snapshot, and send results back via the reply channel.
///
/// Call this at the start of each bus tick to integrate external clients.
pub fn poll_external(
    rx: &mpsc::Receiver<ClientRequest>,
    schema: &Schema,
    stored_rows: &[Row],
) {
    while let Ok(req) = rx.try_recv() {
        let response = handle_request(&req.sql, schema, stored_rows);
        let _ = req.reply_tx.send(response);
    }
}

fn handle_request(sql: &str, schema: &Schema, rows: &[Row]) -> ClientResponse {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Transaction control statements are accepted as no-ops for the Stage 5 stub.
    if upper == "BEGIN" || upper == "COMMIT" || upper == "ROLLBACK" {
        return ClientResponse { columns: vec![], rows: vec![], error: None };
    }

    match sql_to_plan(trimmed, schema.clone()) {
        Ok(plan) => {
            let result = execute(&plan, rows.to_vec());
            let columns: Vec<String> = if result.is_empty() {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                result[0].keys().cloned().collect()
            };
            ClientResponse { columns, rows: result, error: None }
        }
        Err(e) => ClientResponse { columns: vec![], rows: vec![], error: Some(e.to_string()) },
    }
}

// ─── pgwire query handler ─────────────────────────────────────────────────────

/// pgwire query handler that routes SQL through the channel to the bus thread.
pub struct BusQueryHandler {
    tx: mpsc::SyncSender<ClientRequest>,
    conn_id: u64,
}

impl BusQueryHandler {
    pub fn new(tx: mpsc::SyncSender<ClientRequest>, conn_id: u64) -> Self {
        Self { tx, conn_id }
    }
}

impl NoopStartupHandler for BusQueryHandler {}

#[async_trait]
impl SimpleQueryHandler for BusQueryHandler {
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
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);

        self.tx
            .send(ClientRequest {
                conn_id: self.conn_id,
                sql: query.to_string(),
                reply_tx,
            })
            .map_err(|e| {
                PgWireError::IoError(std::io::Error::other(e.to_string()))
            })?;

        let resp = reply_rx.recv().map_err(|e| {
            PgWireError::IoError(std::io::Error::other(e.to_string()))
        })?;

        if let Some(err) = resp.error {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42000".to_string(),
                    err,
                ),
            )));
        }

        if resp.columns.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let fields: Vec<FieldInfo> = resp
            .columns
            .iter()
            .map(|name| {
                FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text)
            })
            .collect();
        let schema = Arc::new(fields);

        let rows_data: Vec<_> = resp
            .rows
            .iter()
            .map(|row| {
                let mut encoder = DataRowEncoder::new(schema.clone());
                for col in &resp.columns {
                    let text = value_to_text(row.get(col).unwrap_or(&Value::Null));
                    encoder.encode_field(&text)?;
                }
                encoder.finish()
            })
            .collect();

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(rows_data),
        ))])
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

// ─── Handler factory ──────────────────────────────────────────────────────────

struct BusHandlerFactory {
    handler: Arc<BusQueryHandler>,
}

impl PgWireHandlerFactory for BusHandlerFactory {
    type StartupHandler = BusQueryHandler;
    type SimpleQueryHandler = BusQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

// ─── Server entry point ───────────────────────────────────────────────────────

/// Start the pgwire server on `port` (defaults to [`NET_DEFAULT_PORT`]).
///
/// Returns the request channel sender so the bus thread can drain requests
/// via [`poll_external`].
///
/// # Panics
/// Panics if the tokio runtime or TCP listener cannot be created.
pub fn start_server(port: Option<u16>) -> mpsc::SyncSender<ClientRequest> {
    let port = port.unwrap_or(NET_DEFAULT_PORT);
    let (tx, rx) = mpsc::sync_channel::<ClientRequest>(256);
    let server_tx = tx.clone();

    // The bus thread holds `rx` and calls poll_external each tick.
    // The server thread holds `server_tx` to inject requests from TCP clients.
    drop(rx); // caller must pass their own receiver; this is the shared sender

    std::thread::spawn(move || {
        let rt = Runtime::new().expect("tokio runtime");
        rt.block_on(async move {
            let addr = format!("0.0.0.0:{}", port);
            let listener = TcpListener::bind(&addr).await.expect("bind");

            // Each connection gets its own handler sharing the same channel.
            let next_conn_id = Arc::new(AtomicU64::new(1));

            loop {
                if let Ok((socket, _addr)) = listener.accept().await {
                    let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
                    let factory = Arc::new(BusHandlerFactory {
                        handler: Arc::new(BusQueryHandler::new(server_tx.clone(), conn_id)),
                    });
                    tokio::spawn(async move {
                        process_socket(socket, None, factory).await
                    });
                }
            }
        });
    });

    tx
}
