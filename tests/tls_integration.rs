//! End-to-end TLS tests. Spawn the `rust-db-server` binary as a subprocess
//! with a self-signed cert generated at test time, then drive it with real
//! clients (tokio-postgres for pgwire, raw rustls + HTTP/1.1 for the web
//! admin port).
//!
//! Run with: `cargo test --features server --test tls_integration`

#![cfg(feature = "server")]

use std::io::Write as _;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

// ─── Fixture setup ──────────────────────────────────────────────────────────

fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().unwrap().port()
}

fn make_self_signed() -> (String, String, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen self-signed");
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let cert_der = cert.cert.der().to_vec();
    (cert_pem, key_pem, cert_der)
}

fn write_temp(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rustdb-tls-it-{}-{}-{}.pem",
        std::process::id(),
        rand::random::<u64>(),
        name,
    ));
    let mut f = std::fs::File::create(&path).expect("create pem");
    f.write_all(contents.as_bytes()).expect("write pem");
    path
}

struct SpawnedServer {
    child: Child,
    pgwire_port: u16,
    web_port: u16,
    cert_der: Vec<u8>,
    cert_path: PathBuf,
    key_path: PathBuf,
    data_dir: PathBuf,
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.cert_path);
        let _ = std::fs::remove_file(&self.key_path);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn spawn_server(tls_mode: Option<&str>) -> SpawnedServer {
    spawn_server_with(tls_mode, &[])
}

fn spawn_server_with(tls_mode: Option<&str>, extra_args: &[&str]) -> SpawnedServer {
    let (cert_pem, key_pem, cert_der) = make_self_signed();
    let cert_path = write_temp("cert", &cert_pem);
    let key_path = write_temp("key", &key_pem);

    let pgwire_port = pick_free_port();
    let web_port = pick_free_port();
    let data_dir = std::env::temp_dir().join(format!(
        "rustdb-tls-it-data-{}-{}",
        std::process::id(),
        rand::random::<u64>(),
    ));
    std::fs::create_dir_all(&data_dir).expect("data dir");

    let bin = env!("CARGO_BIN_EXE_rust-db-server");
    let mut cmd = Command::new(bin);
    cmd.arg("--data-dir")
        .arg(&data_dir)
        .arg("--port")
        .arg(pgwire_port.to_string())
        .arg("--web-port")
        .arg(web_port.to_string())
        .arg("--tls-cert")
        .arg(&cert_path)
        .arg("--tls-key")
        .arg(&key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(m) = tls_mode {
        cmd.arg("--tls-mode").arg(m);
    }
    for a in extra_args {
        cmd.arg(a);
    }
    let child = cmd.spawn().expect("spawn rust-db-server");

    // Wait for both ports to accept TCP connections.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let pg_ready = std::net::TcpStream::connect(("127.0.0.1", pgwire_port)).is_ok();
        let web_ready = std::net::TcpStream::connect(("127.0.0.1", web_port)).is_ok();
        if pg_ready && web_ready {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "server did not start within 15s (pg_ready={}, web_ready={})",
                pg_ready, web_ready
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // The readiness probe above opened a transient TCP connection that the
    // server counts as active until its accept-loop spawn task notices the
    // peer closed. Give it a beat to settle so tests like the conn-limit
    // assertion see a clean active_connections=0.
    std::thread::sleep(Duration::from_millis(250));

    SpawnedServer {
        child,
        pgwire_port,
        web_port,
        cert_der,
        cert_path,
        key_path,
        data_dir,
    }
}

fn client_tls_config(cert_der: &[u8]) -> Arc<ClientConfig> {
    rust_dst_db::tls::install_crypto_provider();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(cert_der.to_vec()))
        .expect("add self-signed root");
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

// ─── pgwire tests ───────────────────────────────────────────────────────────

/// Run a `SELECT <literal>` via simple-query (text format) and assert the
/// returned cell matches `expected`. Avoids type-OID brittleness in the binary
/// extended-query path.
async fn assert_simple_select(client: &tokio_postgres::Client, sql: &str, expected: &str) {
    use tokio_postgres::SimpleQueryMessage;
    let messages = client.simple_query(sql).await.expect("simple_query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("expected at least one row");
    let cell = row.get(0).expect("expected non-null cell");
    assert_eq!(cell, expected, "unexpected cell value");
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_tls_round_trip() {
    let server = spawn_server(Some("optional"));
    let tls = client_tls_config(&server.cert_der);
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());

    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, connector)
        .await
        .expect("tls connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    assert_simple_select(&client, "SELECT 1", "1").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_required_mode_rejects_plaintext() {
    let server = spawn_server(Some("required"));

    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let result = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await;

    let err = match result {
        Ok(_) => panic!("expected plaintext connection to be rejected"),
        Err(e) => e,
    };

    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err} (debug: {err:?})"));
    let message = db_err.message();
    let severity = db_err.severity();
    assert!(
        message.to_lowercase().contains("tls"),
        "expected TLS-related DbError message, got severity={severity:?} message={message:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_required_mode_accepts_tls() {
    let server = spawn_server(Some("required"));
    let tls = client_tls_config(&server.cert_der);
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());

    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, connector)
        .await
        .expect("tls connect under required mode");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    assert_simple_select(&client, "SELECT 42", "42").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conn_limit_returns_fatal_error_response() {
    let server = spawn_server_with(None, &["--max-connections", "1"]);

    // First connection holds the slot.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let (client1, conn1) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("first connection");
    let driver1 = tokio::spawn(async move {
        let _ = conn1.await;
    });
    // Sanity: the first connection works.
    let _ = client1
        .simple_query("SELECT 1")
        .await
        .expect("first conn query");

    // Second connection should be rejected with FATAL/53300 before any backend message.
    let result = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await;
    let err = match result {
        Ok(_) => panic!("expected second connection to be rejected"),
        Err(e) => e,
    };
    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err} (debug: {err:?})"));
    assert_eq!(db_err.code().code(), "53300", "wrong sqlstate");
    assert!(
        db_err.message().to_lowercase().contains("connection"),
        "expected connection-related message, got: {}",
        db_err.message()
    );

    drop(client1);
    let _ = driver1.await;
}

// ─── web admin tests ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn web_https_health_endpoint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsConnector;

    let server = spawn_server(None);
    let tls = client_tls_config(&server.cert_der);
    let connector = TlsConnector::from(tls);

    let stream = tokio::net::TcpStream::connect(("127.0.0.1", server.web_port))
        .await
        .expect("tcp connect web");
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector
        .connect(server_name, stream)
        .await
        .expect("tls handshake web");

    let req = b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    tls_stream.write_all(req).await.expect("write request");

    let mut response = Vec::new();
    tls_stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response_str = String::from_utf8_lossy(&response);

    assert!(
        response_str.starts_with("HTTP/1.1 200"),
        "expected 200 OK, got: {}",
        response_str.lines().next().unwrap_or("")
    );
    assert!(
        response_str.contains("\"status\":\"ok\""),
        "missing status field in body: {response_str}"
    );
}
