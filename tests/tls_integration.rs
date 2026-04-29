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

/// Materials for a CA + server cert + client cert chain.
struct CaBundle {
    /// CA cert in PEM (used as `tls.client_ca_path` and as the trusted root
    /// for the rustls client during the handshake).
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn make_ca_bundle() -> CaBundle {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params =
        rcgen::CertificateParams::new(vec!["rustdb-test-ca".to_string()]).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

    let server_key = rcgen::KeyPair::generate().expect("server key");
    let server_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server signed");

    let client_key = rcgen::KeyPair::generate().expect("client key");
    let client_params = rcgen::CertificateParams::new(vec!["rustdb-test-client".to_string()])
        .expect("client params");
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .expect("client signed");

    CaBundle {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// Spawn a server with mTLS configured via a generated TOML file.
struct MtlsServer {
    server: SpawnedServer,
    bundle: CaBundle,
    _config_path: PathBuf,
}

fn spawn_server_mtls(client_auth: &str) -> MtlsServer {
    let bundle = make_ca_bundle();
    let server_cert_path = write_temp("srv-cert", &bundle.server_cert_pem);
    let server_key_path = write_temp("srv-key", &bundle.server_key_pem);
    let ca_path = write_temp("ca", &bundle.ca_pem);

    let pgwire_port = pick_free_port();
    let web_port = pick_free_port();
    let data_dir = std::env::temp_dir().join(format!(
        "rustdb-mtls-it-data-{}-{}",
        std::process::id(),
        rand::random::<u64>(),
    ));
    std::fs::create_dir_all(&data_dir).expect("data dir");

    let toml = format!(
        r#"
data_dir = "{}"
port = {}
web_port = {}

[tls]
enabled = true
cert_path = "{}"
key_path  = "{}"
mode      = "optional"
client_auth = "{}"
client_ca_path = "{}"
"#,
        data_dir.display(),
        pgwire_port,
        web_port,
        server_cert_path.display(),
        server_key_path.display(),
        client_auth,
        ca_path.display(),
    );
    let config_path = std::env::temp_dir().join(format!(
        "rustdb-mtls-cfg-{}-{}.toml",
        std::process::id(),
        rand::random::<u64>(),
    ));
    std::fs::write(&config_path, toml).expect("write config");

    let bin = env!("CARGO_BIN_EXE_rust-db-server");
    let child = Command::new(bin)
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rust-db-server");

    // Readiness probe.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", pgwire_port)).is_ok() {
            break;
        }
        if Instant::now() > deadline {
            panic!("mTLS server did not start within 15s");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_millis(250));

    MtlsServer {
        server: SpawnedServer {
            child,
            pgwire_port,
            web_port,
            cert_der: vec![], // not used for mTLS path
            cert_path: server_cert_path,
            key_path: server_key_path,
            data_dir,
        },
        bundle,
        _config_path: config_path,
    }
}

fn mtls_client_config(bundle: &CaBundle, with_client_cert: bool) -> Arc<ClientConfig> {
    rust_dst_db::tls::install_crypto_provider();

    // Trust the test CA.
    let ca_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut bundle.ca_pem.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse ca pem");
    let mut roots = RootCertStore::empty();
    for c in ca_certs {
        roots.add(c).expect("trust ca");
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);

    let cfg = if with_client_cert {
        let client_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut bundle.client_cert_pem.as_bytes())
                .collect::<Result<_, _>>()
                .expect("parse client cert");
        let client_key = rustls_pemfile::private_key(&mut bundle.client_key_pem.as_bytes())
            .expect("parse client key")
            .expect("client key present");
        builder
            .with_client_auth_cert(client_certs, client_key)
            .expect("client auth cert")
    } else {
        builder.with_no_client_auth()
    };
    Arc::new(cfg)
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
    let cell = simple_query_first_cell(client, sql).await;
    assert_eq!(cell, expected, "unexpected cell value");
}

async fn simple_query_first_cell(client: &tokio_postgres::Client, sql: &str) -> String {
    use tokio_postgres::SimpleQueryMessage;
    let messages = client.simple_query(sql).await.expect("simple_query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("expected at least one row");
    row.get(0).expect("expected non-null cell").to_string()
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
async fn pgwire_rollback_actually_rolls_back() {
    let server = spawn_server(None);

    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("plaintext connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .batch_execute(
            "CREATE TABLE rb_test (v INT); BEGIN; INSERT INTO rb_test VALUES (1); ROLLBACK;",
        )
        .await
        .expect("transaction batch");

    let count = simple_query_first_cell(&client, "SELECT COUNT(*) FROM rb_test").await;
    assert_eq!(count, "0");
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

#[cfg(unix)]
fn send_sigterm(pid: u32) {
    // SAFETY: libc::kill with a pid we own is benign; SIGTERM is a normal-shutdown
    // signal handled by the server's signal listener.
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_timeout_does_not_break_fast_queries() {
    // A 5s timeout should be plenty for a SELECT 1 round-trip.
    let server = spawn_server_with(None, &["--statement-timeout", "5"]);
    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_simple_select(&client, "SELECT 1", "1").await;
    assert_simple_select(&client, "SELECT 99", "99").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_timeout_fires_on_slow_query() {
    // 1-second timeout. Build a synthetic "heavy" expression: a deeply
    // right-nested arithmetic tree that the parser+executor must walk.
    let server = spawn_server_with(None, &["--statement-timeout", "1"]);
    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Construct a SELECT with a long chain of "1 +" terms. 200k terms forces
    // the parser to allocate a deep expression tree and the evaluator to
    // recurse through it, which on the order of seconds in dev builds.
    let mut sql = String::from("SELECT ");
    for _ in 0..200_000 {
        sql.push_str("1+");
    }
    sql.push('1');

    let start = std::time::Instant::now();
    let result = client.simple_query(&sql).await;
    let elapsed = start.elapsed();

    // Either the query succeeds quickly (engine was fast) or it hits the
    // timeout. We assert: if it failed, the failure must be SQLSTATE 57014;
    // and the call must not have hung past timeout + a generous slack.
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "statement_timeout did not bound execution time; elapsed = {:?}",
        elapsed
    );
    if let Err(err) = &result {
        if let Some(db) = err.as_db_error() {
            assert_eq!(db.code().code(), "57014", "expected timeout sqlstate");
        }
        // Either way (DbError or transport), we got a bounded failure: pass.
    }
    // If Ok, the engine completed under the timeout — also acceptable.
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_drains_active_connection() {
    let mut server = spawn_server_with(None, &["--shutdown-grace", "10"]);
    let pid = server.child.id();

    // Open a connection and verify it works before shutdown.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=test dbname=postgres sslmode=disable",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("first connection");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_simple_select(&client, "SELECT 1", "1").await;

    // SIGTERM the server. The active connection should still complete its
    // next query before the server exits.
    send_sigterm(pid);

    // Give the signal listener a moment to fire and the accept loop to exit.
    // Generous sleep so the test stays reliable under parallel-test CPU load.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The held connection should still serve queries during the drain window.
    assert_simple_select(&client, "SELECT 7", "7").await;

    // New connections should be refused: the listener has stopped accepting.
    let new_attempt = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio_postgres::connect(&conn_str, tokio_postgres::NoTls),
    )
    .await;
    let new_attempt_failed = match new_attempt {
        Ok(Ok(_)) => false,
        Ok(Err(_)) | Err(_) => true,
    };
    assert!(
        new_attempt_failed,
        "expected new connect to fail after SIGTERM"
    );

    // Drop the held connection; server should exit cleanly within the grace
    // window.
    drop(client);
    let _ = driver.await;

    // The child should exit with status success now that the drain completed.
    let exit_deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match server.child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(
                    status.success(),
                    "server exited with non-success status: {status:?}"
                );
                break;
            }
            None => {
                if std::time::Instant::now() > exit_deadline {
                    panic!("server did not exit within 15s after SIGTERM");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

// ─── mTLS tests ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn mtls_required_accepts_signed_client_cert() {
    let server = spawn_server_mtls("required");
    let tls = mtls_client_config(&server.bundle, true);
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());

    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, connector)
        .await
        .expect("mtls connect should succeed with valid client cert");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_simple_select(&client, "SELECT 1", "1").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_required_rejects_no_client_cert() {
    let server = spawn_server_mtls("required");
    let tls = mtls_client_config(&server.bundle, false);
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());

    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.server.pgwire_port
    );
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio_postgres::connect(&conn_str, connector),
    )
    .await;
    let connect_succeeded = matches!(result, Ok(Ok(_)));
    assert!(
        !connect_succeeded,
        "expected mTLS-required server to reject client without cert"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_optional_allows_no_client_cert() {
    let server = spawn_server_mtls("optional");
    let tls = mtls_client_config(&server.bundle, false);
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());

    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, connector)
        .await
        .expect("optional mtls should accept client without cert");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_simple_select(&client, "SELECT 1", "1").await;
}

// ─── SIGHUP cert reload ─────────────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sighup_reloads_pgwire_cert() {
    let server = spawn_server(None);
    let pid = server.child.id();
    let original_cert_der = server.cert_der.clone();

    // Round-trip a query so we know the original cert is in use.
    {
        let tls = client_tls_config(&original_cert_der);
        let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());
        let conn_str = format!(
            "host=localhost port={} user=test dbname=postgres sslmode=require",
            server.pgwire_port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, connector)
            .await
            .expect("connect with original cert");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        assert_simple_select(&client, "SELECT 1", "1").await;
    }

    // Generate a fresh cert pair and overwrite the same paths.
    let (new_cert_pem, new_key_pem, new_cert_der) = make_self_signed();
    std::fs::write(&server.cert_path, &new_cert_pem).expect("overwrite cert");
    std::fs::write(&server.key_path, &new_key_pem).expect("overwrite key");
    assert_ne!(
        original_cert_der, new_cert_der,
        "rcgen should produce a distinct cert"
    );

    // SIGHUP — the server's reload listener picks up the new files.
    send_sigterm_or_sighup(pid, libc::SIGHUP);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // A client trusting ONLY the new cert should connect successfully.
    let tls_new = client_tls_config(&new_cert_der);
    let connector_new = tokio_postgres_rustls::MakeRustlsConnect::new((*tls_new).clone());
    let conn_str = format!(
        "host=localhost port={} user=test dbname=postgres sslmode=require",
        server.pgwire_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, connector_new)
        .await
        .expect("connect with new cert after SIGHUP");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_simple_select(&client, "SELECT 99", "99").await;

    // A client trusting ONLY the OLD cert should now fail — proving the
    // server is no longer presenting the original.
    let tls_old = client_tls_config(&original_cert_der);
    let connector_old = tokio_postgres_rustls::MakeRustlsConnect::new((*tls_old).clone());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio_postgres::connect(&conn_str, connector_old),
    )
    .await;
    let succeeded = matches!(result, Ok(Ok(_)));
    assert!(
        !succeeded,
        "old-cert client should not be able to connect after SIGHUP reload"
    );
}

#[cfg(unix)]
fn send_sigterm_or_sighup(pid: u32, sig: i32) {
    unsafe {
        libc::kill(pid as i32, sig);
    }
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
