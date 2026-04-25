# Rust-DB

An embedded analytical database engine written from scratch in Rust. Speaks
the PostgreSQL wire protocol, supports MVCC snapshot isolation with OCC, and
runs on a deterministic-simulation testing (DST) substrate for the
underlying state machines.

This is **not** a SQLite or PostgreSQL wrapper — every component (storage,
WAL, MVCC, query planner, network adapter, web admin) is bespoke.

## Build & run

Default features compile only the embedded library:

```sh
cargo build
cargo test
```

To run as a network server (pgwire + web admin), enable the `server` feature:

```sh
cargo build --features server --bin rust-db-server
./target/debug/rust-db-server --data-dir ./data --port 5433 --web-port 8080
```

`--user <name> --password <pw>` enables argon2id-hashed cleartext-password
authentication on the pgwire port. Without those flags, authentication is
disabled and any client can connect.

A TOML config file is also accepted via `--config`. CLI flags override file
values.

## TLS

The server can terminate TLS on both the pgwire port and the web admin port.
Client-cert (mTLS) authentication is **not** implemented.

### Generate a development certificate

```sh
openssl req -x509 -newkey rsa:2048 -sha256 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=localhost'
```

### Enable TLS

Either pass the cert paths directly:

```sh
./target/debug/rust-db-server \
  --tls-cert ./cert.pem --tls-key ./key.pem
```

Or configure via TOML:

```toml
[tls]
enabled = true
cert_path = "./cert.pem"
key_path  = "./key.pem"
mode      = "optional"        # or "required"

# Optional per-listener override; defaults to the pair above.
[tls.web]
cert_path = "./web.crt"
key_path  = "./web.key"
```

Available CLI overrides: `--tls-cert`, `--tls-key`, `--tls-mode`, `--no-tls`.
`--no-tls` always wins, even if the config file enables TLS.

### Modes (pgwire only)

- **`optional`** (default when TLS is enabled): the server accepts both TLS
  and plaintext connections. Clients that send `SSLRequest` get a TLS
  handshake; clients that don't get a plaintext session.
- **`required`**: the server rejects plaintext connections with a
  PostgreSQL `FATAL ErrorResponse` ("server requires TLS; connect with
  sslmode=require") and closes the socket. TLS handshakes proceed normally.

The web admin port has no `optional` mode — when TLS is enabled, the
listener serves HTTPS only. Plaintext HTTP requests fail at the transport
layer.

### Connect with `psql`

```sh
psql "host=localhost port=5433 user=test dbname=postgres sslmode=require"
```

For self-signed certs, use `sslmode=require` (encryption only, no
verification). For a CA-signed cert, use `sslmode=verify-full`.

### Out of scope

- mTLS / client-cert authentication
- Cert hot-reload (restart to rotate)
- TLS for the (unbuilt) Raft transport
