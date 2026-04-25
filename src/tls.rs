//! TLS material loading for the pgwire and web admin listeners.
//!
//! Loads PEM-encoded certificate + private key files into a
//! `tokio_rustls::TlsAcceptor` suitable for `process_socket(...)` and
//! `axum_server::bind_rustls(...)`. The rustls crypto provider is installed
//! lazily; calling `install_crypto_provider()` more than once is safe.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Errors that can arise while loading TLS material.
#[derive(Debug)]
pub enum TlsLoadError {
    OpenCert(String, std::io::Error),
    OpenKey(String, std::io::Error),
    ParseCert(String, std::io::Error),
    ParseKey(String, std::io::Error),
    NoCertificates(String),
    NoPrivateKey(String),
    Build(tokio_rustls::rustls::Error),
}

impl std::fmt::Display for TlsLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenCert(p, e) => write!(f, "opening cert '{}': {}", p, e),
            Self::OpenKey(p, e) => write!(f, "opening key '{}': {}", p, e),
            Self::ParseCert(p, e) => write!(f, "parsing cert PEM '{}': {}", p, e),
            Self::ParseKey(p, e) => write!(f, "parsing key PEM '{}': {}", p, e),
            Self::NoCertificates(p) => write!(f, "no certificates found in '{}'", p),
            Self::NoPrivateKey(p) => write!(f, "no private key found in '{}'", p),
            Self::Build(e) => write!(f, "building rustls ServerConfig: {}", e),
        }
    }
}

impl std::error::Error for TlsLoadError {}

/// Install the rustls aws-lc-rs crypto provider as the process default.
/// Idempotent — subsequent calls are silently ignored.
pub fn install_crypto_provider() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Build a `TlsAcceptor` from PEM-encoded cert and key files.
pub fn load_acceptor(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<Arc<TlsAcceptor>, TlsLoadError> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();

    let cert_str = cert_path.display().to_string();
    let key_str = key_path.display().to_string();

    let cert_file =
        File::open(cert_path).map_err(|e| TlsLoadError::OpenCert(cert_str.clone(), e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsLoadError::ParseCert(cert_str.clone(), e))?;
    if certs.is_empty() {
        return Err(TlsLoadError::NoCertificates(cert_str));
    }

    let key_file = File::open(key_path).map_err(|e| TlsLoadError::OpenKey(key_str.clone(), e))?;
    let mut key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| TlsLoadError::ParseKey(key_str.clone(), e))?
        .ok_or_else(|| TlsLoadError::NoPrivateKey(key_str))?;

    let server_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsLoadError::Build)?;

    Ok(Arc::new(TlsAcceptor::from(Arc::new(server_config))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Minimal self-signed cert generated via rcgen, valid for "localhost".
    fn make_self_signed() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        (cert.cert.pem(), cert.key_pair.serialize_pem())
    }

    fn write_temp(contents: &str, suffix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let name = format!(
            "rustdb-tls-test-{}-{}.{}",
            std::process::id(),
            rand::random::<u64>(),
            suffix
        );
        let path = dir.join(name);
        let mut f = File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn loads_self_signed_pair() {
        install_crypto_provider();
        let (cert_pem, key_pem) = make_self_signed();
        let cert_path = write_temp(&cert_pem, "crt");
        let key_path = write_temp(&key_pem, "key");
        let acc = load_acceptor(&cert_path, &key_path).expect("loader should succeed");
        // Sanity: acceptor handed back as an Arc.
        assert!(Arc::strong_count(&acc) >= 1);
        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn missing_cert_file_errors() {
        let result = load_acceptor("/nonexistent/cert.pem", "/nonexistent/key.pem");
        let err = match result {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(err, TlsLoadError::OpenCert(_, _)), "got {}", err);
    }

    #[test]
    fn empty_pem_yields_no_certificates() {
        let cert_path = write_temp("not a real PEM\n", "crt");
        let key_path = write_temp("also not a PEM\n", "key");
        let result = load_acceptor(&cert_path, &key_path);
        let err = match result {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, TlsLoadError::NoCertificates(_)),
            "got {}",
            err
        );
        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn cert_without_matching_key_errors() {
        install_crypto_provider();
        let (cert_pem, _) = make_self_signed();
        let (_, other_key_pem) = make_self_signed();
        let cert_path = write_temp(&cert_pem, "crt");
        let key_path = write_temp(&other_key_pem, "key");
        let result = load_acceptor(&cert_path, &key_path);
        let err = match result {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(err, TlsLoadError::Build(_)), "got {}", err);
        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }
}
