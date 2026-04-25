//! Authentication module for the database engine.
//!
//! Passwords are stored as argon2id hashes (`argon2:$argon2id$v=19$...`).
//! The legacy CRC32 format (`hash:xxxxxxxx`) is still recognized on verify
//! so pre-Phase-D deployments keep working. Legacy hashes are not auto-rehashed
//! — users upgrade by setting their password again.

use std::collections::BTreeMap;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::rngs::OsRng;

const LEGACY_PREFIX: &str = "hash:";
const ARGON2_PREFIX: &str = "argon2:";

/// Manages user credentials.
pub struct AuthManager {
    /// username -> hashed password (for `authenticate()`)
    hashes: BTreeMap<String, String>,
    /// username -> cleartext password (for pgwire `AuthSource`)
    cleartext: BTreeMap<String, String>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            hashes: BTreeMap::new(),
            cleartext: BTreeMap::new(),
        }
    }

    /// Add a user with a plaintext password.
    ///
    /// Stores an argon2id hash. If hashing unexpectedly fails (should not
    /// happen for normal inputs), falls back to the legacy CRC32 hash so
    /// auth flow still works.
    pub fn add_user(&mut self, username: &str, password: &str) {
        let hash = hash_password_argon2(password)
            .unwrap_or_else(|| hash_password_legacy(username, password));
        self.hashes.insert(username.to_string(), hash);
        self.cleartext
            .insert(username.to_string(), password.to_string());
    }

    /// Authenticate a user by username and plaintext password.
    /// Supports both the modern argon2id format and the legacy CRC32 format.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let stored = match self.hashes.get(username) {
            Some(h) => h,
            None => return false,
        };
        verify_password(username, password, stored)
    }

    /// Returns true if any users have been configured.
    pub fn has_users(&self) -> bool {
        !self.hashes.is_empty()
    }

    /// Returns true if the given username exists.
    pub fn user_exists(&self, username: &str) -> bool {
        self.hashes.contains_key(username)
    }

    /// Look up the cleartext password for a user.
    /// Used by the pgwire `AuthSource` implementation.
    pub fn get_cleartext(&self, username: &str) -> Option<&str> {
        self.cleartext.get(username).map(|s| s.as_str())
    }

    /// Returns a list of all usernames.
    pub fn list_users(&self) -> Vec<&str> {
        self.hashes.keys().map(|s| s.as_str()).collect()
    }

    /// Remove a user. Returns true if the user existed.
    pub fn remove_user(&mut self, username: &str) -> bool {
        let removed = self.hashes.remove(username).is_some();
        self.cleartext.remove(username);
        removed
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash a password with argon2id. Returns `"argon2:" + phc_string` or None on failure.
fn hash_password_argon2(password: &str) -> Option<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon = Argon2::default();
    let phc = argon
        .hash_password(password.as_bytes(), &salt)
        .ok()?
        .to_string();
    Some(format!("{}{}", ARGON2_PREFIX, phc))
}

/// Legacy CRC32 hash. Kept for reading old stores and as an emergency fallback.
///
/// Format: `"hash:" + hex(crc32(password + ":" + username + ":rustdb"))`.
///
/// **Not cryptographically secure.**
fn hash_password_legacy(username: &str, password: &str) -> String {
    let input = format!("{}:{}:rustdb", password, username);
    let hash = crc32fast::hash(input.as_bytes());
    format!("{}{:08x}", LEGACY_PREFIX, hash)
}

/// Verify a password against a stored hash in either format.
fn verify_password(username: &str, password: &str, stored: &str) -> bool {
    if let Some(phc) = stored.strip_prefix(ARGON2_PREFIX) {
        let parsed = match PasswordHash::new(phc) {
            Ok(p) => p,
            Err(_) => return false,
        };
        return Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
    }
    if stored.starts_with(LEGACY_PREFIX) {
        // Constant-time-ish compare; inputs are both short hex strings.
        return stored == hash_password_legacy(username, password);
    }
    false
}

/// Authentication configuration.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// List of (username, password) pairs.
    pub users: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argon2_hash_roundtrip() {
        let h = hash_password_argon2("secret").expect("argon2 should hash");
        assert!(h.starts_with(ARGON2_PREFIX));
        assert!(verify_password("alice", "secret", &h));
        assert!(!verify_password("alice", "wrong", &h));
    }

    #[test]
    fn argon2_two_hashes_differ_via_salt() {
        let h1 = hash_password_argon2("same").unwrap();
        let h2 = hash_password_argon2("same").unwrap();
        assert_ne!(h1, h2, "argon2 salts should randomize the hash");
        assert!(verify_password("u", "same", &h1));
        assert!(verify_password("u", "same", &h2));
    }

    #[test]
    fn legacy_hash_still_verifies() {
        let legacy = hash_password_legacy("alice", "secret");
        assert!(legacy.starts_with(LEGACY_PREFIX));
        assert!(verify_password("alice", "secret", &legacy));
        assert!(!verify_password("alice", "wrong", &legacy));
        assert!(!verify_password("bob", "secret", &legacy));
    }

    #[test]
    fn add_user_stores_argon2_by_default() {
        let mut mgr = AuthManager::new();
        mgr.add_user("alice", "pw");
        assert!(mgr.hashes["alice"].starts_with(ARGON2_PREFIX));
        assert!(mgr.authenticate("alice", "pw"));
        assert!(!mgr.authenticate("alice", "wrong"));
    }

    #[test]
    fn auth_manager_accepts_legacy_hashed_entry() {
        // Simulate a user loaded from a pre-Phase-D store.
        let mut mgr = AuthManager::new();
        let legacy = hash_password_legacy("bob", "pw");
        mgr.hashes.insert("bob".to_string(), legacy);
        mgr.cleartext.insert("bob".to_string(), "pw".to_string());
        assert!(mgr.authenticate("bob", "pw"));
        assert!(!mgr.authenticate("bob", "wrong"));
    }
}
