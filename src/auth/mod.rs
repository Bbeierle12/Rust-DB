//! Authentication module for the database engine.
//!
//! Provides credential management and password verification.
//! Passwords are stored as hashed values internally; cleartext is also
//! retained for pgwire protocol authentication when the `server` feature
//! is enabled.

use std::collections::BTreeMap;

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
    pub fn add_user(&mut self, username: &str, password: &str) {
        let hash = hash_password(username, password);
        self.hashes.insert(username.to_string(), hash);
        self.cleartext.insert(username.to_string(), password.to_string());
    }

    /// Authenticate a user by username and plaintext password.
    /// Returns true if credentials match.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        match self.hashes.get(username) {
            Some(stored_hash) => {
                let provided_hash = hash_password(username, password);
                stored_hash == &provided_hash
            }
            None => false,
        }
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

/// Simple password hash using CRC32 (already a project dependency).
///
/// Format: `"hash:" + hex(crc32(password + ":" + username + ":rustdb"))`.
///
/// **Not cryptographically secure** — suitable for development and testing.
/// For production, replace with bcrypt or argon2.
fn hash_password(username: &str, password: &str) -> String {
    let input = format!("{}:{}:rustdb", password, username);
    let hash = crc32fast::hash(input.as_bytes());
    format!("hash:{:08x}", hash)
}

/// Authentication configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// List of (username, password) pairs.
    pub users: Vec<(String, String)>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            users: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_password_deterministic() {
        let h1 = hash_password("alice", "secret");
        let h2 = hash_password("alice", "secret");
        assert_eq!(h1, h2);
        assert!(h1.starts_with("hash:"));
    }

    #[test]
    fn hash_password_different_for_different_inputs() {
        let h1 = hash_password("alice", "secret");
        let h2 = hash_password("bob", "secret");
        assert_ne!(h1, h2);
    }
}
