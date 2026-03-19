use rust_dst_db::auth::{AuthConfig, AuthManager};

#[test]
fn auth_manager_add_and_authenticate() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "correcthorsebatterystaple");

    assert!(mgr.authenticate("alice", "correcthorsebatterystaple"));
}

#[test]
fn auth_manager_wrong_password() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "correcthorsebatterystaple");

    assert!(!mgr.authenticate("alice", "wrongpassword"));
}

#[test]
fn auth_manager_unknown_user() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "secret");

    assert!(!mgr.authenticate("bob", "secret"));
}

#[test]
fn auth_manager_no_users() {
    let mgr = AuthManager::new();

    assert!(!mgr.has_users());
    assert!(!mgr.authenticate("anyone", "anything"));
}

#[test]
fn auth_manager_has_users_and_exists() {
    let mut mgr = AuthManager::new();
    assert!(!mgr.has_users());
    assert!(!mgr.user_exists("alice"));

    mgr.add_user("alice", "pw");
    assert!(mgr.has_users());
    assert!(mgr.user_exists("alice"));
    assert!(!mgr.user_exists("bob"));
}

#[test]
fn auth_manager_multiple_users() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "pw_a");
    mgr.add_user("bob", "pw_b");

    assert!(mgr.authenticate("alice", "pw_a"));
    assert!(mgr.authenticate("bob", "pw_b"));
    assert!(!mgr.authenticate("alice", "pw_b"));
    assert!(!mgr.authenticate("bob", "pw_a"));
}

#[test]
fn auth_manager_overwrite_password() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "old_password");
    assert!(mgr.authenticate("alice", "old_password"));

    mgr.add_user("alice", "new_password");
    assert!(!mgr.authenticate("alice", "old_password"));
    assert!(mgr.authenticate("alice", "new_password"));
}

#[test]
fn auth_manager_get_cleartext() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice", "secret123");

    assert_eq!(mgr.get_cleartext("alice"), Some("secret123"));
    assert_eq!(mgr.get_cleartext("bob"), None);
}

#[test]
fn auth_config_defaults() {
    let cfg = AuthConfig::default();
    assert!(!cfg.enabled);
    assert!(cfg.users.is_empty());
}

#[test]
fn auth_config_with_users() {
    let cfg = AuthConfig {
        enabled: true,
        users: vec![
            ("admin".to_string(), "admin123".to_string()),
            ("reader".to_string(), "read_only".to_string()),
        ],
    };
    assert!(cfg.enabled);
    assert_eq!(cfg.users.len(), 2);
    assert_eq!(cfg.users[0].0, "admin");
}
