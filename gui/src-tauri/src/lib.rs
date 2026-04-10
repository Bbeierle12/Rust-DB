use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Connection {
    id: String,
    name: String,
    host: String,
    port: u16,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    last_used: Option<i64>,
}

#[derive(Serialize, Deserialize, Default)]
struct Store {
    connections: Vec<Connection>,
}

fn store_path(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("app_config_dir: {e}"))?;
    fs::create_dir_all(&dir).map_err(|e| format!("create dir {}: {e}", dir.display()))?;
    Ok(dir.join("connections.json"))
}

fn load_store(app: &AppHandle) -> Store {
    let Ok(path) = store_path(app) else {
        return Store::default();
    };
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_store(app: &AppHandle, store: &Store) -> Result<(), String> {
    let path = store_path(app)?;
    let json = serde_json::to_string_pretty(store).map_err(|e| e.to_string())?;
    fs::write(&path, json).map_err(|e| format!("write {}: {e}", path.display()))
}

fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tauri::command]
fn list_connections(app: AppHandle) -> Vec<Connection> {
    let mut store = load_store(&app);
    store
        .connections
        .sort_by_key(|c| std::cmp::Reverse(c.last_used.unwrap_or(0)));
    store.connections
}

#[tauri::command]
fn save_connection(app: AppHandle, conn: Connection) -> Result<(), String> {
    let mut store = load_store(&app);
    if let Some(existing) = store.connections.iter_mut().find(|c| c.id == conn.id) {
        *existing = conn;
    } else {
        store.connections.push(conn);
    }
    save_store(&app, &store)
}

#[tauri::command]
fn delete_connection(app: AppHandle, id: String) -> Result<(), String> {
    let mut store = load_store(&app);
    store.connections.retain(|c| c.id != id);
    save_store(&app, &store)
}

#[tauri::command]
fn touch_connection(app: AppHandle, id: String) -> Result<(), String> {
    let mut store = load_store(&app);
    if let Some(c) = store.connections.iter_mut().find(|c| c.id == id) {
        c.last_used = Some(now_epoch_secs());
    }
    save_store(&app, &store)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
                // Auto-open devtools in debug builds.
                if let Some(win) = app.get_webview_window("main") {
                    win.open_devtools();
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_connections,
            save_connection,
            delete_connection,
            touch_connection,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
