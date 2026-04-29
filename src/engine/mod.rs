//! Production database engine — synchronous, real-I/O path.
//!
//! Reuses the existing `MvccStore` and OCC conflict detection directly,
//! bypassing the actor-model `MessageBus` (which remains for DST testing).
//!
//! # Architecture
//!
//! ```text
//! Client (pgwire / Rust API)
//!   │
//!   ▼
//! Database  ─── owns ──→ MvccStore (in-memory MVCC KV)
//!   │                  → FileWal (real-file WAL)
//!   │                  → Catalog (schema storage in MvccStore)
//!   │
//!   ├─ begin()    → Transaction { txn_id, start_ts, write_set }
//!   ├─ get()      → snapshot read from MvccStore
//!   ├─ put()      → buffer in write_set
//!   ├─ commit()   → OCC validate → WAL append+fsync → apply to MvccStore
//!   ├─ execute_sql() → parse → plan → execute against MvccStore
//!   └─ recover()  → replay WAL records into MvccStore
//! ```

pub mod catalog;
pub mod snapshot;
pub mod wal;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::query::executor::{execute, execute_with_sources};
use crate::query::expr::{Row, Schema, Value};
use crate::query::sql::{SqlError, sql_to_plan_multi};
use crate::txn::conflict::{self, ValidationResult};
use crate::txn::mvcc::MvccStore;

use self::catalog::{Catalog, IndexDef};
use self::snapshot::{SnapshotEntry, SnapshotReader};
use self::wal::FileWal;

/// Default WAL-bytes-written threshold after which a checkpoint auto-fires.
pub const DEFAULT_CHECKPOINT_WAL_BYTES: u64 = 16 * 1024 * 1024;

/// Decoded commit-record writes: list of (key, optional value) pairs.
type CommitWrites = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// WAL record type tag for transaction commits.
const WAL_TXN_COMMIT: u8 = 10;
/// WAL record type tag for DDL operations.
const WAL_DDL: u8 = 20;

/// Errors from the database engine.
#[derive(Debug, Clone)]
pub enum DbError {
    NoSuchTxn(u64),
    Conflict(String),
    Sql(String),
    NoSuchTable(String),
    TableExists(String),
    Io(String),
    Constraint(String),
    Poison(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::NoSuchTxn(id) => write!(f, "transaction {} not found", id),
            DbError::Conflict(msg) => write!(f, "conflict: {}", msg),
            DbError::Sql(msg) => write!(f, "SQL error: {}", msg),
            DbError::NoSuchTable(t) => write!(f, "table '{}' not found", t),
            DbError::TableExists(t) => write!(f, "table '{}' already exists", t),
            DbError::Io(msg) => write!(f, "I/O error: {}", msg),
            DbError::Constraint(msg) => write!(f, "constraint violation: {}", msg),
            DbError::Poison(msg) => write!(f, "lock poisoned: {}", msg),
        }
    }
}

impl std::error::Error for DbError {}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e.to_string())
    }
}

impl From<SqlError> for DbError {
    fn from(e: SqlError) -> Self {
        DbError::Sql(e.0)
    }
}

pub type DbResult<T> = Result<T, DbError>;

struct ActiveTxn {
    start_ts: u64,
    write_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ddl_ops: Vec<DdlOp>,
}

enum DdlOp {
    CreateTable(Schema),
    DropTable(String),
    CreateIndex(IndexDef),
    DropIndex(String),
}

pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

struct DatabaseInner {
    store: MvccStore,
    wal: FileWal,
    active: BTreeMap<u64, ActiveTxn>,
    next_ts: u64,
    data_dir: PathBuf,
    /// Bytes appended to the WAL since the last successful checkpoint.
    wal_bytes_since_checkpoint: u64,
    /// Threshold at which commit() auto-fires a checkpoint (0 disables auto).
    checkpoint_wal_bytes: u64,
    /// Total checkpoints written since open (manual + auto). Used by metrics.
    checkpoint_count: u64,
}

impl Database {
    pub fn open(data_dir: impl Into<PathBuf>) -> DbResult<Self> {
        Self::open_with_checkpoint_bytes(data_dir, DEFAULT_CHECKPOINT_WAL_BYTES)
    }

    /// Open with a custom auto-checkpoint threshold. Pass 0 to disable auto-checkpoint.
    pub fn open_with_checkpoint_bytes(
        data_dir: impl Into<PathBuf>,
        checkpoint_wal_bytes: u64,
    ) -> DbResult<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        // Clean up any incomplete snapshot from a prior crash.
        snapshot::clear_partial(&data_dir)
            .map_err(|e| DbError::Io(format!("snapshot cleanup failed: {}", e)))?;

        let mut store = MvccStore::new();
        let mut max_ts = 0u64;

        // Step 1: load snapshot if present and valid.
        if let Some(reader) = SnapshotReader::read(&data_dir)
            .map_err(|e| DbError::Io(format!("snapshot read failed: {}", e)))?
        {
            if reader.next_ts > max_ts {
                max_ts = reader.next_ts.saturating_sub(1);
            }
            let entries = reader.entries.into_iter().map(|e| {
                // Install every snapshot entry at commit_ts = 1. The next_ts
                // from the snapshot header becomes the starting timestamp for
                // new transactions, so there's no ambiguity.
                (e.key, e.value, 1u64)
            });
            store.install_snapshot_entries(entries);
        }

        // Step 2: replay WAL on top of the snapshot.
        let wal_path = data_dir.join("wal.log");
        let mut wal = FileWal::open(&wal_path)?;

        let records = wal.read_all()?;

        for (_lsn, data) in &records {
            if data.is_empty() {
                continue;
            }
            match data[0] {
                WAL_TXN_COMMIT => {
                    if let Some((_, commit_ts, writes)) = decode_commit_record(data) {
                        for (key, value) in writes {
                            store.write(key, commit_ts, value);
                        }
                        if commit_ts > max_ts {
                            max_ts = commit_ts;
                        }
                    }
                }
                WAL_DDL => {
                    if let Some((commit_ts, ddl_data)) = decode_ddl_record(data) {
                        if ddl_data.first() == Some(&1) {
                            let key_data = &ddl_data[1..];
                            if let Some((key_len_bytes, rest)) = key_data.split_first_chunk::<4>() {
                                let key_len = u32::from_le_bytes(*key_len_bytes) as usize;
                                if rest.len() >= key_len {
                                    let key = rest[..key_len].to_vec();
                                    let value = rest[key_len..].to_vec();
                                    store.write(key, commit_ts, Some(value));
                                }
                            }
                        } else if ddl_data.first() == Some(&2) {
                            let key_data = &ddl_data[1..];
                            if key_data.len() >= 4 {
                                let key_len =
                                    u32::from_le_bytes(key_data[..4].try_into().unwrap()) as usize;
                                if key_data.len() >= 4 + key_len {
                                    let key = key_data[4..4 + key_len].to_vec();
                                    store.write(key, commit_ts, None);
                                }
                            }
                        } else if ddl_data.first() == Some(&3) {
                            let key_data = &ddl_data[1..];
                            if let Some((key_len_bytes, rest)) = key_data.split_first_chunk::<4>() {
                                let key_len = u32::from_le_bytes(*key_len_bytes) as usize;
                                if rest.len() >= key_len {
                                    let key = rest[..key_len].to_vec();
                                    let value = rest[key_len..].to_vec();
                                    store.write(key, commit_ts, Some(value));
                                }
                            }
                        } else if ddl_data.first() == Some(&4) {
                            let key_data = &ddl_data[1..];
                            if key_data.len() >= 4 {
                                let key_len =
                                    u32::from_le_bytes(key_data[..4].try_into().unwrap()) as usize;
                                if key_data.len() >= 4 + key_len {
                                    let key = key_data[4..4 + key_len].to_vec();
                                    store.write(key, commit_ts, None);
                                }
                            }
                        }
                        if commit_ts > max_ts {
                            max_ts = commit_ts;
                        }
                    }
                }
                _ => {}
            }
        }

        let wal_bytes_since_checkpoint = wal.file_len();
        let inner = DatabaseInner {
            store,
            wal,
            active: BTreeMap::new(),
            next_ts: max_ts + 1,
            data_dir,
            wal_bytes_since_checkpoint,
            checkpoint_wal_bytes,
            checkpoint_count: 0,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Total checkpoints written since this Database was opened.
    pub fn checkpoint_count(&self) -> DbResult<u64> {
        Ok(self.lock_inner()?.checkpoint_count)
    }

    /// Force-write a snapshot of the current state and truncate the WAL.
    pub fn checkpoint(&self) -> DbResult<()> {
        let mut inner = self.lock_inner()?;
        checkpoint_locked(&mut inner)
    }

    fn lock_inner(&self) -> DbResult<std::sync::MutexGuard<'_, DatabaseInner>> {
        self.inner
            .lock()
            .map_err(|e| DbError::Poison(e.to_string()))
    }

    pub fn begin(&self) -> DbResult<u64> {
        let mut inner = self.lock_inner()?;
        let txn_id = inner.next_ts;
        inner.next_ts += 1;
        inner.active.insert(
            txn_id,
            ActiveTxn {
                start_ts: txn_id,
                write_set: BTreeMap::new(),
                ddl_ops: Vec::new(),
            },
        );
        Ok(txn_id)
    }

    pub fn get(&self, txn_id: u64, key: &[u8]) -> DbResult<Option<Vec<u8>>> {
        let inner = self.lock_inner()?;
        let txn = inner
            .active
            .get(&txn_id)
            .ok_or(DbError::NoSuchTxn(txn_id))?;
        if let Some(buffered) = txn.write_set.get(key) {
            return Ok(buffered.clone());
        }
        Ok(inner.store.read(key, txn.start_ts))
    }

    pub fn put(&self, txn_id: u64, key: Vec<u8>, value: Vec<u8>) -> DbResult<()> {
        let mut inner = self.lock_inner()?;
        let txn = inner
            .active
            .get_mut(&txn_id)
            .ok_or(DbError::NoSuchTxn(txn_id))?;
        txn.write_set.insert(key, Some(value));
        Ok(())
    }

    pub fn delete(&self, txn_id: u64, key: Vec<u8>) -> DbResult<()> {
        let mut inner = self.lock_inner()?;
        let txn = inner
            .active
            .get_mut(&txn_id)
            .ok_or(DbError::NoSuchTxn(txn_id))?;
        txn.write_set.insert(key, None);
        Ok(())
    }

    pub fn scan(
        &self,
        txn_id: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> DbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = self.lock_inner()?;
        let txn = inner
            .active
            .get(&txn_id)
            .ok_or(DbError::NoSuchTxn(txn_id))?;
        let mut entries = inner.store.scan(start, end, txn.start_ts);
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for (k, v) in &entries {
            merged.insert(k.clone(), Some(v.clone()));
        }
        for (k, v) in &txn.write_set {
            if let Some(s) = start
                && k.as_slice() < s
            {
                continue;
            }
            if let Some(e) = end
                && k.as_slice() >= e
            {
                continue;
            }
            merged.insert(k.clone(), v.clone());
        }
        entries = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();
        Ok(entries)
    }

    pub fn commit(&self, txn_id: u64) -> DbResult<()> {
        let mut inner = self.lock_inner()?;
        let txn = inner
            .active
            .remove(&txn_id)
            .ok_or(DbError::NoSuchTxn(txn_id))?;
        if txn.write_set.is_empty() && txn.ddl_ops.is_empty() {
            return Ok(());
        }

        let validation = conflict::validate_write_set(&inner.store, &txn.write_set, txn.start_ts);
        match validation {
            ValidationResult::Conflict { key } => {
                return Err(DbError::Conflict(format!(
                    "write-write conflict on key {:?}",
                    key
                )));
            }
            ValidationResult::Ok => {}
        }

        let commit_ts = inner.next_ts;
        inner.next_ts += 1;

        if !txn.write_set.is_empty() {
            let wal_data = encode_commit_record(txn_id, commit_ts, &txn.write_set);
            inner
                .wal
                .append_sync(&wal_data)
                .map_err(|e| DbError::Io(format!("WAL write failed: {}", e)))?;
        }

        for ddl_op in &txn.ddl_ops {
            let wal_data = encode_ddl_record(commit_ts, ddl_op);
            inner
                .wal
                .append_sync(&wal_data)
                .map_err(|e| DbError::Io(format!("WAL DDL write failed: {}", e)))?;
        }

        for (key, value) in &txn.write_set {
            inner.store.write(key.clone(), commit_ts, value.clone());
        }

        for ddl_op in &txn.ddl_ops {
            match ddl_op {
                DdlOp::CreateTable(schema) => {
                    let _ = Catalog::create_table(&mut inner.store, schema, commit_ts);
                }
                DdlOp::DropTable(name) => {
                    let _ = Catalog::drop_table(&mut inner.store, name, commit_ts);
                }
                DdlOp::CreateIndex(index_def) => {
                    let _ = Catalog::create_index(&mut inner.store, index_def, commit_ts);
                }
                DdlOp::DropIndex(name) => {
                    let _ = Catalog::drop_index(&mut inner.store, name, commit_ts);
                }
            }
        }

        // Track WAL growth and auto-checkpoint when the threshold is crossed.
        inner.wal_bytes_since_checkpoint = inner.wal.file_len();
        if inner.checkpoint_wal_bytes > 0
            && inner.wal_bytes_since_checkpoint >= inner.checkpoint_wal_bytes
        {
            checkpoint_locked(&mut inner)?;
        }
        Ok(())
    }

    pub fn abort(&self, txn_id: u64) -> DbResult<()> {
        let mut inner = self.lock_inner()?;
        inner.active.remove(&txn_id);
        Ok(())
    }

    fn ensure_active_txn(&self, txn_id: u64) -> DbResult<()> {
        let inner = self.lock_inner()?;
        if inner.active.contains_key(&txn_id) {
            Ok(())
        } else {
            Err(DbError::NoSuchTxn(txn_id))
        }
    }

    fn statement_txn(&self, txn_id: Option<u64>) -> DbResult<(u64, bool)> {
        match txn_id {
            Some(txn_id) => {
                self.ensure_active_txn(txn_id)?;
                Ok((txn_id, false))
            }
            None => Ok((self.begin()?, true)),
        }
    }

    fn finish_statement_txn(&self, txn_id: u64, implicit: bool) -> DbResult<()> {
        if implicit {
            self.commit(txn_id)?;
        }
        Ok(())
    }

    fn abort_statement_txn(&self, txn_id: u64) -> DbResult<()> {
        self.abort(txn_id)
    }

    pub fn execute_sql(&self, sql: &str) -> DbResult<SqlResult> {
        let stripped = strip_sql_comments(sql);
        let trimmed = stripped.trim();
        let upper = trimmed.to_uppercase();
        if upper == "BEGIN" {
            return Ok(SqlResult::Begin(self.begin()?));
        }
        if upper == "COMMIT" {
            return Ok(SqlResult::Commit);
        }
        if upper == "ROLLBACK" {
            return Ok(SqlResult::Rollback);
        }
        if upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE UNIQUE INDEX") {
            return self.execute_create_index(trimmed);
        }
        if upper.starts_with("DROP INDEX") {
            return self.execute_drop_index(trimmed);
        }
        if upper.starts_with("CREATE TABLE") {
            return self.execute_create_table(trimmed);
        }
        if upper.starts_with("DROP TABLE") {
            return self.execute_drop_table(trimmed);
        }
        if upper.starts_with("INSERT") {
            return self.execute_insert(trimmed);
        }
        if upper.starts_with("UPDATE") {
            return self.execute_update(trimmed);
        }
        if upper.starts_with("DELETE") {
            return self.execute_delete(trimmed);
        }
        self.execute_select(trimmed)
    }

    pub fn execute_sql_in_txn(&self, sql: &str, txn_id: u64) -> DbResult<SqlResult> {
        self.ensure_active_txn(txn_id)?;
        let stripped = strip_sql_comments(sql);
        let trimmed = stripped.trim();
        let upper = trimmed.to_uppercase();
        if upper == "BEGIN" {
            return Err(DbError::Sql("transaction already active".into()));
        }
        if upper == "COMMIT" {
            return Ok(SqlResult::Commit);
        }
        if upper == "ROLLBACK" {
            return Ok(SqlResult::Rollback);
        }
        if upper.starts_with("CREATE") || upper.starts_with("DROP") {
            return Err(DbError::Sql(
                "DDL statements inside explicit transactions are not yet supported".into(),
            ));
        }
        if upper.starts_with("INSERT") {
            return self.execute_insert_in_txn(trimmed, Some(txn_id));
        }
        if upper.starts_with("UPDATE") {
            return self.execute_update_in_txn(trimmed, Some(txn_id));
        }
        if upper.starts_with("DELETE") {
            return self.execute_delete_in_txn(trimmed, Some(txn_id));
        }
        self.execute_select_in_txn(trimmed, Some(txn_id))
    }

    fn execute_create_table(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            sqlparser::ast::Statement::CreateTable(ct) => {
                let table_name = ct.name.to_string();
                let mut columns = Vec::new();
                for col_def in &ct.columns {
                    let col_type = sql_type_to_value_type(&col_def.data_type);
                    let mut col =
                        crate::query::expr::Column::new(col_def.name.value.clone(), col_type);
                    for opt in &col_def.options {
                        if matches!(opt.option, sqlparser::ast::ColumnOption::NotNull) {
                            col = col.not_null();
                        }
                    }
                    columns.push(col);
                }
                let schema = Schema::new(table_name, columns);
                let txn_id = self.begin()?;
                {
                    let mut inner = self.lock_inner()?;
                    let snapshot_ts = inner.active.get(&txn_id).unwrap().start_ts;
                    if Catalog::get_table(&inner.store, &schema.table, snapshot_ts).is_some() {
                        inner.active.remove(&txn_id);
                        if ct.if_not_exists {
                            return Ok(SqlResult::Execute(0));
                        }
                        return Err(DbError::TableExists(schema.table.clone()));
                    }
                    inner
                        .active
                        .get_mut(&txn_id)
                        .unwrap()
                        .ddl_ops
                        .push(DdlOp::CreateTable(schema));
                }
                self.commit(txn_id)?;
                Ok(SqlResult::Execute(0))
            }
            _ => Err(DbError::Sql("expected CREATE TABLE".into())),
        }
    }

    fn execute_drop_table(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            sqlparser::ast::Statement::Drop { names, .. } => {
                let table_name = names
                    .first()
                    .ok_or_else(|| DbError::Sql("expected table name".into()))?
                    .to_string();
                let txn_id = self.begin()?;
                {
                    let mut inner = self.lock_inner()?;
                    let snapshot_ts = inner.active.get(&txn_id).unwrap().start_ts;
                    if Catalog::get_table(&inner.store, &table_name, snapshot_ts).is_none() {
                        inner.active.remove(&txn_id);
                        return Err(DbError::NoSuchTable(table_name));
                    }
                    inner
                        .active
                        .get_mut(&txn_id)
                        .unwrap()
                        .ddl_ops
                        .push(DdlOp::DropTable(table_name));
                }
                self.commit(txn_id)?;
                Ok(SqlResult::Execute(0))
            }
            _ => Err(DbError::Sql("expected DROP TABLE".into())),
        }
    }

    fn execute_create_index(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            sqlparser::ast::Statement::CreateIndex(ci) => {
                let index_name = ci
                    .name
                    .as_ref()
                    .ok_or_else(|| DbError::Sql("index name required".into()))?
                    .to_string();
                let table_name = ci.table_name.to_string();
                let unique = ci.unique;
                let columns: Vec<String> = ci.columns.iter().map(|c| c.expr.to_string()).collect();
                if columns.is_empty() {
                    return Err(DbError::Sql("index must have at least one column".into()));
                }

                let inner = self.lock_inner()?;
                let snapshot_ts = inner.next_ts.saturating_sub(1);
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                if Catalog::get_index(&inner.store, &index_name, snapshot_ts).is_some() {
                    if ci.if_not_exists {
                        return Ok(SqlResult::Execute(0));
                    }
                    return Err(DbError::Sql(format!(
                        "index '{}' already exists",
                        index_name
                    )));
                }
                for col in &columns {
                    if schema.column_index(col).is_none() {
                        return Err(DbError::Sql(format!(
                            "column '{}' does not exist in table '{}'",
                            col, table_name
                        )));
                    }
                }
                drop(inner);

                let index_def = IndexDef {
                    name: index_name,
                    table: table_name.clone(),
                    columns: columns.clone(),
                    unique,
                };
                let rows = self.scan_table_rows(&schema)?;

                if unique && rows.len() > 1 {
                    let mut seen_values: Vec<Vec<u8>> = Vec::new();
                    for row in &rows {
                        let col_key = encode_index_column_values(&columns, row);
                        if seen_values.contains(&col_key) {
                            return Err(DbError::Constraint(format!(
                                "cannot create unique index '{}': duplicate values exist",
                                index_def.name
                            )));
                        }
                        seen_values.push(col_key);
                    }
                }

                let txn_id = self.begin()?;
                for row in &rows {
                    let idx_key = make_index_entry_key(&index_def, &schema, row);
                    self.put(txn_id, idx_key, Vec::new())?;
                }
                {
                    let mut inner = self.lock_inner()?;
                    inner
                        .active
                        .get_mut(&txn_id)
                        .ok_or(DbError::NoSuchTxn(txn_id))?
                        .ddl_ops
                        .push(DdlOp::CreateIndex(index_def));
                }
                self.commit(txn_id)?;
                Ok(SqlResult::Execute(0))
            }
            _ => Err(DbError::Sql("expected CREATE INDEX".into())),
        }
    }

    fn execute_drop_index(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            sqlparser::ast::Statement::Drop { names, .. } => {
                let index_name = names
                    .first()
                    .ok_or_else(|| DbError::Sql("expected index name".into()))?
                    .to_string();
                let inner = self.lock_inner()?;
                let snapshot_ts = inner.next_ts.saturating_sub(1);
                let index_def = Catalog::get_index(&inner.store, &index_name, snapshot_ts)
                    .ok_or_else(|| {
                        DbError::Sql(format!("index '{}' does not exist", index_name))
                    })?;
                let prefix = format!("__idx__\x00{}\x00", index_def.name);
                let prefix_bytes = prefix.as_bytes();
                let mut end_bytes = prefix_bytes.to_vec();
                if let Some(last) = end_bytes.last_mut() {
                    *last = last.wrapping_add(1);
                }
                let entries = inner
                    .store
                    .scan(Some(prefix_bytes), Some(&end_bytes), snapshot_ts);
                drop(inner);

                let txn_id = self.begin()?;
                for (key, _) in entries {
                    self.delete(txn_id, key)?;
                }
                {
                    let mut inner = self.lock_inner()?;
                    inner
                        .active
                        .get_mut(&txn_id)
                        .ok_or(DbError::NoSuchTxn(txn_id))?
                        .ddl_ops
                        .push(DdlOp::DropIndex(index_name));
                }
                self.commit(txn_id)?;
                Ok(SqlResult::Execute(0))
            }
            _ => Err(DbError::Sql("expected DROP INDEX".into())),
        }
    }

    fn maintain_index_insert(
        &self,
        txn_id: u64,
        schema: &Schema,
        index: &IndexDef,
        row: &Row,
    ) -> DbResult<()> {
        if index.unique {
            let inner = self.lock_inner()?;
            let snapshot_ts = inner.next_ts.saturating_sub(1);
            let col_prefix = make_index_column_prefix(index, row);
            let mut end_bytes = col_prefix.clone();
            if let Some(last) = end_bytes.last_mut() {
                *last = last.wrapping_add(1);
            }
            let existing = inner
                .store
                .scan(Some(&col_prefix), Some(&end_bytes), snapshot_ts);
            drop(inner);
            if !existing.is_empty() {
                return Err(DbError::Constraint(format!(
                    "duplicate key violates unique index '{}'",
                    index.name
                )));
            }
        }
        let idx_key = make_index_entry_key(index, schema, row);
        self.put(txn_id, idx_key, Vec::new())?;
        Ok(())
    }

    fn maintain_index_delete(
        &self,
        txn_id: u64,
        schema: &Schema,
        index: &IndexDef,
        row: &Row,
    ) -> DbResult<()> {
        let idx_key = make_index_entry_key(index, schema, row);
        self.delete(txn_id, idx_key)?;
        Ok(())
    }

    pub fn index_lookup(
        &self,
        schema: &Schema,
        index: &IndexDef,
        lookup_values: &[Value],
    ) -> DbResult<Vec<Row>> {
        let inner = self.lock_inner()?;
        let snapshot_ts = inner.next_ts.saturating_sub(1);
        let mut prefix = format!("__idx__\x00{}\x00", index.name).into_bytes();
        for val in lookup_values {
            prefix.extend_from_slice(&val.encode());
        }
        prefix.push(0x00);
        let mut end_bytes = prefix.clone();
        if let Some(last) = end_bytes.last_mut() {
            *last = last.wrapping_add(1);
        }
        let idx_entries = inner
            .store
            .scan(Some(&prefix), Some(&end_bytes), snapshot_ts);
        let mut rows = Vec::new();
        for (idx_key, _) in &idx_entries {
            let pk_encoded = &idx_key[prefix.len()..];
            let mut table_key = schema.table.as_bytes().to_vec();
            table_key.push(0x00);
            table_key.extend_from_slice(pk_encoded);
            if let Some(data) = inner.store.read(&table_key, snapshot_ts)
                && let Some(row) = schema.decode_row(&data)
            {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn execute_insert(&self, sql: &str) -> DbResult<SqlResult> {
        self.execute_insert_in_txn(sql, None)
    }

    fn execute_insert_in_txn(&self, sql: &str, statement_txn: Option<u64>) -> DbResult<SqlResult> {
        use sqlparser::ast::{SetExpr, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            Statement::Insert(insert) => {
                let table_name = insert.table_name.to_string();
                let inner = self.lock_inner()?;
                let snapshot_ts = statement_snapshot_ts(&inner, statement_txn)?;
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                let indexes =
                    Catalog::list_indexes_for_table(&inner.store, &table_name, snapshot_ts);
                drop(inner);

                let target_cols: Vec<String> = if insert.columns.is_empty() {
                    schema.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    insert.columns.iter().map(|c| c.value.clone()).collect()
                };
                let body = insert
                    .source
                    .as_ref()
                    .ok_or_else(|| DbError::Sql("INSERT requires VALUES".into()))?;
                let value_rows = match body.body.as_ref() {
                    SetExpr::Values(values) => &values.rows,
                    _ => return Err(DbError::Sql("expected VALUES clause".into())),
                };

                // Parse RETURNING clause
                let has_returning = insert.returning.as_ref().is_some_and(|r| !r.is_empty());
                let returning_cols = if has_returning {
                    parse_returning_cols(insert.returning.as_ref().unwrap(), &schema)?
                } else {
                    vec![]
                };
                let mut returned_rows: Vec<Row> = Vec::new();

                // Parse ON CONFLICT clause
                let on_conflict = &insert.on;

                let (txn_id, implicit_txn) = self.statement_txn(statement_txn)?;
                let mut count = 0u64;
                for value_row in value_rows {
                    if value_row.len() != target_cols.len() {
                        self.abort_statement_txn(txn_id)?;
                        return Err(DbError::Sql(format!(
                            "expected {} values, got {}",
                            target_cols.len(),
                            value_row.len()
                        )));
                    }
                    let mut row = Row::new();
                    for (col_name, expr) in target_cols.iter().zip(value_row.iter()) {
                        let value = sql_expr_to_value(expr)?;
                        let coerced = match coerce_value_for_column(value, col_name, &schema) {
                            Ok(v) => v,
                            Err(e) => {
                                self.abort_statement_txn(txn_id)?;
                                return Err(e);
                            }
                        };
                        row.insert(col_name.clone(), coerced);
                    }
                    for col in &schema.columns {
                        if !col.nullable {
                            let val = row.get(&col.name).unwrap_or(&Value::Null);
                            if val.is_null() {
                                self.abort_statement_txn(txn_id)?;
                                return Err(DbError::Constraint(format!(
                                    "column '{}' cannot be null",
                                    col.name
                                )));
                            }
                        }
                    }
                    let pk_val = row
                        .get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);

                    // Check for existing row (for ON CONFLICT handling)
                    let existing = self.get(txn_id, &key)?;
                    if let (Some(existing_data), Some(sqlparser::ast::OnInsert::OnConflict(oc))) =
                        (existing, on_conflict)
                    {
                        match &oc.action {
                            sqlparser::ast::OnConflictAction::DoNothing => {
                                continue;
                            }
                            sqlparser::ast::OnConflictAction::DoUpdate(do_update) => {
                                let mut merged = schema
                                    .decode_row(&existing_data)
                                    .ok_or_else(|| DbError::Sql("corrupt row data".into()))?;
                                for assignment in &do_update.assignments {
                                    let col_name = match &assignment.target {
                                        sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                            name.to_string()
                                        }
                                        sqlparser::ast::AssignmentTarget::Tuple(names) => {
                                            names.first().map(|n| n.to_string()).unwrap_or_default()
                                        }
                                    };
                                    let value = resolve_excluded_value(&assignment.value, &row)?;
                                    let coerced =
                                        coerce_value_for_column(value, &col_name, &schema)?;
                                    merged.insert(col_name, coerced);
                                }
                                let encoded = schema.encode_row(&merged);
                                self.put(txn_id, key, encoded)?;
                                if let Some(old_row) = schema.decode_row(&existing_data) {
                                    for index in &indexes {
                                        self.maintain_index_delete(
                                            txn_id, &schema, index, &old_row,
                                        )?;
                                        self.maintain_index_insert(
                                            txn_id, &schema, index, &merged,
                                        )?;
                                    }
                                }
                                if has_returning {
                                    returned_rows.push(project_returning(
                                        &merged,
                                        &returning_cols,
                                        &schema,
                                    ));
                                }
                                count += 1;
                                continue;
                            }
                        }
                    }

                    let value = schema.encode_row(&row);
                    self.put(txn_id, key, value)?;
                    for index in &indexes {
                        self.maintain_index_insert(txn_id, &schema, index, &row)?;
                    }
                    if has_returning {
                        returned_rows.push(project_returning(&row, &returning_cols, &schema));
                    }
                    count += 1;
                }
                self.finish_statement_txn(txn_id, implicit_txn)?;
                if has_returning {
                    let column_types = column_types_from_schema(&returning_cols, &schema);
                    Ok(SqlResult::Query {
                        columns: returning_cols,
                        column_types,
                        rows: returned_rows,
                    })
                } else {
                    Ok(SqlResult::Execute(count))
                }
            }
            _ => Err(DbError::Sql("expected INSERT".into())),
        }
    }

    fn execute_update(&self, sql: &str) -> DbResult<SqlResult> {
        self.execute_update_in_txn(sql, None)
    }

    fn execute_update_in_txn(&self, sql: &str, statement_txn: Option<u64>) -> DbResult<SqlResult> {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            Statement::Update {
                table,
                assignments,
                selection,
                returning,
                ..
            } => {
                let table_name = table.relation.to_string();
                let inner = self.lock_inner()?;
                let snapshot_ts = statement_snapshot_ts(&inner, statement_txn)?;
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                let indexes =
                    Catalog::list_indexes_for_table(&inner.store, &table_name, snapshot_ts);
                drop(inner);

                // Parse RETURNING clause
                let has_returning = returning.as_ref().is_some_and(|r| !r.is_empty());
                let returning_cols = if has_returning {
                    parse_returning_cols(returning.as_ref().unwrap(), &schema)?
                } else {
                    vec![]
                };
                let mut returned_rows: Vec<Row> = Vec::new();

                let rows = self.scan_table_rows_for_txn(&schema, statement_txn)?;
                let predicate = match selection {
                    Some(expr) => Some(crate::query::sql::sql_to_plan(
                        &format!("SELECT * FROM {} WHERE {}", table_name, expr),
                        schema.clone(),
                    )?),
                    None => None,
                };

                let (txn_id, implicit_txn) = self.statement_txn(statement_txn)?;
                let mut count = 0u64;
                for row in &rows {
                    if let Some(ref plan) = predicate
                        && execute(plan, vec![row.clone()]).is_empty()
                    {
                        continue;
                    }
                    let mut updated_row = row.clone();
                    for assignment in assignments {
                        let col_name = match &assignment.target {
                            sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                            sqlparser::ast::AssignmentTarget::Tuple(names) => {
                                names.first().map(|n| n.to_string()).unwrap_or_default()
                            }
                        };
                        let value = resolve_update_value(&assignment.value, &updated_row)?;
                        let coerced = coerce_value_for_column(value, &col_name, &schema)?;
                        updated_row.insert(col_name, coerced);
                    }
                    for index in &indexes {
                        self.maintain_index_delete(txn_id, &schema, index, row)?;
                        self.maintain_index_insert(txn_id, &schema, index, &updated_row)?;
                    }
                    let pk_val = updated_row
                        .get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);
                    let value = schema.encode_row(&updated_row);
                    self.put(txn_id, key, value)?;
                    if has_returning {
                        returned_rows.push(project_returning(
                            &updated_row,
                            &returning_cols,
                            &schema,
                        ));
                    }
                    count += 1;
                }
                self.finish_statement_txn(txn_id, implicit_txn)?;
                if has_returning {
                    let column_types = column_types_from_schema(&returning_cols, &schema);
                    Ok(SqlResult::Query {
                        columns: returning_cols,
                        column_types,
                        rows: returned_rows,
                    })
                } else {
                    Ok(SqlResult::Execute(count))
                }
            }
            _ => Err(DbError::Sql("expected UPDATE".into())),
        }
    }

    fn execute_delete(&self, sql: &str) -> DbResult<SqlResult> {
        self.execute_delete_in_txn(sql, None)
    }

    fn execute_delete_in_txn(&self, sql: &str, statement_txn: Option<u64>) -> DbResult<SqlResult> {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        match &stmts[0] {
            Statement::Delete(delete) => {
                let from_tables = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables) => tables.clone(),
                    sqlparser::ast::FromTable::WithoutKeyword(tables) => tables.clone(),
                };
                let table_name = if !delete.tables.is_empty() {
                    delete.tables[0].to_string()
                } else {
                    from_tables
                        .first()
                        .map(|f| f.relation.to_string())
                        .ok_or_else(|| DbError::Sql("DELETE requires FROM clause".into()))?
                };
                let inner = self.lock_inner()?;
                let snapshot_ts = statement_snapshot_ts(&inner, statement_txn)?;
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                let indexes =
                    Catalog::list_indexes_for_table(&inner.store, &table_name, snapshot_ts);
                drop(inner);

                // Parse RETURNING clause
                let has_returning = delete.returning.as_ref().is_some_and(|r| !r.is_empty());
                let returning_cols = if has_returning {
                    parse_returning_cols(delete.returning.as_ref().unwrap(), &schema)?
                } else {
                    vec![]
                };
                let mut returned_rows: Vec<Row> = Vec::new();

                let rows = self.scan_table_rows_for_txn(&schema, statement_txn)?;
                let predicate = match &delete.selection {
                    Some(expr) => Some(crate::query::sql::sql_to_plan(
                        &format!("SELECT * FROM {} WHERE {}", table_name, expr),
                        schema.clone(),
                    )?),
                    None => None,
                };

                let (txn_id, implicit_txn) = self.statement_txn(statement_txn)?;
                let mut count = 0u64;
                for row in &rows {
                    if let Some(ref plan) = predicate
                        && execute(plan, vec![row.clone()]).is_empty()
                    {
                        continue;
                    }
                    if has_returning {
                        returned_rows.push(project_returning(row, &returning_cols, &schema));
                    }
                    for index in &indexes {
                        self.maintain_index_delete(txn_id, &schema, index, row)?;
                    }
                    let pk_val = row
                        .get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);
                    self.delete(txn_id, key)?;
                    count += 1;
                }
                self.finish_statement_txn(txn_id, implicit_txn)?;
                if has_returning {
                    let column_types = column_types_from_schema(&returning_cols, &schema);
                    Ok(SqlResult::Query {
                        columns: returning_cols,
                        column_types,
                        rows: returned_rows,
                    })
                } else {
                    Ok(SqlResult::Execute(count))
                }
            }
            _ => Err(DbError::Sql("expected DELETE".into())),
        }
    }

    fn execute_select(&self, sql: &str) -> DbResult<SqlResult> {
        self.execute_select_in_txn(sql, None)
    }

    fn execute_select_in_txn(&self, sql: &str, statement_txn: Option<u64>) -> DbResult<SqlResult> {
        let inner = self.lock_inner()?;
        let snapshot_ts = statement_snapshot_ts(&inner, statement_txn)?;

        // Try multi-table path first (handles JOINs).
        let plan = sql_to_plan_multi(sql, |name| {
            Catalog::get_table(&inner.store, name, snapshot_ts)
        })?;

        let table_names = plan.collect_table_names();

        if table_names.len() > 1 {
            // Multi-table query: build sources map with prefixed column names.
            let mut schemas: BTreeMap<String, Schema> = BTreeMap::new();
            for tname in &table_names {
                let schema = Catalog::get_table(&inner.store, tname, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(tname.clone()))?;
                schemas.insert(tname.clone(), schema);
            }
            drop(inner);

            let mut sources: BTreeMap<String, Vec<Row>> = BTreeMap::new();
            for (tname, schema) in &schemas {
                let raw_rows = self.scan_table_rows_for_txn(schema, statement_txn)?;
                let prefixed: Vec<Row> = raw_rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|(k, v)| (format!("{}.{}", tname, k), v))
                            .collect()
                    })
                    .collect();
                sources.insert(tname.clone(), prefixed);
            }

            let result_rows = execute_with_sources(&plan, &sources);
            let columns: Vec<String> = if let Some(proj_cols) = plan.project_columns() {
                proj_cols
            } else if result_rows.is_empty() {
                table_names
                    .iter()
                    .flat_map(|t| match schemas.get(t) {
                        Some(schema) => schema
                            .columns
                            .iter()
                            .map(|c| format!("{}.{}", t, c.name))
                            .collect::<Vec<_>>(),
                        None => Vec::new(),
                    })
                    .collect()
            } else {
                result_rows[0].keys().cloned().collect()
            };
            let column_types = column_types_from_rows(&columns, &result_rows);
            Ok(SqlResult::Query {
                columns,
                column_types,
                rows: result_rows,
            })
        } else {
            // Single-table query: use original non-prefixed path.
            let table_name = table_names
                .into_iter()
                .next()
                .ok_or_else(|| DbError::Sql("could not determine table name".into()))?;
            let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
            drop(inner);

            let rows = self.scan_table_rows_for_txn(&schema, statement_txn)?;
            let result_rows = execute(&plan, rows);
            let columns: Vec<String> = if let Some(proj_cols) = plan.project_columns() {
                proj_cols
            } else if result_rows.is_empty() {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                result_rows[0].keys().cloned().collect()
            };
            let column_types = column_types_from_schema(&columns, &schema);
            Ok(SqlResult::Query {
                columns,
                column_types,
                rows: result_rows,
            })
        }
    }

    fn scan_table_rows(&self, schema: &Schema) -> DbResult<Vec<Row>> {
        let inner = self.lock_inner()?;
        let snapshot_ts = inner.next_ts.saturating_sub(1);
        let (prefix_bytes, end_bytes) = table_key_bounds(&schema.table);
        let raw_entries = inner
            .store
            .scan(Some(&prefix_bytes), Some(&end_bytes), snapshot_ts);
        Ok(raw_entries
            .iter()
            .filter_map(|(_, data)| schema.decode_row(data))
            .collect())
    }

    fn scan_table_rows_for_txn(
        &self,
        schema: &Schema,
        statement_txn: Option<u64>,
    ) -> DbResult<Vec<Row>> {
        match statement_txn {
            Some(txn_id) => {
                let (prefix_bytes, end_bytes) = table_key_bounds(&schema.table);
                let raw_entries = self.scan(txn_id, Some(&prefix_bytes), Some(&end_bytes))?;
                Ok(raw_entries
                    .iter()
                    .filter_map(|(_, data)| schema.decode_row(data))
                    .collect())
            }
            None => self.scan_table_rows(schema),
        }
    }

    pub fn data_dir(&self) -> DbResult<PathBuf> {
        Ok(self.lock_inner()?.data_dir.clone())
    }

    pub fn table_count(&self) -> DbResult<usize> {
        let inner = self.lock_inner()?;
        let ts = inner.next_ts.saturating_sub(1);
        Ok(Catalog::list_tables(&inner.store, ts).len())
    }

    pub fn get_schema(&self, table_name: &str) -> DbResult<Option<Schema>> {
        let inner = self.lock_inner()?;
        let ts = inner.next_ts.saturating_sub(1);
        Ok(Catalog::get_table(&inner.store, table_name, ts))
    }

    pub fn get_index(&self, index_name: &str) -> DbResult<Option<IndexDef>> {
        let inner = self.lock_inner()?;
        let ts = inner.next_ts.saturating_sub(1);
        Ok(Catalog::get_index(&inner.store, index_name, ts))
    }

    pub fn list_tables(&self) -> DbResult<Vec<Schema>> {
        let inner = self.lock_inner()?;
        let ts = inner.next_ts.saturating_sub(1);
        Ok(Catalog::list_tables(&inner.store, ts))
    }

    pub fn list_indexes(&self, table_name: &str) -> DbResult<Vec<IndexDef>> {
        let inner = self.lock_inner()?;
        let ts = inner.next_ts.saturating_sub(1);
        Ok(Catalog::list_indexes_for_table(
            &inner.store,
            table_name,
            ts,
        ))
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Write a snapshot of the current MVCC state, then truncate the WAL.
///
/// Caller holds the inner lock. The snapshot captures the latest committed
/// value for every key as of `next_ts - 1`, plus `next_ts` itself so new
/// transactions keep a monotonic timestamp after reopen.
fn checkpoint_locked(inner: &mut DatabaseInner) -> DbResult<()> {
    let latest = inner.store.snapshot_latest();
    let entries: Vec<SnapshotEntry> = latest
        .into_iter()
        .map(|(key, value, _ts)| SnapshotEntry { key, value })
        .collect();

    snapshot::write_snapshot(&inner.data_dir, inner.next_ts, &entries)
        .map_err(|e| DbError::Io(format!("snapshot write failed: {}", e)))?;

    inner
        .wal
        .truncate()
        .map_err(|e| DbError::Io(format!("WAL truncate failed: {}", e)))?;
    inner.wal_bytes_since_checkpoint = 0;
    inner.checkpoint_count = inner.checkpoint_count.saturating_add(1);
    Ok(())
}

fn statement_snapshot_ts(inner: &DatabaseInner, txn_id: Option<u64>) -> DbResult<u64> {
    match txn_id {
        Some(txn_id) => inner
            .active
            .get(&txn_id)
            .map(|txn| txn.start_ts)
            .ok_or(DbError::NoSuchTxn(txn_id)),
        None => Ok(inner.next_ts.saturating_sub(1)),
    }
}

fn table_key_bounds(table: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix_bytes = format!("{}\x00", table).into_bytes();
    let mut end_bytes = prefix_bytes.clone();
    if let Some(last) = end_bytes.last_mut() {
        *last = last.wrapping_add(1);
    }
    (prefix_bytes, end_bytes)
}

pub enum SqlResult {
    Query {
        columns: Vec<String>,
        column_types: Vec<crate::query::expr::ValueType>,
        rows: Vec<Row>,
    },
    Execute(u64),
    Begin(u64),
    Commit,
    Rollback,
}

// ─── SQL preprocessing ───────────────────────────────────────────────────────

/// Strip SQL `--` line comments outside of string literals.
/// Handles cartograph's migration runner output: lines like
/// `-- Migration 001\nCREATE TABLE ...` parse as bare `CREATE TABLE`.
pub(crate) fn strip_sql_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'\'' {
                    if i < bytes.len() && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                out.push(bytes[i] as char);
                i += 1;
            }
            if i < bytes.len() {
                out.push('"');
                i += 1;
            }
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Skip until newline.
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    out
}

// ─── Index key helpers ───────────────────────────────────────────────────────

fn make_index_entry_key(index: &IndexDef, schema: &Schema, row: &Row) -> Vec<u8> {
    let mut key = format!("__idx__\x00{}\x00", index.name).into_bytes();
    for col_name in &index.columns {
        let val = row.get(col_name).unwrap_or(&Value::Null);
        key.extend_from_slice(&val.encode());
    }
    key.push(0x00);
    let pk_val = row.get(&schema.columns[0].name).unwrap_or(&Value::Null);
    key.extend_from_slice(&pk_val.encode());
    key
}

fn make_index_column_prefix(index: &IndexDef, row: &Row) -> Vec<u8> {
    let mut prefix = format!("__idx__\x00{}\x00", index.name).into_bytes();
    for col_name in &index.columns {
        let val = row.get(col_name).unwrap_or(&Value::Null);
        prefix.extend_from_slice(&val.encode());
    }
    prefix.push(0x00);
    prefix
}

fn encode_index_column_values(columns: &[String], row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();
    for col_name in columns {
        let val = row.get(col_name).unwrap_or(&Value::Null);
        buf.extend_from_slice(&val.encode());
    }
    buf
}

// ─── WAL encoding ────────────────────────────────────────────────────────────

fn encode_commit_record(
    txn_id: u64,
    commit_ts: u64,
    write_set: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> Vec<u8> {
    let mut data = Vec::new();
    data.push(WAL_TXN_COMMIT);
    data.extend_from_slice(&txn_id.to_le_bytes());
    data.extend_from_slice(&commit_ts.to_le_bytes());
    data.extend_from_slice(&(write_set.len() as u32).to_le_bytes());
    for (key, value) in write_set {
        match value {
            Some(v) => {
                data.push(0);
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
            None => {
                data.push(1);
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
            }
        }
    }
    data
}

fn decode_commit_record(data: &[u8]) -> Option<(u64, u64, CommitWrites)> {
    if data.is_empty() || data[0] != WAL_TXN_COMMIT {
        return None;
    }
    let mut pos = 1;
    let txn_id = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
    pos += 8;
    let commit_ts = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
    pos += 8;
    let write_count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let mut writes = Vec::with_capacity(write_count);
    for _ in 0..write_count {
        let is_delete = *data.get(pos)?;
        pos += 1;
        let key_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;
        let key = data.get(pos..pos + key_len)?.to_vec();
        pos += key_len;
        if is_delete == 0 {
            let val_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
            pos += 4;
            let value = data.get(pos..pos + val_len)?.to_vec();
            pos += val_len;
            writes.push((key, Some(value)));
        } else {
            writes.push((key, None));
        }
    }
    Some((txn_id, commit_ts, writes))
}

fn encode_ddl_record(commit_ts: u64, ddl_op: &DdlOp) -> Vec<u8> {
    let mut data = Vec::new();
    data.push(WAL_DDL);
    data.extend_from_slice(&commit_ts.to_le_bytes());
    match ddl_op {
        DdlOp::CreateTable(schema) => {
            data.push(1);
            let cat_key = format!("__catalog__\x00{}", schema.table);
            let key_bytes = cat_key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);
            data.extend_from_slice(&catalog::encode_schema_public(schema));
        }
        DdlOp::DropTable(name) => {
            data.push(2);
            let cat_key = format!("__catalog__\x00{}", name);
            let key_bytes = cat_key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);
        }
        DdlOp::CreateIndex(index_def) => {
            data.push(3);
            let idx_key = format!("__index__\x00{}", index_def.name);
            let key_bytes = idx_key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);
            data.extend_from_slice(&catalog::encode_index_public(index_def));
        }
        DdlOp::DropIndex(name) => {
            data.push(4);
            let idx_key = format!("__index__\x00{}", name);
            let key_bytes = idx_key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);
        }
    }
    data
}

fn decode_ddl_record(data: &[u8]) -> Option<(u64, Vec<u8>)> {
    if data.is_empty() || data[0] != WAL_DDL {
        return None;
    }
    let commit_ts = u64::from_le_bytes(data.get(1..9)?.try_into().ok()?);
    Some((commit_ts, data.get(9..)?.to_vec()))
}

// ─── SQL helpers ─────────────────────────────────────────────────────────────

/// Coerce a value to the target column's declared type when there's a clear path.
/// For VECTOR columns: parse `Value::Text("[...]")` into `Value::Vector` and
/// validate the dim. Other types pass through unchanged.
fn coerce_value_for_column(
    value: Value,
    col_name: &str,
    schema: &crate::query::expr::Schema,
) -> DbResult<Value> {
    use crate::query::expr::{ValueType, parse_vector_str};
    let col = match schema.columns.iter().find(|c| c.name == col_name) {
        Some(c) => c,
        None => return Ok(value),
    };
    match (col.col_type, value) {
        (ValueType::Vector { dim }, Value::Text(s)) => {
            let v = parse_vector_str(&s).ok_or_else(|| {
                DbError::Sql(format!(
                    "invalid vector literal for column '{}': '{}'",
                    col_name, s
                ))
            })?;
            check_vector_dim(&v, dim, col_name)?;
            Ok(Value::Vector(v))
        }
        (ValueType::Vector { dim }, Value::Vector(v)) => {
            check_vector_dim(&v, dim, col_name)?;
            Ok(Value::Vector(v))
        }
        (_, v) => Ok(v),
    }
}

fn check_vector_dim(v: &[f32], expected: u32, col_name: &str) -> DbResult<()> {
    if expected == 0 {
        return Ok(());
    }
    if v.len() as u32 != expected {
        return Err(DbError::Constraint(format!(
            "vector dimension mismatch for column '{}': expected {}, got {}",
            col_name,
            expected,
            v.len()
        )));
    }
    Ok(())
}

fn is_vector_type_name(name: &sqlparser::ast::ObjectName) -> bool {
    let n = name.to_string().to_uppercase();
    n == "VECTOR" || n == "EMBEDDING"
}

fn sql_type_to_value_type(dt: &sqlparser::ast::DataType) -> crate::query::expr::ValueType {
    use crate::query::expr::ValueType;
    use sqlparser::ast::DataType;
    match dt {
        DataType::Boolean | DataType::Bool => ValueType::Bool,
        DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::TinyInt(_)
        | DataType::Int2(_)
        | DataType::Int4(_)
        | DataType::Int8(_) => ValueType::Int64,
        DataType::Float(_)
        | DataType::Double
        | DataType::DoublePrecision
        | DataType::Real
        | DataType::Float4
        | DataType::Float8 => ValueType::Float64,
        DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => ValueType::Decimal,
        DataType::Timestamp(_, _) => ValueType::Timestamp,
        DataType::Date => ValueType::Date,
        DataType::Uuid => ValueType::Uuid,
        DataType::Bytea | DataType::Blob(_) | DataType::Binary(_) | DataType::Varbinary(_) => {
            ValueType::Bytes
        }
        DataType::Custom(name, modifiers) => {
            let n = name.to_string().to_uppercase();
            if n == "VECTOR" || n == "EMBEDDING" {
                let dim = modifiers
                    .first()
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                return ValueType::Vector { dim };
            }
            ValueType::Text
        }
        _ => ValueType::Text,
    }
}

fn sql_expr_to_value(expr: &sqlparser::ast::Expr) -> DbResult<Value> {
    use crate::query::expr::{
        parse_date_str, parse_decimal_str, parse_timestamp_str, parse_uuid_str, parse_vector_str,
    };
    use sqlparser::ast::{DataType, Expr, UnaryOperator, Value as SqlValue};
    match expr {
        Expr::Value(SqlValue::Number(s, _)) => {
            if let Ok(n) = s.parse::<i64>() {
                Ok(Value::Int64(n))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Err(DbError::Sql(format!("invalid number: {}", s)))
            }
        }
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(Value::Text(s.clone())),
        Expr::Value(SqlValue::DoubleQuotedString(s)) => Ok(Value::Text(s.clone())),
        Expr::Value(SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
        Expr::Value(SqlValue::Null) => Ok(Value::Null),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match sql_expr_to_value(expr)? {
            Value::Int64(n) => Ok(Value::Int64(-n)),
            Value::Float64(f) => Ok(Value::Float64(-f)),
            Value::Decimal(v, s) => Ok(Value::Decimal(-v, s)),
            other => Err(DbError::Sql(format!("cannot negate {:?}", other))),
        },
        Expr::TypedString { data_type, value } => match data_type {
            DataType::Timestamp(_, _) => {
                let us = parse_timestamp_str(value)
                    .ok_or_else(|| DbError::Sql(format!("invalid timestamp: '{}'", value)))?;
                Ok(Value::Timestamp(us))
            }
            DataType::Date => {
                let days = parse_date_str(value)
                    .ok_or_else(|| DbError::Sql(format!("invalid date: '{}'", value)))?;
                Ok(Value::Date(days))
            }
            DataType::Uuid => {
                let bytes = parse_uuid_str(value)
                    .ok_or_else(|| DbError::Sql(format!("invalid UUID: '{}'", value)))?;
                Ok(Value::Uuid(bytes))
            }
            DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => {
                let (val, scale) = parse_decimal_str(value)
                    .ok_or_else(|| DbError::Sql(format!("invalid decimal: '{}'", value)))?;
                Ok(Value::Decimal(val, scale))
            }
            DataType::Custom(name, _) if is_vector_type_name(name) => {
                let v = parse_vector_str(value)
                    .ok_or_else(|| DbError::Sql(format!("invalid vector literal: '{}'", value)))?;
                Ok(Value::Vector(v))
            }
            _ => Ok(Value::Text(value.clone())),
        },
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner_val = sql_expr_to_value(expr)?;
            match data_type {
                DataType::Uuid => {
                    if let Value::Text(s) = &inner_val {
                        let bytes = parse_uuid_str(s)
                            .ok_or_else(|| DbError::Sql(format!("invalid UUID: '{}'", s)))?;
                        Ok(Value::Uuid(bytes))
                    } else {
                        Err(DbError::Sql("CAST to UUID requires a text value".into()))
                    }
                }
                DataType::Timestamp(_, _) => {
                    if let Value::Text(s) = &inner_val {
                        let us = parse_timestamp_str(s)
                            .ok_or_else(|| DbError::Sql(format!("invalid timestamp: '{}'", s)))?;
                        Ok(Value::Timestamp(us))
                    } else {
                        Err(DbError::Sql(
                            "CAST to TIMESTAMP requires a text value".into(),
                        ))
                    }
                }
                DataType::Date => {
                    if let Value::Text(s) = &inner_val {
                        let days = parse_date_str(s)
                            .ok_or_else(|| DbError::Sql(format!("invalid date: '{}'", s)))?;
                        Ok(Value::Date(days))
                    } else {
                        Err(DbError::Sql("CAST to DATE requires a text value".into()))
                    }
                }
                DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => {
                    match &inner_val {
                        Value::Text(s) => {
                            let (val, scale) = parse_decimal_str(s)
                                .ok_or_else(|| DbError::Sql(format!("invalid decimal: '{}'", s)))?;
                            Ok(Value::Decimal(val, scale))
                        }
                        Value::Int64(n) => Ok(Value::Decimal(*n as i128, 0)),
                        Value::Float64(f) => {
                            // Convert to string and parse to preserve precision.
                            let s = format!("{}", f);
                            let (val, scale) = parse_decimal_str(&s)
                                .ok_or_else(|| DbError::Sql(format!("invalid decimal: '{}'", s)))?;
                            Ok(Value::Decimal(val, scale))
                        }
                        _ => Err(DbError::Sql("unsupported CAST to DECIMAL".into())),
                    }
                }
                DataType::Custom(name, _) if is_vector_type_name(name) => {
                    if let Value::Text(s) = &inner_val {
                        let v = parse_vector_str(s).ok_or_else(|| {
                            DbError::Sql(format!("invalid vector literal: '{}'", s))
                        })?;
                        Ok(Value::Vector(v))
                    } else {
                        Err(DbError::Sql("CAST to VECTOR requires a text value".into()))
                    }
                }
                _ => Ok(inner_val),
            }
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            use sqlparser::ast::FunctionArguments;
            match name.as_str() {
                "COALESCE" => {
                    if let FunctionArguments::List(list) = &func.args {
                        for arg in &list.args {
                            if let sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) = arg
                            {
                                let val = sql_expr_to_value(e)?;
                                if !val.is_null() {
                                    return Ok(val);
                                }
                            }
                        }
                    }
                    Ok(Value::Null)
                }
                "NOW" => {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let us = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as i64;
                    Ok(Value::Timestamp(us))
                }
                _ => Err(DbError::Sql(format!("unsupported function: {}", name))),
            }
        }
        other => Err(DbError::Sql(format!(
            "unsupported value expression: {:?}",
            other
        ))),
    }
}

/// Get column types for a list of column names from a schema.
fn column_types_from_schema(
    cols: &[String],
    schema: &crate::query::expr::Schema,
) -> Vec<crate::query::expr::ValueType> {
    cols.iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .find(|c| c.name == *name)
                .map(|c| c.col_type)
                .unwrap_or(crate::query::expr::ValueType::Text)
        })
        .collect()
}

/// Infer column types from the first result row (fallback when schema is unavailable).
fn column_types_from_rows(cols: &[String], rows: &[Row]) -> Vec<crate::query::expr::ValueType> {
    if let Some(first_row) = rows.first() {
        cols.iter()
            .map(|name| {
                first_row
                    .get(name)
                    .map(|v| v.value_type())
                    .unwrap_or(crate::query::expr::ValueType::Text)
            })
            .collect()
    } else {
        vec![crate::query::expr::ValueType::Text; cols.len()]
    }
}

/// Parse RETURNING clause items into column names.
/// For RETURNING *, returns all schema column names.
fn parse_returning_cols(
    items: &[sqlparser::ast::SelectItem],
    schema: &crate::query::expr::Schema,
) -> DbResult<Vec<String>> {
    let mut cols = Vec::new();
    for item in items {
        match item {
            sqlparser::ast::SelectItem::Wildcard(_) => {
                return Ok(schema.columns.iter().map(|c| c.name.clone()).collect());
            }
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                sqlparser::ast::Expr::Identifier(ident) => {
                    cols.push(ident.value.clone());
                }
                _ => {
                    return Err(DbError::Sql(
                        "RETURNING only supports column names and *".into(),
                    ));
                }
            },
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                cols.push(alias.value.clone());
            }
            _ => {
                return Err(DbError::Sql("unsupported RETURNING item".into()));
            }
        }
    }
    Ok(cols)
}

/// Project a row to only the RETURNING columns.
fn project_returning(
    row: &crate::query::expr::Row,
    cols: &[String],
    _schema: &crate::query::expr::Schema,
) -> crate::query::expr::Row {
    let mut result = BTreeMap::new();
    for col in cols {
        if let Some(val) = row.get(col) {
            result.insert(col.clone(), val.clone());
        } else {
            result.insert(col.clone(), Value::Null);
        }
    }
    result
}

/// Resolve an expression in UPDATE SET context, supporting column references against the current row.
fn resolve_update_value(
    expr: &sqlparser::ast::Expr,
    current_row: &crate::query::expr::Row,
) -> DbResult<Value> {
    use sqlparser::ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match expr {
        // Column reference — look up in current row
        Expr::Identifier(ident) => Ok(current_row
            .get(&ident.value)
            .cloned()
            .unwrap_or(Value::Null)),
        // Qualified column reference (table.column)
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let col = &parts[1].value;
            Ok(current_row.get(col).cloned().unwrap_or(Value::Null))
        }
        // Binary operations (e.g., buy_count + 1)
        Expr::BinaryOp { left, op, right } => {
            let left_val = resolve_update_value(left, current_row)?;
            let right_val = resolve_update_value(right, current_row)?;
            use crate::query::expr::Expr as QExpr;
            type BinFn = fn(QExpr, QExpr) -> QExpr;
            let arith_op: Option<BinFn> = match op {
                BinaryOperator::Plus => Some(QExpr::add),
                BinaryOperator::Minus => Some(QExpr::sub),
                BinaryOperator::Multiply => Some(QExpr::mul),
                BinaryOperator::Divide => Some(QExpr::div),
                BinaryOperator::Modulo => Some(QExpr::modulo),
                _ => None,
            };
            if let Some(make_expr) = arith_op {
                let dummy_row: crate::query::expr::Row = BTreeMap::new();
                let e = make_expr(QExpr::Lit(left_val), QExpr::Lit(right_val));
                Ok(e.eval(&dummy_row))
            } else {
                Err(DbError::Sql(format!(
                    "unsupported binary op in UPDATE SET: {:?}",
                    op
                )))
            }
        }
        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let val = resolve_update_value(inner, current_row)?;
            Ok(Value::Bool(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = resolve_update_value(inner, current_row)?;
            Ok(Value::Bool(!val.is_null()))
        }
        // CASE WHEN ... THEN ... ELSE ... END
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if operand.is_some() {
                return Err(DbError::Sql(
                    "CASE <operand> not supported in UPDATE SET".into(),
                ));
            }
            for (cond, result) in conditions.iter().zip(results.iter()) {
                let cond_val = resolve_update_value(cond, current_row)?;
                if cond_val == Value::Bool(true) {
                    return resolve_update_value(result, current_row);
                }
            }
            if let Some(else_expr) = else_result {
                resolve_update_value(else_expr, current_row)
            } else {
                Ok(Value::Null)
            }
        }
        // Functions (COALESCE, NOW)
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            match name.as_str() {
                "COALESCE" => {
                    if let FunctionArguments::List(list) = &func.args {
                        for arg in &list.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                                let val = resolve_update_value(e, current_row)?;
                                if !val.is_null() {
                                    return Ok(val);
                                }
                            }
                        }
                    }
                    Ok(Value::Null)
                }
                "NOW" => {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let us = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as i64;
                    Ok(Value::Timestamp(us))
                }
                _ => Err(DbError::Sql(format!(
                    "unsupported function in UPDATE SET: {}",
                    name
                ))),
            }
        }
        // Comparison operators for CASE WHEN conditions
        Expr::IsFalse(inner) => {
            let val = resolve_update_value(inner, current_row)?;
            Ok(Value::Bool(val == Value::Bool(false)))
        }
        Expr::IsTrue(inner) => {
            let val = resolve_update_value(inner, current_row)?;
            Ok(Value::Bool(val == Value::Bool(true)))
        }
        // Fall through to literal/cast handling
        _ => sql_expr_to_value(expr),
    }
}

/// Resolve a value expression, handling EXCLUDED.column references for ON CONFLICT DO UPDATE.
fn resolve_excluded_value(
    expr: &sqlparser::ast::Expr,
    new_row: &crate::query::expr::Row,
) -> DbResult<Value> {
    match expr {
        sqlparser::ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.to_uppercase();
            let col = &parts[1].value;
            if table == "EXCLUDED" {
                return Ok(new_row.get(col).cloned().unwrap_or(Value::Null));
            }
            sql_expr_to_value(expr)
        }
        _ => sql_expr_to_value(expr),
    }
}
