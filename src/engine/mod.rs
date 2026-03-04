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
pub mod wal;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::query::executor::execute;
use crate::query::expr::{Row, Schema, Value};
use crate::query::sql::{sql_to_plan, SqlError};
use crate::txn::conflict::{self, ValidationResult};
use crate::txn::mvcc::MvccStore;

use self::catalog::Catalog;
use self::wal::FileWal;

/// WAL record type tag for transaction commits.
const WAL_TXN_COMMIT: u8 = 10;
/// WAL record type tag for DDL (CREATE TABLE, DROP TABLE).
const WAL_DDL: u8 = 20;

/// Errors from the database engine.
#[derive(Debug, Clone)]
pub enum DbError {
    /// Transaction not found.
    NoSuchTxn(u64),
    /// Write-write conflict.
    Conflict(String),
    /// SQL parse or plan error.
    Sql(String),
    /// Table not found.
    NoSuchTable(String),
    /// Table already exists.
    TableExists(String),
    /// I/O error.
    Io(String),
    /// Constraint violation.
    Constraint(String),
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

/// Result type for database operations.
pub type DbResult<T> = Result<T, DbError>;

/// An active transaction's state.
struct ActiveTxn {
    start_ts: u64,
    write_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// DDL operations to apply on commit.
    ddl_ops: Vec<DdlOp>,
}

enum DdlOp {
    CreateTable(Schema),
    DropTable(String),
}

/// The production database engine.
///
/// Thread-safe via internal `Mutex`. Clone the `Arc` to share across threads.
pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

struct DatabaseInner {
    store: MvccStore,
    wal: FileWal,
    active: BTreeMap<u64, ActiveTxn>,
    next_ts: u64,
    data_dir: PathBuf,
}

impl Database {
    /// Open or create a database at the given directory path.
    pub fn open(data_dir: impl Into<PathBuf>) -> DbResult<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.join("wal.log");
        let mut wal = FileWal::open(&wal_path)?;

        // Recover: replay WAL records into a fresh MvccStore.
        let mut store = MvccStore::new();
        let mut max_ts = 0u64;
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
                        // DDL records store schema data directly.
                        // Re-parse and apply to catalog.
                        if ddl_data.first() == Some(&1) {
                            // CreateTable: [1][schema_bytes]
                            let key_data = &ddl_data[1..];
                            // The DDL record stores the catalog key+value inline.
                            if let Some((key_len_bytes, rest)) = key_data.split_first_chunk::<4>() {
                                let key_len = u32::from_le_bytes(*key_len_bytes) as usize;
                                if rest.len() >= key_len {
                                    let key = rest[..key_len].to_vec();
                                    let value = rest[key_len..].to_vec();
                                    store.write(key, commit_ts, Some(value));
                                }
                            }
                        } else if ddl_data.first() == Some(&2) {
                            // DropTable: [2][key_len:u32][key]
                            let key_data = &ddl_data[1..];
                            if key_data.len() >= 4 {
                                let key_len = u32::from_le_bytes(
                                    key_data[..4].try_into().unwrap()
                                ) as usize;
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

        let inner = DatabaseInner {
            store,
            wal,
            active: BTreeMap::new(),
            next_ts: max_ts + 1,
            data_dir,
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Begin a new transaction. Returns the transaction ID.
    pub fn begin(&self) -> DbResult<u64> {
        let mut inner = self.inner.lock().unwrap();
        let txn_id = inner.next_ts;
        inner.next_ts += 1;

        inner.active.insert(txn_id, ActiveTxn {
            start_ts: txn_id,
            write_set: BTreeMap::new(),
            ddl_ops: Vec::new(),
        });

        Ok(txn_id)
    }

    /// Read a key within a transaction.
    pub fn get(&self, txn_id: u64, key: &[u8]) -> DbResult<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let txn = inner.active.get(&txn_id).ok_or(DbError::NoSuchTxn(txn_id))?;

        // Read-your-writes.
        if let Some(buffered) = txn.write_set.get(key) {
            return Ok(buffered.clone());
        }

        Ok(inner.store.read(key, txn.start_ts))
    }

    /// Buffer a write within a transaction.
    pub fn put(&self, txn_id: u64, key: Vec<u8>, value: Vec<u8>) -> DbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let txn = inner.active.get_mut(&txn_id).ok_or(DbError::NoSuchTxn(txn_id))?;
        txn.write_set.insert(key, Some(value));
        Ok(())
    }

    /// Buffer a delete within a transaction.
    pub fn delete(&self, txn_id: u64, key: Vec<u8>) -> DbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let txn = inner.active.get_mut(&txn_id).ok_or(DbError::NoSuchTxn(txn_id))?;
        txn.write_set.insert(key, None);
        Ok(())
    }

    /// Scan a key range within a transaction.
    pub fn scan(&self, txn_id: u64, start: Option<&[u8]>, end: Option<&[u8]>) -> DbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = self.inner.lock().unwrap();
        let txn = inner.active.get(&txn_id).ok_or(DbError::NoSuchTxn(txn_id))?;

        let mut entries = inner.store.scan(start, end, txn.start_ts);

        // Merge write set.
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for (k, v) in &entries {
            merged.insert(k.clone(), Some(v.clone()));
        }
        for (k, v) in &txn.write_set {
            if let Some(s) = start {
                if k.as_slice() < s { continue; }
            }
            if let Some(e) = end {
                if k.as_slice() >= e { continue; }
            }
            merged.insert(k.clone(), v.clone());
        }

        entries = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();

        Ok(entries)
    }

    /// Commit a transaction: OCC validate → WAL append+fsync → apply.
    pub fn commit(&self, txn_id: u64) -> DbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let txn = inner.active.remove(&txn_id).ok_or(DbError::NoSuchTxn(txn_id))?;

        if txn.write_set.is_empty() && txn.ddl_ops.is_empty() {
            // Read-only transaction — nothing to do.
            return Ok(());
        }

        // OCC validation.
        let validation = conflict::validate_write_set(&inner.store, &txn.write_set, txn.start_ts);
        match validation {
            ValidationResult::Conflict { key } => {
                return Err(DbError::Conflict(format!(
                    "write-write conflict on key {:?}", key
                )));
            }
            ValidationResult::Ok => {}
        }

        let commit_ts = inner.next_ts;
        inner.next_ts += 1;

        // WAL: log the commit record.
        if !txn.write_set.is_empty() {
            let wal_data = encode_commit_record(txn_id, commit_ts, &txn.write_set);
            inner.wal.append_sync(&wal_data)
                .map_err(|e| DbError::Io(format!("WAL write failed: {}", e)))?;
        }

        // WAL: log DDL operations.
        for ddl_op in &txn.ddl_ops {
            let wal_data = encode_ddl_record(commit_ts, ddl_op);
            inner.wal.append_sync(&wal_data)
                .map_err(|e| DbError::Io(format!("WAL DDL write failed: {}", e)))?;
        }

        // Apply writes to MvccStore.
        for (key, value) in &txn.write_set {
            inner.store.write(key.clone(), commit_ts, value.clone());
        }

        // Apply DDL operations.
        for ddl_op in &txn.ddl_ops {
            match ddl_op {
                DdlOp::CreateTable(schema) => {
                    let _ = Catalog::create_table(&mut inner.store, schema, commit_ts);
                }
                DdlOp::DropTable(name) => {
                    let _ = Catalog::drop_table(&mut inner.store, name, commit_ts);
                }
            }
        }

        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&self, txn_id: u64) -> DbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.active.remove(&txn_id);
        Ok(())
    }

    /// Execute a SQL statement. Returns (column_names, rows) for queries,
    /// or a count of affected rows for DML/DDL.
    pub fn execute_sql(&self, sql: &str) -> DbResult<SqlResult> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        // Transaction control.
        if upper == "BEGIN" {
            let txn_id = self.begin()?;
            return Ok(SqlResult::Begin(txn_id));
        }
        if upper == "COMMIT" {
            return Ok(SqlResult::Commit);
        }
        if upper == "ROLLBACK" {
            return Ok(SqlResult::Rollback);
        }

        // DDL: CREATE TABLE.
        if upper.starts_with("CREATE TABLE") {
            return self.execute_create_table(trimmed);
        }

        // DDL: DROP TABLE.
        if upper.starts_with("DROP TABLE") {
            return self.execute_drop_table(trimmed);
        }

        // DML: INSERT.
        if upper.starts_with("INSERT") {
            return self.execute_insert(trimmed);
        }

        // DML: UPDATE.
        if upper.starts_with("UPDATE") {
            return self.execute_update(trimmed);
        }

        // DML: DELETE.
        if upper.starts_with("DELETE") {
            return self.execute_delete(trimmed);
        }

        // SELECT.
        self.execute_select(trimmed)
    }

    fn execute_create_table(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
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
                    let mut col = crate::query::expr::Column::new(
                        col_def.name.value.clone(),
                        col_type,
                    );

                    // Check for NOT NULL constraint.
                    for opt in &col_def.options {
                        if matches!(opt.option, sqlparser::ast::ColumnOption::NotNull) {
                            col = col.not_null();
                        }
                    }

                    columns.push(col);
                }

                let schema = Schema::new(table_name, columns);

                // Use an auto-commit transaction for DDL.
                let txn_id = self.begin()?;
                {
                    let mut inner = self.inner.lock().unwrap();
                    let snapshot_ts = inner.active.get(&txn_id).unwrap().start_ts;

                    // Check if table exists.
                    if Catalog::get_table(&inner.store, &schema.table, snapshot_ts).is_some() {
                        inner.active.remove(&txn_id);
                        return Err(DbError::TableExists(schema.table.clone()));
                    }

                    let txn = inner.active.get_mut(&txn_id).unwrap();
                    txn.ddl_ops.push(DdlOp::CreateTable(schema));
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

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;

        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }

        match &stmts[0] {
            sqlparser::ast::Statement::Drop { names, .. } => {
                let table_name = names.first()
                    .ok_or_else(|| DbError::Sql("expected table name".into()))?
                    .to_string();

                let txn_id = self.begin()?;
                {
                    let mut inner = self.inner.lock().unwrap();
                    let snapshot_ts = inner.active.get(&txn_id).unwrap().start_ts;

                    if Catalog::get_table(&inner.store, &table_name, snapshot_ts).is_none() {
                        inner.active.remove(&txn_id);
                        return Err(DbError::NoSuchTable(table_name));
                    }

                    let txn = inner.active.get_mut(&txn_id).unwrap();
                    txn.ddl_ops.push(DdlOp::DropTable(table_name));
                }
                self.commit(txn_id)?;

                Ok(SqlResult::Execute(0))
            }
            _ => Err(DbError::Sql("expected DROP TABLE".into())),
        }
    }

    fn execute_insert(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        use sqlparser::ast::{SetExpr, Statement};

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;

        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }

        match &stmts[0] {
            Statement::Insert(insert) => {
                let table_name = insert.table_name.to_string();

                let inner = self.inner.lock().unwrap();
                let snapshot_ts = inner.next_ts.saturating_sub(1);
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                drop(inner);

                // Extract column names (if specified) or use schema order.
                let target_cols: Vec<String> = if insert.columns.is_empty() {
                    schema.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    insert.columns.iter().map(|c| c.value.clone()).collect()
                };

                // Extract rows from VALUES clause.
                let body = insert.source.as_ref()
                    .ok_or_else(|| DbError::Sql("INSERT requires VALUES".into()))?;
                let value_rows = match body.body.as_ref() {
                    SetExpr::Values(values) => &values.rows,
                    _ => return Err(DbError::Sql("expected VALUES clause".into())),
                };

                let txn_id = self.begin()?;
                let mut count = 0u64;

                for value_row in value_rows {
                    if value_row.len() != target_cols.len() {
                        self.abort(txn_id)?;
                        return Err(DbError::Sql(format!(
                            "expected {} values, got {}",
                            target_cols.len(),
                            value_row.len()
                        )));
                    }

                    let mut row = Row::new();
                    for (col_name, expr) in target_cols.iter().zip(value_row.iter()) {
                        let value = sql_expr_to_value(expr)?;
                        row.insert(col_name.clone(), value);
                    }

                    // Check NOT NULL constraints.
                    for col in &schema.columns {
                        if !col.nullable {
                            let val = row.get(&col.name).unwrap_or(&Value::Null);
                            if val.is_null() {
                                self.abort(txn_id)?;
                                return Err(DbError::Constraint(format!(
                                    "column '{}' cannot be null", col.name
                                )));
                            }
                        }
                    }

                    // Build storage key (first column is primary key).
                    let pk_val = row.get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);
                    let value = schema.encode_row(&row);

                    self.put(txn_id, key, value)?;
                    count += 1;
                }

                self.commit(txn_id)?;
                Ok(SqlResult::Execute(count))
            }
            _ => Err(DbError::Sql("expected INSERT".into())),
        }
    }

    fn execute_update(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        use sqlparser::ast::Statement;

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;

        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }

        match &stmts[0] {
            Statement::Update { table, assignments, selection, .. } => {
                let table_name = table.relation.to_string();

                let inner = self.inner.lock().unwrap();
                let snapshot_ts = inner.next_ts.saturating_sub(1);
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                drop(inner);

                // Fetch all rows.
                let rows = self.scan_table_rows(&schema)?;

                // Build WHERE filter.
                let predicate = match selection {
                    Some(expr) => {
                        Some(crate::query::sql::sql_to_plan(
                            &format!("SELECT * FROM {} WHERE {}", table_name, expr),
                            schema.clone(),
                        )?)
                    }
                    None => None,
                };

                let txn_id = self.begin()?;
                let mut count = 0u64;

                for row in &rows {
                    // Apply filter.
                    if let Some(ref plan) = predicate {
                        let result = execute(plan, vec![row.clone()]);
                        if result.is_empty() {
                            continue;
                        }
                    }

                    let mut updated_row = row.clone();
                    for assignment in assignments {
                        let col_name = match &assignment.target {
                            sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                            sqlparser::ast::AssignmentTarget::Tuple(names) => {
                                names.first().map(|n| n.to_string()).unwrap_or_default()
                            }
                        };
                        let value = sql_expr_to_value(&assignment.value)?;
                        updated_row.insert(col_name, value);
                    }

                    let pk_val = updated_row.get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);
                    let value = schema.encode_row(&updated_row);

                    self.put(txn_id, key, value)?;
                    count += 1;
                }

                self.commit(txn_id)?;
                Ok(SqlResult::Execute(count))
            }
            _ => Err(DbError::Sql("expected UPDATE".into())),
        }
    }

    fn execute_delete(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        use sqlparser::ast::Statement;

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)
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
                    from_tables.first()
                        .map(|f| f.relation.to_string())
                        .ok_or_else(|| DbError::Sql("DELETE requires FROM clause".into()))?
                };

                let inner = self.inner.lock().unwrap();
                let snapshot_ts = inner.next_ts.saturating_sub(1);
                let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
                    .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;
                drop(inner);

                let rows = self.scan_table_rows(&schema)?;

                let predicate = match &delete.selection {
                    Some(expr) => {
                        Some(crate::query::sql::sql_to_plan(
                            &format!("SELECT * FROM {} WHERE {}", table_name, expr),
                            schema.clone(),
                        )?)
                    }
                    None => None,
                };

                let txn_id = self.begin()?;
                let mut count = 0u64;

                for row in &rows {
                    if let Some(ref plan) = predicate {
                        let result = execute(plan, vec![row.clone()]);
                        if result.is_empty() {
                            continue;
                        }
                    }

                    let pk_val = row.get(&schema.columns[0].name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    let key = schema.make_key(&pk_val);

                    self.delete(txn_id, key)?;
                    count += 1;
                }

                self.commit(txn_id)?;
                Ok(SqlResult::Execute(count))
            }
            _ => Err(DbError::Sql("expected DELETE".into())),
        }
    }

    fn execute_select(&self, sql: &str) -> DbResult<SqlResult> {
        let inner = self.inner.lock().unwrap();
        let snapshot_ts = inner.next_ts.saturating_sub(1);

        // Try to extract table name from SQL to look up schema.
        let table_name = extract_table_name(sql)
            .ok_or_else(|| DbError::Sql("could not determine table name".into()))?;

        let schema = Catalog::get_table(&inner.store, &table_name, snapshot_ts)
            .ok_or_else(|| DbError::NoSuchTable(table_name.clone()))?;

        let plan = sql_to_plan(sql, schema.clone())?;

        // Scan table data.
        let table_prefix = format!("{}\x00", schema.table);
        let prefix_bytes = table_prefix.as_bytes();
        let mut end_bytes = prefix_bytes.to_vec();
        if let Some(last) = end_bytes.last_mut() {
            *last = last.wrapping_add(1);
        }

        let raw_entries = inner.store.scan(Some(prefix_bytes), Some(&end_bytes), snapshot_ts);
        let rows: Vec<Row> = raw_entries
            .iter()
            .filter_map(|(_, data)| schema.decode_row(data))
            .collect();

        drop(inner);

        let result_rows = execute(&plan, rows);

        let columns: Vec<String> = if result_rows.is_empty() {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            result_rows[0].keys().cloned().collect()
        };

        Ok(SqlResult::Query { columns, rows: result_rows })
    }

    /// Scan all decoded rows for a table.
    fn scan_table_rows(&self, schema: &Schema) -> DbResult<Vec<Row>> {
        let inner = self.inner.lock().unwrap();
        let snapshot_ts = inner.next_ts.saturating_sub(1);

        let table_prefix = format!("{}\x00", schema.table);
        let prefix_bytes = table_prefix.as_bytes();
        let mut end_bytes = prefix_bytes.to_vec();
        if let Some(last) = end_bytes.last_mut() {
            *last = last.wrapping_add(1);
        }

        let raw_entries = inner.store.scan(Some(prefix_bytes), Some(&end_bytes), snapshot_ts);
        Ok(raw_entries.iter().filter_map(|(_, data)| schema.decode_row(data)).collect())
    }

    /// Get a reference to the data directory.
    pub fn data_dir(&self) -> PathBuf {
        self.inner.lock().unwrap().data_dir.clone()
    }

    /// Get table count (for diagnostics).
    pub fn table_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        let ts = inner.next_ts.saturating_sub(1);
        Catalog::list_tables(&inner.store, ts).len()
    }

    /// Look up a table's schema by name.
    pub fn get_schema(&self, table_name: &str) -> Option<Schema> {
        let inner = self.inner.lock().unwrap();
        let ts = inner.next_ts.saturating_sub(1);
        Catalog::get_table(&inner.store, table_name, ts)
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Result of executing a SQL statement.
pub enum SqlResult {
    /// Query result with column names and rows.
    Query { columns: Vec<String>, rows: Vec<Row> },
    /// DML/DDL result with affected row count.
    Execute(u64),
    /// BEGIN was executed; returns txn ID.
    Begin(u64),
    /// COMMIT signal.
    Commit,
    /// ROLLBACK signal.
    Rollback,
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
                data.push(0); // put
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
            None => {
                data.push(1); // delete
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
            }
        }
    }

    data
}

fn decode_commit_record(
    data: &[u8],
) -> Option<(u64, u64, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
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
            data.push(1); // CreateTable tag
            let catalog_key = format!("__catalog__\x00{}", schema.table);
            let key_bytes = catalog_key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);

            let schema_data = catalog::encode_schema_public(schema);
            data.extend_from_slice(&schema_data);
        }
        DdlOp::DropTable(name) => {
            data.push(2); // DropTable tag
            let catalog_key = format!("__catalog__\x00{}", name);
            let key_bytes = catalog_key.as_bytes();
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
    let rest = data.get(9..)?.to_vec();
    Some((commit_ts, rest))
}

// ─── SQL helpers ─────────────────────────────────────────────────────────────

fn sql_type_to_value_type(dt: &sqlparser::ast::DataType) -> crate::query::expr::ValueType {
    use crate::query::expr::ValueType;
    use sqlparser::ast::DataType;

    match dt {
        DataType::Boolean | DataType::Bool => ValueType::Bool,
        DataType::SmallInt(_) | DataType::Int(_) | DataType::Integer(_)
        | DataType::BigInt(_) | DataType::TinyInt(_) | DataType::Int2(_)
        | DataType::Int4(_) | DataType::Int8(_) => ValueType::Int64,
        DataType::Float(_) | DataType::Double | DataType::DoublePrecision
        | DataType::Real | DataType::Float4 | DataType::Float8
        | DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => ValueType::Float64,
        DataType::Bytea | DataType::Blob(_) | DataType::Binary(_)
        | DataType::Varbinary(_) => ValueType::Bytes,
        // Default to Text for VARCHAR, TEXT, CHAR, etc.
        _ => ValueType::Text,
    }
}

fn sql_expr_to_value(expr: &sqlparser::ast::Expr) -> DbResult<Value> {
    use sqlparser::ast::{Expr, Value as SqlValue, UnaryOperator};
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
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => {
            match sql_expr_to_value(expr)? {
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(-f)),
                other => Err(DbError::Sql(format!("cannot negate {:?}", other))),
            }
        }
        other => Err(DbError::Sql(format!("unsupported value expression: {:?}", other))),
    }
}

/// Extract the table name from a SELECT query (simple heuristic).
fn extract_table_name(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let from_pos = upper.find("FROM")?;
    let after_from = &sql[from_pos + 4..].trim_start();

    // Take the first word after FROM.
    let table = after_from.split_whitespace().next()?;
    // Strip trailing semicolons, commas, etc.
    let table = table.trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_');
    if table.is_empty() {
        None
    } else {
        Some(table.to_string())
    }
}
