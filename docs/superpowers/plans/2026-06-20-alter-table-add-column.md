# `ALTER TABLE ADD COLUMN` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ALTER TABLE <t> ADD COLUMN <name> <type> [NOT NULL] [DEFAULT const]` to Rust-DB, preserving existing on-disk rows (they read the new column as its DEFAULT or NULL).

**Architecture:** Forward-compatible row decode + catalog append (spec Option A). `Schema::decode_row` pads rows that have fewer stored columns than the current schema; `ALTER` updates the catalog schema through the existing transactional, WAL-logged DDL path (`DdlOp` → commit). No row rewrite.

**Tech Stack:** Rust (edition 2024), `sqlparser` 0.53 (already parses ALTER), the in-process `Database`/`MvccStore`/`Catalog` engine. Tests: `cargo test` with `#[cfg(test)]` unit modules + `tests/engine_tests.rs` integration harness.

## Global Constraints

- **ADD COLUMN only.** Any other `AlterTableOperation` (DropColumn, RenameColumn, AlterColumn, …) → loud `DbError::Sql("unsupported ALTER operation: …")`. (verbatim from spec)
- **Preserve existing rows** via forward-compatible decode — never rewrite rows.
- **Durable** through the existing txn → `ddl_ops` → commit path (WAL-logged; replays on reopen). No new persistence mechanism.
- **Reuse** `CreateTable`'s column mapping: `sql_type_to_value_type`, `ColumnOption::NotNull`, constant `ColumnOption::Default` via `sql_expr_to_value` (only constant defaults captured; non-constant ignored).
- **v2 schema encoding unchanged** (`encode_schema` already serializes per-column defaults).
- Errors loud, never silent: unknown table → `DbError::NoSuchTable`; duplicate column → `DbError::Sql`.

---

## File Structure

| File | Change |
|---|---|
| `src/query/expr.rs` | Relax `Schema::decode_row` to pad short rows from column defaults/NULL; reject over-long rows. Unit test. |
| `src/engine/catalog.rs` | New `Catalog::add_column(store, table, column, commit_ts)`. Unit test. |
| `src/engine/mod.rs` | New `DdlOp::AddColumn { table, column }` variant + apply-loop arm; `execute_alter_table` handler; `ALTER TABLE` dispatch in `execute_sql` (and the in-txn DDL path). |
| `tests/engine_tests.rs` | Integration tests: preserve old rows, DEFAULT, reopen/replay, error cases. |

---

### Task 1: Forward-compatible `Schema::decode_row`

**Files:**
- Modify: `src/query/expr.rs` (the `decode_row` method, currently ~`:605`; add a `#[cfg(test)]` test)

**Interfaces:**
- Consumes: existing `Schema`, `Column { name, col_type, nullable, default: Option<Value> }`, `Value`, `Row = BTreeMap<String, Value>`.
- Produces: `decode_row` that accepts a stored row with `count <= columns.len()` (pads the trailing columns) and rejects `count > columns.len()`.

- [ ] **Step 1: Write the failing test** — add to the `#[cfg(test)] mod tests` in `src/query/expr.rs` (create the module if none exists):

```rust
#[test]
fn decode_row_pads_added_columns_with_default_or_null() {
    use std::collections::BTreeMap;
    // A row written under a 2-column schema.
    let old = Schema::new("t", vec![
        Column::new("id", ValueType::Int64),
        Column::new("name", ValueType::Text),
    ]);
    let mut r: BTreeMap<String, Value> = BTreeMap::new();
    r.insert("id".to_string(), Value::Int64(1));
    r.insert("name".to_string(), Value::Text("a".into()));
    let bytes = old.encode_row(&r);

    // Decoded under a 4-column schema: the two new trailing columns pad.
    let new = Schema::new("t", vec![
        Column::new("id", ValueType::Int64),
        Column::new("name", ValueType::Text),
        Column::new("assignee", ValueType::Int64),                              // no default -> NULL
        Column::new("flag", ValueType::Text).with_default(Value::Text("x".into())),
    ]);
    let decoded = new.decode_row(&bytes).expect("short row must decode");
    assert_eq!(decoded.get("id"), Some(&Value::Int64(1)));
    assert_eq!(decoded.get("name"), Some(&Value::Text("a".into())));
    assert_eq!(decoded.get("assignee"), Some(&Value::Null), "no default -> NULL");
    assert_eq!(decoded.get("flag"), Some(&Value::Text("x".into())), "default applied");

    // A row with MORE stored columns than the schema is rejected (corruption).
    let one = Schema::new("t", vec![Column::new("id", ValueType::Int64)]);
    assert!(one.decode_row(&bytes).is_none(), "count > len -> None");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p rust-dst-db decode_row_pads 2>$null`
Expected: FAIL — the current strict `if count != self.columns.len() { return None; }` rejects the short row, so `expect("short row must decode")` panics.

- [ ] **Step 3: Implement the relaxed decode** — replace the body of `decode_row`:

```rust
    /// Decode bytes back to a Row using this schema.
    ///
    /// Forward-compatible: a stored row may have FEWER columns than the current
    /// schema (it was written before an `ALTER TABLE ADD COLUMN`). The present
    /// values fill the leading columns; each remaining column is filled from its
    /// `default` (or `Null`). A row with MORE columns than the schema is rejected
    /// (corruption — this engine never drops columns).
    pub fn decode_row(&self, data: &[u8]) -> Option<Row> {
        let mut pos = 0;
        let count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;

        if count > self.columns.len() {
            return None;
        }

        let mut row = BTreeMap::new();
        for col in &self.columns[..count] {
            let (val, consumed) = Value::decode(&data[pos..])?;
            pos += consumed;
            row.insert(col.name.clone(), val);
        }
        for col in &self.columns[count..] {
            row.insert(col.name.clone(), col.default.clone().unwrap_or(Value::Null));
        }
        Some(row)
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p rust-dst-db 2>$null`
Expected: PASS — the new test plus all existing `expr` tests (existing rows where `count == len` still decode identically).

- [ ] **Step 5: Commit**

```bash
git add src/query/expr.rs
git -c user.name="Bbeierle12" -c user.email="bbeierle21@gmail.com" commit -m "feat(expr): forward-compatible decode_row (pad added columns)"
```

---

### Task 2: `Catalog::add_column` + `DdlOp::AddColumn`

**Files:**
- Modify: `src/engine/catalog.rs` (new `add_column` + unit test)
- Modify: `src/engine/mod.rs` (`DdlOp` enum variant + apply-loop arm)

**Interfaces:**
- Consumes: `MvccStore` (`read`/`write`), `Catalog::get_table`, `encode_schema`, `Column`, `Schema` (all in `catalog.rs`'s scope).
- Produces: `Catalog::add_column(store: &mut MvccStore, table_name: &str, column: Column, commit_ts: u64) -> Result<(), String>`; `DdlOp::AddColumn { table: String, column: crate::query::expr::Column }` applied in the commit loop.

- [ ] **Step 1: Write the failing test** — add to `#[cfg(test)] mod tests` in `src/engine/catalog.rs`:

```rust
#[test]
fn add_column_appends_and_rejects_dup_and_unknown() {
    let mut store = MvccStore::new();
    let schema = Schema::new("users", vec![
        Column::new("id", ValueType::Int64).not_null(),
        Column::new("name", ValueType::Text),
    ]);
    Catalog::create_table(&mut store, &schema, 1).unwrap();

    Catalog::add_column(&mut store, "users", Column::new("age", ValueType::Int64), 2).unwrap();
    let s = Catalog::get_table(&store, "users", 2).unwrap();
    assert_eq!(s.columns.len(), 3);
    assert_eq!(s.columns[2].name, "age");

    // Duplicate column name -> error.
    assert!(Catalog::add_column(&mut store, "users", Column::new("age", ValueType::Int64), 3).is_err());
    // Unknown table -> error.
    assert!(Catalog::add_column(&mut store, "nope", Column::new("x", ValueType::Int64), 4).is_err());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p rust-dst-db add_column_appends 2>$null`
Expected: FAIL — `no function or associated item named 'add_column' found for struct 'Catalog'`.

- [ ] **Step 3: Implement `Catalog::add_column`** — in `src/engine/catalog.rs`, inside `impl Catalog`, after `create_table`:

```rust
    /// Append a column to an existing table's schema. Preserves existing rows:
    /// only the catalog entry changes (old rows pad via `Schema::decode_row`).
    pub fn add_column(
        store: &mut MvccStore,
        table_name: &str,
        column: Column,
        commit_ts: u64,
    ) -> Result<(), String> {
        let mut schema = Self::get_table(store, table_name, commit_ts.saturating_sub(1))
            .ok_or_else(|| format!("table '{table_name}' not found"))?;
        if schema.columns.iter().any(|c| c.name == column.name) {
            return Err(format!("column '{}' already exists", column.name));
        }
        schema.columns.push(column);
        let key = catalog_key(table_name);
        store.write(key, commit_ts, Some(encode_schema(&schema)));
        Ok(())
    }
```

- [ ] **Step 4: Add the `DdlOp` variant + apply arm** — in `src/engine/mod.rs`, extend the `DdlOp` enum (~`:103`):

```rust
enum DdlOp {
    CreateTable(Schema),
    DropTable(String),
    CreateIndex(IndexDef),
    DropIndex(String),
    AddColumn { table: String, column: crate::query::expr::Column },
}
```

And in the commit apply loop (~`:397`, alongside the other `DdlOp` arms):

```rust
                DdlOp::AddColumn { table, column } => {
                    let _ = Catalog::add_column(&mut inner.store, table, column.clone(), commit_ts);
                }
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p rust-dst-db 2>$null`
Expected: PASS — `add_column_appends_and_rejects_dup_and_unknown` plus all existing tests. (The new `DdlOp` variant compiles; it has no constructor yet — Task 3 adds it — which is fine.)

- [ ] **Step 6: Commit**

```bash
git add src/engine/catalog.rs src/engine/mod.rs
git -c user.name="Bbeierle12" -c user.email="bbeierle21@gmail.com" commit -m "feat(catalog): Catalog::add_column + DdlOp::AddColumn apply"
```

---

### Task 3: `execute_alter_table` handler + dispatch + integration tests

**Files:**
- Modify: `src/engine/mod.rs` (dispatch in `execute_sql` ~`:460` and the in-txn DDL routing ~`:511`; new `execute_alter_table` method near `execute_create_table` ~`:528`)
- Modify: `tests/engine_tests.rs` (integration tests)

**Interfaces:**
- Consumes: `DdlOp::AddColumn` (Task 2), `sql_type_to_value_type`/`sql_expr_to_value` (existing in `engine/mod.rs`), `Catalog::get_table`, `self.begin/commit/lock_inner`, `crate::query::expr::Column`, `DbError`, `SqlResult`.
- Produces: `ALTER TABLE … ADD COLUMN …` executes end-to-end via `Database::execute_sql`.

- [ ] **Step 1: Write the failing integration tests** — add to `tests/engine_tests.rs`:

```rust
#[test]
fn alter_table_add_column_preserves_existing_rows() {
    let dir = tmp_dir("alter_add_col");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)").unwrap();
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute_sql("ALTER TABLE users ADD COLUMN age BIGINT").unwrap();

    // The pre-ALTER row reads NULL for the new column; prior columns intact.
    match db.execute_sql("SELECT id, name, age FROM users").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Text("Alice".into())));
            assert_eq!(rows[0].get("age"), Some(&Value::Null));
        }
        _ => panic!("expected query result"),
    }

    // A new insert carries the added column.
    db.execute_sql("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 40)").unwrap();
    match db.execute_sql("SELECT age FROM users WHERE id = 2").unwrap() {
        SqlResult::Query { rows, .. } => assert_eq!(rows[0].get("age"), Some(&Value::Int64(40))),
        _ => panic!("expected query result"),
    }
}

#[test]
fn alter_add_column_with_default_persists_across_reopen() {
    let dir = tmp_dir("alter_default_reopen");
    {
        let db = Database::open(&dir).unwrap();
        db.execute_sql("CREATE TABLE t (id BIGINT NOT NULL)").unwrap();
        db.execute_sql("INSERT INTO t (id) VALUES (1)").unwrap();
        db.execute_sql("ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'new'").unwrap();
    } // drop -> close
    let db = Database::open(&dir).unwrap(); // WAL replay restores the schema change
    match db.execute_sql("SELECT id, status FROM t").unwrap() {
        SqlResult::Query { rows, .. } => {
            assert_eq!(rows[0].get("status"), Some(&Value::Text("new".into())),
                "old row reads the column DEFAULT after reopen");
        }
        _ => panic!("expected query result"),
    }
}

#[test]
fn alter_table_error_cases() {
    let dir = tmp_dir("alter_errors");
    let db = Database::open(&dir).unwrap();
    db.execute_sql("CREATE TABLE t (id BIGINT, name TEXT)").unwrap();
    assert!(db.execute_sql("ALTER TABLE nope ADD COLUMN x BIGINT").is_err(), "unknown table");
    assert!(db.execute_sql("ALTER TABLE t ADD COLUMN name TEXT").is_err(), "duplicate column");
    assert!(db.execute_sql("ALTER TABLE t DROP COLUMN name").is_err(), "unsupported op");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p rust-dst-db --test engine_tests alter_ 2>$null`
Expected: FAIL — `ALTER` is not dispatched, so `execute_sql` returns an "unsupported statement" error and `.unwrap()` panics.

- [ ] **Step 3: Add dispatch** — in `execute_sql` (`src/engine/mod.rs` ~`:479`, with the other DDL prefixes), add:

```rust
        if upper.starts_with("ALTER TABLE") {
            return self.execute_alter_table(trimmed);
        }
```

In `execute_sql_in_txn` (~`:511`), extend the DDL prefix check so ALTER is routed there too:

```rust
        if upper.starts_with("CREATE") || upper.starts_with("DROP") || upper.starts_with("ALTER") {
```

(this branch already delegates DDL to the non-txn handlers, which begin/commit their own txn — same behavior CREATE/DROP have today).

- [ ] **Step 4: Add the `execute_alter_table` handler** — in `src/engine/mod.rs`, after `execute_create_table`:

```rust
    fn execute_alter_table(&self, sql: &str) -> DbResult<SqlResult> {
        use sqlparser::ast::{AlterTableOperation, ColumnOption, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| DbError::Sql(format!("parse error: {}", e)))?;
        if stmts.len() != 1 {
            return Err(DbError::Sql("expected one statement".into()));
        }
        let (table_name, operations) = match &stmts[0] {
            Statement::AlterTable { name, operations, .. } => (name.to_string(), operations),
            _ => return Err(DbError::Sql("expected ALTER TABLE".into())),
        };

        // Translate each ADD COLUMN op to a catalog Column (reuse CreateTable's mapping).
        let mut new_columns: Vec<crate::query::expr::Column> = Vec::new();
        for op in operations {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_type = sql_type_to_value_type(&column_def.data_type);
                    let mut col =
                        crate::query::expr::Column::new(column_def.name.value.clone(), col_type);
                    for opt in &column_def.options {
                        match &opt.option {
                            ColumnOption::NotNull => col = col.not_null(),
                            ColumnOption::Default(expr) => {
                                if let Ok(value) = sql_expr_to_value(expr) {
                                    if !value.is_null() {
                                        col = col.with_default(value);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    new_columns.push(col);
                }
                other => {
                    return Err(DbError::Sql(format!("unsupported ALTER operation: {:?}", other)));
                }
            }
        }
        if new_columns.is_empty() {
            return Err(DbError::Sql("ALTER TABLE requires at least one ADD COLUMN".into()));
        }

        let txn_id = self.begin()?;
        {
            let mut inner = self.lock_inner()?;
            let snapshot_ts = inner.active.get(&txn_id).unwrap().start_ts;
            let schema = match Catalog::get_table(&inner.store, &table_name, snapshot_ts) {
                Some(s) => s,
                None => {
                    inner.active.remove(&txn_id);
                    return Err(DbError::NoSuchTable(table_name));
                }
            };
            // Reject duplicates against the existing schema AND within this statement.
            let mut seen: std::collections::HashSet<String> =
                schema.columns.iter().map(|c| c.name.clone()).collect();
            for col in &new_columns {
                if !seen.insert(col.name.clone()) {
                    inner.active.remove(&txn_id);
                    return Err(DbError::Sql(format!("column '{}' already exists", col.name)));
                }
            }
            let txn = inner.active.get_mut(&txn_id).unwrap();
            for col in new_columns {
                txn.ddl_ops.push(DdlOp::AddColumn {
                    table: table_name.clone(),
                    column: col,
                });
            }
        }
        self.commit(txn_id)?;
        Ok(SqlResult::Execute(0))
    }
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p rust-dst-db 2>$null`
Expected: PASS — the three `alter_*` integration tests plus the full existing suite (unit + integration).

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs tests/engine_tests.rs
git -c user.name="Bbeierle12" -c user.email="bbeierle21@gmail.com" commit -m "feat(engine): ALTER TABLE ADD COLUMN (dispatch + handler + tests)"
```

---

## Self-Review

**Spec coverage:**
- Parse/dispatch `ALTER TABLE … ADD COLUMN` → Task 3 (dispatch + handler). ✓
- Reuse CreateTable column mapping (`sql_type_to_value_type`, NotNull, constant Default) → Task 3. ✓
- `DdlOp::AddColumn` + apply via `Catalog::add_column` → Task 2. ✓
- `Catalog::add_column` (load schema, dup-check, append, write back; v2 encoding) → Task 2. ✓
- Preserve mechanism — `decode_row` pads short rows from default/NULL, rejects over-long → Task 1. ✓
- Durability via existing txn→WAL DDL commit → Task 2 apply arm + Task 3 commit; verified by the reopen test (Task 3). ✓
- Errors (unknown table, duplicate column, unsupported op) → Task 3 handler + test. ✓
- Multiple ADD COLUMN in one statement → Task 3 loops `operations` (the within-statement dup check covers it). ✓
- Out-of-scope items (DROP/RENAME/type change, non-constant defaults, eager rewrite, JOIN) → none implemented. ✓

**Placeholder scan:** No TBD/TODO. Every code step shows complete code; every test step shows the assertions and the exact `cargo test` command + expected outcome.

**Type consistency:** `Catalog::add_column(store, table_name, column, commit_ts) -> Result<(), String>` is defined in Task 2 and constructed nowhere else; `DdlOp::AddColumn { table: String, column: crate::query::expr::Column }` is defined (Task 2) and constructed (Task 3) with matching field names/types; `decode_row` pad behavior (Task 1) is what the Task 3 reopen/NULL assertions rely on. `Value::Int64`/`Value::Text`/`Value::Null`, `Column::new/.not_null()/.with_default()`, `Schema::new`, `SqlResult::{Execute,Query{rows}}`, and `DbError::{Sql,NoSuchTable}` all match the existing engine API confirmed in `expr.rs`, `catalog.rs`, `engine/mod.rs`, and `tests/engine_tests.rs`.

**Note for the executor:** `cargo test` for this crate compiles the engine only (no openssl/web-push here), but run from a shell with the standard toolchain on PATH; suppress native stderr with `2>$null` per the PowerShell caveat. Default `cargo test` excludes the `server`/`python`/`pgwire` features — these changes are in the default library path, so the default test command exercises them.
