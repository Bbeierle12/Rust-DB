# Rust-DB: `ALTER TABLE ... ADD COLUMN` — design

**Date:** 2026-06-20
**Repo:** `Bbeierle12/Rust-DB` (crate `rust-dst-db`)
**Status:** Spec, pre-implementation

## Goal

Add `ALTER TABLE <table> ADD COLUMN <name> <type> [NOT NULL] [DEFAULT <const>]`
to the Rust-DB SQL engine. Existing on-disk rows must be **preserved** — after a
column is added, rows written before the ALTER read back with the new column set
to its `DEFAULT` (or `NULL`). The change must be durable (survives restart via
the existing WAL/DDL path) and is the first `ALTER` operation the engine supports.

Downstream motivation (not part of this spec): it lets `chore-loop-backend` store
per-person chore scope as real columns instead of side tables. That work is a
separate plan.

## Context (current engine)

- **SQL dispatch** — `Database::execute_sql` (`src/engine/mod.rs:460`) routes by
  uppercased keyword prefix: `CREATE TABLE` → `execute_create_table`,
  `DROP TABLE` → `execute_drop_table`, `CREATE INDEX`, `INSERT`, `UPDATE`,
  `DELETE`. There is **no `ALTER` branch** today, so `ALTER …` falls through to
  the generic statement path and errors as unsupported.
  `execute_sql_in_txn` (`:497`) routes `CREATE`/`DROP` to a DDL path; no ALTER.
- **DDL model** — DDL is staged then committed transactionally. `execute_create_table`
  (`:528`) parses with `sqlparser` (`GenericDialect`), builds a `Schema` of
  `crate::query::expr::Column`s (mapping each `ColumnDef` via
  `sql_type_to_value_type`, honoring `NotNull` and constant `Default` through
  `sql_expr_to_value`), begins a txn, pushes a `DdlOp` onto the txn's `ddl_ops`,
  and commits. On commit, the apply loop (`src/engine/mod.rs:397`) runs each
  `DdlOp` against the catalog; the commit is WAL-logged, so DDL replays on reopen.
- **`DdlOp` enum** (`src/engine/mod.rs:103`): `CreateTable(Schema)`,
  `DropTable(String)`, `CreateIndex(IndexDef)`, `DropIndex(String)`.
- **Catalog** (`src/engine/catalog.rs`) — schemas live in the MVCC store under
  `__catalog__\x00{table}`. `Catalog::create_table` (`:270`) errors if the table
  exists, else `encode_schema` + `store.write`. `Catalog::get_table` reads/decodes
  a `Schema`. The schema encoding is **v2** and already carries a per-column
  `default: Option<Value>` (`encode_schema`/`decode_schema`, `catalog.rs:29`+).
- **Row format** — `Schema::encode_row` (`src/query/expr.rs:594`) writes
  `[col_count: u32][value…]` in schema order. `Schema::decode_row` (`:605`) reads
  the count and **strictly rejects** any row whose stored `count != columns.len()`
  (returns `None`). This strictness is what would break old rows after an ADD
  COLUMN — and is the one behavior this design changes.
- `Column` (`expr.rs:538`) already supports `default` and `nullable`; JOINs are
  already supported in `src/query/sql.rs` (not relevant here).

## Approach — forward-compatible row decode (Option A, chosen)

ADD COLUMN updates the catalog schema (append the column) and **does not rewrite
existing rows**. `decode_row` is relaxed so a row with *fewer* stored values than
the current schema — exactly what a pre-ALTER row becomes — fills the missing
trailing columns from each column's `default` (or `Null`). New writes already
encode the full current schema, so they carry the column normally.

Rejected alternative — eager row rewrite (scan every row on ALTER, decode-old /
re-encode-new, keep `decode_row` strict): more code and a bulk WAL-heavy write
inside the DDL txn, for no functional gain over Option A. The count-prefixed row
format is already built for forward-compatible decode.

## Design

### 1. Dispatch

In `execute_sql` (`engine/mod.rs:460`), add before the generic fallthrough:

```
if upper.starts_with("ALTER TABLE") { return self.execute_alter_table(trimmed); }
```

In `execute_sql_in_txn` (`:497`), include `ALTER` in the DDL routing alongside
`CREATE`/`DROP` (`:511`) so ALTER works inside an explicit transaction too.

### 2. `execute_alter_table` handler

A new method mirroring `execute_create_table`:

- Parse with `Parser::parse_sql(&GenericDialect{}, sql)`; expect one
  `Statement::AlterTable { name, operations, .. }`.
- For each operation, support **only** `AlterTableOperation::AddColumn { column_def, .. }`.
  Map `column_def` → `crate::query::expr::Column` using the **same** logic
  `execute_create_table` uses (`sql_type_to_value_type`, `ColumnOption::NotNull`,
  constant `ColumnOption::Default` via `sql_expr_to_value`). Any other
  `AlterTableOperation` (DropColumn, RenameColumn, AlterColumn, …) →
  `DbError::Sql("unsupported ALTER operation: …")`.
- Begin a txn; under the lock, at the txn snapshot:
  - `Catalog::get_table(&store, &table, snapshot_ts)` is `None` →
    `DbError::NoSuchTable(table)`.
  - The column name already exists in that schema → `DbError::Sql("column '…'
    already exists")`.
  - Otherwise push `DdlOp::AddColumn { table, column }` for **each** ADD COLUMN op
    (a single `ALTER TABLE … ADD COLUMN a …, ADD COLUMN b …` stages multiple ops).
- Commit. Return `SqlResult::Execute(0)`.

Note: the duplicate/exists checks read the catalog at the txn snapshot for an
early, clear error; the authoritative apply (below) runs at commit. For the
single-writer embedded use this is sufficient and matches how `execute_create_table`
checks existence before staging.

### 3. `DdlOp::AddColumn` + apply

Extend the enum (`engine/mod.rs:103`):

```
AddColumn { table: String, column: crate::query::expr::Column },
```

In the apply loop (`engine/mod.rs:397`), add:

```
DdlOp::AddColumn { table, column } => {
    let _ = Catalog::add_column(&mut inner.store, table, column.clone(), commit_ts);
}
```

(matches the existing `let _ = Catalog::…` pattern; the commit that wraps this is
WAL-logged, so the schema change replays on reopen).

### 4. `Catalog::add_column`

New catalog method:

```
pub fn add_column(store, table_name, column, commit_ts) -> Result<(), String>:
    let key = catalog_key(table_name);
    let mut schema = get_table(store, table_name, commit_ts.saturating_sub(1))
        .ok_or("no such table")?;
    if schema.columns.iter().any(|c| c.name == column.name) {
        return Err("column already exists");
    }
    schema.columns.push(column);
    store.write(key, commit_ts, Some(encode_schema(&schema)));
    Ok(())
```

Reuses the existing v2 `encode_schema` (which already serializes per-column
defaults), so no schema-format change is needed.

### 5. Row decode — forward compatibility (the preserve mechanism)

Relax `Schema::decode_row` (`expr.rs:605`):

- Read the stored `count` (u32).
- If `count > self.columns.len()` → still return `None` (corruption / a dropped
  column, which this engine doesn't produce).
- Decode the first `count` values into the first `count` columns by position.
- For each remaining column `columns[count..]`, insert its `default` if present,
  else `Value::Null`.

`encode_row` is unchanged (always writes the full current schema). Result:
pre-ALTER rows (stored `count` < new `len`) read back with the added column
defaulted; post-ALTER rows round-trip exactly.

### 6. Errors (all loud, never silent)

- Unknown table → `DbError::NoSuchTable`.
- Duplicate column → `DbError::Sql("column '…' already exists")`.
- Unsupported ALTER operation (anything but ADD COLUMN) → `DbError::Sql`.
- Parse failure → existing `DbError::Sql("parse error: …")`.

## Durability

`ALTER` goes through the same txn → `ddl_ops` → commit path as `CREATE TABLE`,
and commits are WAL-logged and replayed on `Database::open`. No new persistence
mechanism: the updated schema is a normal versioned catalog write at `commit_ts`.
Existing rows are untouched on disk and remain valid under the relaxed decode.

## Testing / acceptance criteria

Engine tests (Rust-DB `tests/` and/or `#[cfg(test)]` using the in-process
`Database::open` + `execute_sql` harness):

1. **Add to populated table, old rows preserved:** create a table, insert rows,
   `ALTER TABLE t ADD COLUMN c BIGINT`, then `SELECT` → existing rows show `NULL`
   for `c`; all prior columns unchanged.
2. **DEFAULT applied to old rows:** `ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'x'`
   → pre-existing rows read `c = 'x'`.
3. **New writes carry the column:** after the ALTER, `INSERT` a row specifying `c`
   → reads back the inserted value; an `INSERT` omitting `c` uses its default/NULL.
4. **Persists across reopen:** ALTER, drop the `Database`, `open` the same path →
   schema has the new column and rows (old + new) read correctly (WAL replay).
5. **Multiple ADD COLUMN in one statement** stage and apply both.
6. **Errors:** ALTER on an unknown table → `NoSuchTable`; adding an existing
   column name → error; `ALTER TABLE t DROP COLUMN c` → unsupported-op error.
7. **Round-trip unit test** for the relaxed `decode_row`: a buffer with `count < len`
   pads with defaults; `count == len` unchanged; `count > len` returns `None`.

## Out of scope

- `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN TYPE`, add/drop constraints,
  rename table — future ALTER operations (the dispatch + handler make them easy to
  add later, but YAGNI now).
- Non-constant column defaults (`now()`, `gen_random_uuid()`) — already ignored by
  the shared `CreateTable` default logic; same behavior here.
- Eager row rewrite / backfill jobs.
- Any `chore-loop-backend` change to use the new columns (separate plan).
- `JOIN` (already supported).
