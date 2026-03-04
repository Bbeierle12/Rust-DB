//! Schema catalog — stores table definitions in the MVCC store.
//!
//! Schemas are stored under the key prefix `__catalog__\x00{table_name}`.
//! The value is a simple binary encoding of the Schema.

use crate::query::expr::{Column, Schema, ValueType};
use crate::txn::mvcc::MvccStore;

const CATALOG_PREFIX: &[u8] = b"__catalog__\x00";

/// Encode a Schema to bytes for storage.
fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();

    // Table name.
    let name_bytes = schema.table.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);

    // Column count.
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());

    for col in &schema.columns {
        // Column name.
        let col_bytes = col.name.as_bytes();
        buf.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(col_bytes);

        // Column type tag.
        let type_tag: u8 = match col.col_type {
            ValueType::Null => 0,
            ValueType::Bool => 1,
            ValueType::Int64 => 2,
            ValueType::Float64 => 3,
            ValueType::Text => 4,
            ValueType::Bytes => 5,
        };
        buf.push(type_tag);

        // Nullable.
        buf.push(col.nullable as u8);
    }

    buf
}

/// Decode a Schema from bytes.
fn decode_schema(data: &[u8]) -> Option<Schema> {
    let mut pos = 0;

    // Table name.
    let name_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let table = std::str::from_utf8(data.get(pos..pos + name_len)?).ok()?.to_string();
    pos += name_len;

    // Column count.
    let col_count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let col_name_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;
        let col_name = std::str::from_utf8(data.get(pos..pos + col_name_len)?).ok()?.to_string();
        pos += col_name_len;

        let type_tag = *data.get(pos)?;
        pos += 1;
        let col_type = match type_tag {
            0 => ValueType::Null,
            1 => ValueType::Bool,
            2 => ValueType::Int64,
            3 => ValueType::Float64,
            4 => ValueType::Text,
            5 => ValueType::Bytes,
            _ => return None,
        };

        let nullable = *data.get(pos)? != 0;
        pos += 1;

        let mut col = Column::new(col_name, col_type);
        if !nullable {
            col = col.not_null();
        }
        columns.push(col);
    }

    Some(Schema::new(table, columns))
}

/// Public wrapper for encoding a Schema (used by WAL DDL records).
pub fn encode_schema_public(schema: &Schema) -> Vec<u8> {
    encode_schema(schema)
}

/// Build the catalog key for a table name.
fn catalog_key(table_name: &str) -> Vec<u8> {
    let mut key = CATALOG_PREFIX.to_vec();
    key.extend_from_slice(table_name.as_bytes());
    key
}

/// Catalog operations — thin wrappers over the MvccStore.
pub struct Catalog;

impl Catalog {
    /// Register a table schema in the store.
    pub fn create_table(store: &mut MvccStore, schema: &Schema, commit_ts: u64) -> Result<(), String> {
        let key = catalog_key(&schema.table);

        // Check if table already exists.
        if store.read(&key, commit_ts.saturating_sub(1)).is_some() {
            return Err(format!("table '{}' already exists", schema.table));
        }

        let data = encode_schema(schema);
        store.write(key, commit_ts, Some(data));
        Ok(())
    }

    /// Look up a table schema by name.
    pub fn get_table(store: &MvccStore, table_name: &str, snapshot_ts: u64) -> Option<Schema> {
        let key = catalog_key(table_name);
        let data = store.read(&key, snapshot_ts)?;
        decode_schema(&data)
    }

    /// List all table schemas.
    pub fn list_tables(store: &MvccStore, snapshot_ts: u64) -> Vec<Schema> {
        let start = CATALOG_PREFIX.to_vec();
        let mut end = CATALOG_PREFIX.to_vec();
        // Increment last byte to get exclusive end bound.
        if let Some(last) = end.last_mut() {
            *last = last.wrapping_add(1);
        }

        let entries = store.scan(Some(&start), Some(&end), snapshot_ts);
        entries
            .into_iter()
            .filter_map(|(_, data)| decode_schema(&data))
            .collect()
    }

    /// Drop a table by writing a tombstone.
    pub fn drop_table(store: &mut MvccStore, table_name: &str, commit_ts: u64) -> Result<(), String> {
        let key = catalog_key(table_name);

        if store.read(&key, commit_ts.saturating_sub(1)).is_none() {
            return Err(format!("table '{}' does not exist", table_name));
        }

        store.write(key, commit_ts, None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrip() {
        let schema = Schema::new("users", vec![
            Column::new("id", ValueType::Int64).not_null(),
            Column::new("name", ValueType::Text),
            Column::new("active", ValueType::Bool),
        ]);

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        assert_eq!(schema, decoded);
    }

    #[test]
    fn catalog_create_and_get() {
        let mut store = MvccStore::new();
        let schema = Schema::new("users", vec![
            Column::new("id", ValueType::Int64).not_null(),
            Column::new("name", ValueType::Text),
        ]);

        Catalog::create_table(&mut store, &schema, 1).unwrap();

        let retrieved = Catalog::get_table(&store, "users", 1).unwrap();
        assert_eq!(schema, retrieved);
    }

    #[test]
    fn catalog_duplicate_table_error() {
        let mut store = MvccStore::new();
        let schema = Schema::new("users", vec![
            Column::new("id", ValueType::Int64),
        ]);

        Catalog::create_table(&mut store, &schema, 1).unwrap();
        let result = Catalog::create_table(&mut store, &schema, 2);
        assert!(result.is_err());
    }

    #[test]
    fn catalog_list_tables() {
        let mut store = MvccStore::new();

        let s1 = Schema::new("users", vec![Column::new("id", ValueType::Int64)]);
        let s2 = Schema::new("orders", vec![Column::new("id", ValueType::Int64)]);

        Catalog::create_table(&mut store, &s1, 1).unwrap();
        Catalog::create_table(&mut store, &s2, 2).unwrap();

        let tables = Catalog::list_tables(&store, 2);
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn catalog_drop_table() {
        let mut store = MvccStore::new();
        let schema = Schema::new("users", vec![Column::new("id", ValueType::Int64)]);

        Catalog::create_table(&mut store, &schema, 1).unwrap();
        assert!(Catalog::get_table(&store, "users", 1).is_some());

        Catalog::drop_table(&mut store, "users", 2).unwrap();
        assert!(Catalog::get_table(&store, "users", 2).is_none());
    }
}
