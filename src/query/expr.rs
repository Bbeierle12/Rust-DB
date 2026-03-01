use std::collections::BTreeMap;
use std::fmt;

/// Scalar value type used in query results and expressions.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int64(n) => write!(f, "{}", n),
            Value::Float64(n) => write!(f, "{}", n),
            Value::Text(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "0x{}", hex_encode(b)),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// The type tag of a Value, without the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Null,
    Bool,
    Int64,
    Float64,
    Text,
    Bytes,
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int64(_) => ValueType::Int64,
            Value::Float64(_) => ValueType::Float64,
            Value::Text(_) => ValueType::Text,
            Value::Bytes(_) => ValueType::Bytes,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Serialize a Value to bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Value::Null => buf.push(0),
            Value::Bool(b) => {
                buf.push(1);
                buf.push(*b as u8);
            }
            Value::Int64(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float64(n) => {
                buf.push(3);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Text(s) => {
                buf.push(4);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Bytes(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
        buf
    }

    /// Deserialize a Value from bytes. Returns (value, bytes_consumed).
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        let tag = *data.first()?;
        let mut pos = 1;
        match tag {
            0 => Some((Value::Null, pos)),
            1 => {
                let b = *data.get(pos)?;
                pos += 1;
                Some((Value::Bool(b != 0), pos))
            }
            2 => {
                let n = i64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
                pos += 8;
                Some((Value::Int64(n), pos))
            }
            3 => {
                let n = f64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
                pos += 8;
                Some((Value::Float64(n), pos))
            }
            4 => {
                let len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let s = std::str::from_utf8(data.get(pos..pos + len)?).ok()?.to_string();
                pos += len;
                Some((Value::Text(s), pos))
            }
            5 => {
                let len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let b = data.get(pos..pos + len)?.to_vec();
                pos += len;
                Some((Value::Bytes(b), pos))
            }
            _ => None,
        }
    }
}

/// A named row: column_name → value.
/// Uses BTreeMap for deterministic iteration order.
pub type Row = BTreeMap<String, Value>;

/// Column definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub col_type: ValueType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, col_type: ValueType) -> Self {
        Self {
            name: name.into(),
            col_type,
            nullable: true,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

/// Table schema: ordered list of columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub table: String,
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(table: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            table: table.into(),
            columns,
        }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Encode a row to bytes for storage in the KV store.
    /// Format: [col_count: u32][for each col: encoded value]
    pub fn encode_row(&self, row: &Row) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
            let val = row.get(&col.name).unwrap_or(&Value::Null);
            buf.extend_from_slice(&val.encode());
        }
        buf
    }

    /// Decode bytes back to a Row using this schema.
    pub fn decode_row(&self, data: &[u8]) -> Option<Row> {
        let mut pos = 0;
        let count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;

        if count != self.columns.len() {
            return None;
        }

        let mut row = BTreeMap::new();
        for col in &self.columns {
            let (val, consumed) = Value::decode(&data[pos..])?;
            pos += consumed;
            row.insert(col.name.clone(), val);
        }
        Some(row)
    }

    /// Build a storage key for a row: `{table}\x00{primary_key_value_encoded}`.
    /// Uses the first column as the primary key.
    pub fn make_key(&self, primary_key: &Value) -> Vec<u8> {
        let mut key = self.table.as_bytes().to_vec();
        key.push(0x00); // separator
        key.extend_from_slice(&primary_key.encode());
        key
    }
}

/// Query expression — used in filters, projections, and sort keys.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Reference to a column by name.
    Col(String),
    /// Literal value.
    Lit(Value),
    /// Equality: left = right.
    Eq(Box<Expr>, Box<Expr>),
    /// Less-than.
    Lt(Box<Expr>, Box<Expr>),
    /// Greater-than.
    Gt(Box<Expr>, Box<Expr>),
    /// Less-than-or-equal.
    Le(Box<Expr>, Box<Expr>),
    /// Greater-than-or-equal.
    Ge(Box<Expr>, Box<Expr>),
    /// Not-equal.
    Ne(Box<Expr>, Box<Expr>),
    /// Logical AND.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR.
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT.
    Not(Box<Expr>),
}

impl Expr {
    /// Evaluate the expression against a row, returning a Value.
    pub fn eval(&self, row: &Row) -> Value {
        match self {
            Expr::Col(name) => row.get(name).cloned().unwrap_or(Value::Null),
            Expr::Lit(v) => v.clone(),
            Expr::Eq(l, r) => Value::Bool(l.eval(row) == r.eval(row)),
            Expr::Lt(l, r) => {
                Value::Bool(partial_cmp(&l.eval(row), &r.eval(row)) == Some(std::cmp::Ordering::Less))
            }
            Expr::Gt(l, r) => Value::Bool(
                partial_cmp(&l.eval(row), &r.eval(row)) == Some(std::cmp::Ordering::Greater),
            ),
            Expr::Le(l, r) => Value::Bool(matches!(
                partial_cmp(&l.eval(row), &r.eval(row)),
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
            )),
            Expr::Ge(l, r) => Value::Bool(matches!(
                partial_cmp(&l.eval(row), &r.eval(row)),
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
            )),
            Expr::Ne(l, r) => Value::Bool(l.eval(row) != r.eval(row)),
            Expr::And(l, r) => {
                let lv = l.eval(row);
                let rv = r.eval(row);
                Value::Bool(is_true(&lv) && is_true(&rv))
            }
            Expr::Or(l, r) => {
                let lv = l.eval(row);
                let rv = r.eval(row);
                Value::Bool(is_true(&lv) || is_true(&rv))
            }
            Expr::Not(e) => Value::Bool(!is_true(&e.eval(row))),
        }
    }

    /// Return true if the expression evaluates to a truthy value for the given row.
    pub fn is_true(&self, row: &Row) -> bool {
        is_true(&self.eval(row))
    }

    // Convenience constructors.
    pub fn col(name: impl Into<String>) -> Self {
        Expr::Col(name.into())
    }
    pub fn lit(v: impl Into<Value>) -> Self {
        Expr::Lit(v.into())
    }
    pub fn eq(l: Expr, r: Expr) -> Self {
        Expr::Eq(Box::new(l), Box::new(r))
    }
    pub fn lt(l: Expr, r: Expr) -> Self {
        Expr::Lt(Box::new(l), Box::new(r))
    }
    pub fn gt(l: Expr, r: Expr) -> Self {
        Expr::Gt(Box::new(l), Box::new(r))
    }
    pub fn le(l: Expr, r: Expr) -> Self {
        Expr::Le(Box::new(l), Box::new(r))
    }
    pub fn ge(l: Expr, r: Expr) -> Self {
        Expr::Ge(Box::new(l), Box::new(r))
    }
    pub fn ne(l: Expr, r: Expr) -> Self {
        Expr::Ne(Box::new(l), Box::new(r))
    }
    pub fn and(l: Expr, r: Expr) -> Self {
        Expr::And(Box::new(l), Box::new(r))
    }
    pub fn or(l: Expr, r: Expr) -> Self {
        Expr::Or(Box::new(l), Box::new(r))
    }
    pub fn not(e: Expr) -> Self {
        Expr::Not(Box::new(e))
    }
}

// Value → Expr conversions.
impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int64(n)
    }
}
impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float64(n)
    }
}
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}
impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

fn is_true(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Int64(n) => *n != 0,
        _ => true,
    }
}

/// Partial comparison of two Values (same type only).
fn partial_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x.partial_cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.partial_cmp(y),
        (Value::Text(x), Value::Text(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
        (Value::Bytes(x), Value::Bytes(y)) => x.partial_cmp(y),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}
