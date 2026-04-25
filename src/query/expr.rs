use std::collections::BTreeMap;
use std::fmt;

/// Scalar value type used in query results and expressions.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// Microseconds since Unix epoch (1970-01-01T00:00:00Z).
    Timestamp(i64),
    /// Days since Unix epoch (1970-01-01).
    Date(i32),
    /// UUID as 16 raw bytes.
    Uuid([u8; 16]),
    /// Fixed-point decimal: (unscaled_value, scale). E.g. Decimal(12345, 2) = 123.45
    Decimal(i128, u8),
    /// Dense float32 vector for embeddings.
    Vector(Vec<f32>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Decimal(av, as_), Value::Decimal(bv, bs)) => {
                // Normalize scales for comparison.
                let max_s = (*as_).max(*bs);
                let a_norm = *av * 10i128.pow((max_s - *as_) as u32);
                let b_norm = *bv * 10i128.pow((max_s - *bs) as u32);
                a_norm == b_norm
            }
            (Value::Vector(a), Value::Vector(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        partial_cmp(self, other)
    }
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
            Value::Timestamp(us) => write!(f, "{}", format_timestamp(*us)),
            Value::Date(days) => write!(f, "{}", format_date(*days)),
            Value::Uuid(bytes) => write!(f, "{}", format_uuid(bytes)),
            Value::Decimal(val, scale) => write!(f, "{}", format_decimal(*val, *scale)),
            Value::Vector(v) => write!(f, "{}", format_vector(v)),
        }
    }
}

/// Format a vector as `[v1, v2, ...]` with f32 default formatting.
pub fn format_vector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 6 + 2);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&x.to_string());
    }
    s.push(']');
    s
}

/// Parse `[v1, v2, ...]` into `Vec<f32>`. Whitespace-tolerant.
pub fn parse_vector_str(s: &str) -> Option<Vec<f32>> {
    let trimmed = s.trim();
    let inner = trimmed.strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    inner
        .split(',')
        .map(|p| p.trim().parse::<f32>().ok())
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Format microseconds since epoch as ISO 8601 timestamp.
pub fn format_timestamp(us: i64) -> String {
    let total_secs = us.div_euclid(1_000_000);
    let frac_us = us.rem_euclid(1_000_000);
    let days_from_epoch = total_secs.div_euclid(86400);
    let secs_in_day = total_secs.rem_euclid(86400);
    let (y, m, d) = days_to_ymd(days_from_epoch as i32);
    let hour = secs_in_day / 3600;
    let min = (secs_in_day % 3600) / 60;
    let sec = secs_in_day % 60;
    if frac_us == 0 {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            y, m, d, hour, min, sec
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            y, m, d, hour, min, sec, frac_us
        )
    }
}

/// Format days since epoch as YYYY-MM-DD.
pub fn format_date(days: i32) -> String {
    let (y, m, d) = days_to_ymd(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Format UUID bytes as the standard hex-dash format.
pub fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// Format a decimal (unscaled, scale) as a string with decimal point.
pub fn format_decimal(val: i128, scale: u8) -> String {
    if scale == 0 {
        return val.to_string();
    }
    let negative = val < 0;
    let abs_val = val.unsigned_abs();
    let s = abs_val.to_string();
    let scale = scale as usize;
    if s.len() <= scale {
        // Need leading zeros: e.g. 5 with scale 2 -> "0.05"
        let zeros = scale - s.len();
        let result = format!("0.{}{}", "0".repeat(zeros), s);
        if negative {
            format!("-{}", result)
        } else {
            result
        }
    } else {
        let (integer, frac) = s.split_at(s.len() - scale);
        let result = format!("{}.{}", integer, frac);
        if negative {
            format!("-{}", result)
        } else {
            result
        }
    }
}

// ─── Date/time helpers (no external deps) ────────────────────────────────────

fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn days_in_year(y: i32) -> i32 {
    if is_leap_year(y) { 366 } else { 365 }
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(mut days: i32) -> (i32, u32, u32) {
    // Civil date from day count algorithm.
    let mut y = 1970;
    if days >= 0 {
        loop {
            let dy = days_in_year(y);
            if days < dy {
                break;
            }
            days -= dy;
            y += 1;
        }
    } else {
        loop {
            y -= 1;
            days += days_in_year(y);
            if days >= 0 {
                break;
            }
        }
    }
    let mut m = 1u32;
    loop {
        let dm = days_in_month(y, m) as i32;
        if days < dm {
            break;
        }
        days -= dm;
        m += 1;
    }
    (y, m, (days + 1) as u32)
}

/// Convert (year, month, day) to days since Unix epoch.
pub fn ymd_to_days(y: i32, m: u32, d: u32) -> i32 {
    let mut days: i32 = 0;
    if y >= 1970 {
        for yr in 1970..y {
            days += days_in_year(yr);
        }
    } else {
        for yr in y..1970 {
            days -= days_in_year(yr);
        }
    }
    for mo in 1..m {
        days += days_in_month(y, mo) as i32;
    }
    days += (d as i32) - 1;
    days
}

/// Parse "YYYY-MM-DD" to days since epoch.
pub fn parse_date_str(s: &str) -> Option<i32> {
    let s = s.trim();
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    Some(ymd_to_days(y, m, d))
}

/// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS" to microseconds since epoch.
pub fn parse_timestamp_str(s: &str) -> Option<i64> {
    let s = s.trim();
    // Split on space or 'T'
    let (date_part, time_part) = if let Some(idx) = s.find('T') {
        (&s[..idx], &s[idx + 1..])
    } else if let Some(idx) = s.find(' ') {
        (&s[..idx], &s[idx + 1..])
    } else {
        // Date only, assume 00:00:00
        (s, "00:00:00")
    };
    let days = parse_date_str(date_part)?;
    // Strip trailing 'Z' if present
    let time_part = time_part.strip_suffix('Z').unwrap_or(time_part);
    let time_parts: Vec<&str> = time_part.split(':').collect();
    if time_parts.len() < 2 || time_parts.len() > 3 {
        return None;
    }
    let h: i64 = time_parts[0].parse().ok()?;
    let m: i64 = time_parts[1].parse().ok()?;
    let (s_int, s_frac_us): (i64, i64) = if time_parts.len() == 3 {
        let sec_str = time_parts[2];
        if let Some(dot_idx) = sec_str.find('.') {
            let int_part: i64 = sec_str[..dot_idx].parse().ok()?;
            let frac_str = &sec_str[dot_idx + 1..];
            // Pad or truncate to 6 digits
            let padded = format!("{:0<6}", frac_str);
            let frac: i64 = padded[..6].parse().ok()?;
            (int_part, frac)
        } else {
            (sec_str.parse().ok()?, 0)
        }
    } else {
        (0, 0)
    };
    let total_us = (days as i64) * 86_400_000_000
        + h * 3_600_000_000
        + m * 60_000_000
        + s_int * 1_000_000
        + s_frac_us;
    Some(total_us)
}

/// Parse UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" to 16 bytes.
pub fn parse_uuid_str(s: &str) -> Option<[u8; 16]> {
    let s = s.trim();
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Parse a decimal string like "123.45" to (unscaled, scale).
pub fn parse_decimal_str(s: &str) -> Option<(i128, u8)> {
    let s = s.trim();
    let negative = s.starts_with('-');
    let s = if negative { &s[1..] } else { s };
    if let Some(dot_idx) = s.find('.') {
        let int_part = &s[..dot_idx];
        let frac_part = &s[dot_idx + 1..];
        let scale = frac_part.len() as u8;
        let combined = format!("{}{}", int_part, frac_part);
        let val: i128 = combined.parse().ok()?;
        Some(if negative {
            (-val, scale)
        } else {
            (val, scale)
        })
    } else {
        let val: i128 = s.parse().ok()?;
        Some(if negative { (-val, 0) } else { (val, 0) })
    }
}

/// The type tag of a Value, without the data.
///
/// `Vector { dim }` carries its dimensionality so the catalog can validate
/// inserts. Other types are zero-payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Null,
    Bool,
    Int64,
    Float64,
    Text,
    Bytes,
    Timestamp,
    Date,
    Uuid,
    Decimal,
    Vector { dim: u32 },
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
            Value::Timestamp(_) => ValueType::Timestamp,
            Value::Date(_) => ValueType::Date,
            Value::Uuid(_) => ValueType::Uuid,
            Value::Decimal(_, _) => ValueType::Decimal,
            Value::Vector(v) => ValueType::Vector {
                dim: v.len() as u32,
            },
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
            Value::Timestamp(us) => {
                buf.push(6);
                buf.extend_from_slice(&us.to_le_bytes());
            }
            Value::Date(days) => {
                buf.push(7);
                buf.extend_from_slice(&days.to_le_bytes());
            }
            Value::Uuid(bytes) => {
                buf.push(8);
                buf.extend_from_slice(bytes);
            }
            Value::Decimal(val, scale) => {
                buf.push(9);
                buf.extend_from_slice(&val.to_le_bytes());
                buf.push(*scale);
            }
            Value::Vector(v) => {
                buf.push(10);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for x in v {
                    buf.extend_from_slice(&x.to_le_bytes());
                }
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
                let s = std::str::from_utf8(data.get(pos..pos + len)?)
                    .ok()?
                    .to_string();
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
            6 => {
                let us = i64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
                pos += 8;
                Some((Value::Timestamp(us), pos))
            }
            7 => {
                let days = i32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?);
                pos += 4;
                Some((Value::Date(days), pos))
            }
            8 => {
                let bytes: [u8; 16] = data.get(pos..pos + 16)?.try_into().ok()?;
                pos += 16;
                Some((Value::Uuid(bytes), pos))
            }
            9 => {
                let val = i128::from_le_bytes(data.get(pos..pos + 16)?.try_into().ok()?);
                pos += 16;
                let scale = *data.get(pos)?;
                pos += 1;
                Some((Value::Decimal(val, scale), pos))
            }
            10 => {
                let len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    let x = f32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?);
                    pos += 4;
                    v.push(x);
                }
                Some((Value::Vector(v), pos))
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
    /// Addition.
    Add(Box<Expr>, Box<Expr>),
    /// Subtraction.
    Sub(Box<Expr>, Box<Expr>),
    /// Multiplication.
    Mul(Box<Expr>, Box<Expr>),
    /// Division.
    Div(Box<Expr>, Box<Expr>),
    /// Modulo.
    Mod(Box<Expr>, Box<Expr>),
    /// Unary negation.
    Negate(Box<Expr>),
    /// IS NULL check.
    IsNull(Box<Expr>),
    /// IS NOT NULL check.
    IsNotNull(Box<Expr>),
    /// LIKE pattern matching (value, pattern, escape char).
    Like(Box<Expr>, Box<Expr>, Option<char>),
    /// IN list check (value, list).
    In(Box<Expr>, Vec<Expr>),
    /// BETWEEN check (value, low, high).
    Between(Box<Expr>, Box<Expr>, Box<Expr>),
    /// CASE expression.
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// SQL function call (name, arguments).
    Function(String, Vec<Expr>),
}

impl Expr {
    /// Evaluate the expression against a row, returning a Value.
    pub fn eval(&self, row: &Row) -> Value {
        match self {
            Expr::Col(name) => {
                // Try qualified name first; fall back to stripping table prefix.
                // e.g. "devices.status" → try "devices.status", then "status".
                if let Some(v) = row.get(name) {
                    v.clone()
                } else if let Some(dot) = name.rfind('.') {
                    row.get(&name[dot + 1..]).cloned().unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Expr::Lit(v) => v.clone(),
            Expr::Eq(l, r) => Value::Bool(l.eval(row) == r.eval(row)),
            Expr::Lt(l, r) => Value::Bool(
                partial_cmp(&l.eval(row), &r.eval(row)) == Some(std::cmp::Ordering::Less),
            ),
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
            Expr::Add(l, r) => eval_arith(&l.eval(row), &r.eval(row), ArithOp::Add),
            Expr::Sub(l, r) => eval_arith(&l.eval(row), &r.eval(row), ArithOp::Sub),
            Expr::Mul(l, r) => eval_arith(&l.eval(row), &r.eval(row), ArithOp::Mul),
            Expr::Div(l, r) => eval_arith(&l.eval(row), &r.eval(row), ArithOp::Div),
            Expr::Mod(l, r) => eval_arith(&l.eval(row), &r.eval(row), ArithOp::Mod),
            Expr::Negate(e) => match e.eval(row) {
                Value::Int64(n) => Value::Int64(-n),
                Value::Float64(f) => Value::Float64(-f),
                Value::Decimal(v, s) => Value::Decimal(-v, s),
                _ => Value::Null,
            },
            Expr::IsNull(e) => Value::Bool(e.eval(row).is_null()),
            Expr::IsNotNull(e) => Value::Bool(!e.eval(row).is_null()),
            Expr::Like(val_expr, pat_expr, escape) => {
                match (val_expr.eval(row), pat_expr.eval(row)) {
                    (Value::Text(val), Value::Text(pat)) => {
                        Value::Bool(like_match(&val, &pat, *escape))
                    }
                    _ => Value::Null,
                }
            }
            Expr::In(val_expr, list) => {
                let val = val_expr.eval(row);
                let found = list.iter().any(|item| item.eval(row) == val);
                Value::Bool(found)
            }
            Expr::Between(val_expr, low_expr, high_expr) => {
                let val = val_expr.eval(row);
                let low = low_expr.eval(row);
                let high = high_expr.eval(row);
                let ge_low = matches!(
                    partial_cmp(&val, &low),
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                );
                let le_high = matches!(
                    partial_cmp(&val, &high),
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                );
                Value::Bool(ge_low && le_high)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    let op_val = op.eval(row);
                    for (when_expr, then_expr) in when_clauses {
                        if when_expr.eval(row) == op_val {
                            return then_expr.eval(row);
                        }
                    }
                } else {
                    for (when_expr, then_expr) in when_clauses {
                        if is_true(&when_expr.eval(row)) {
                            return then_expr.eval(row);
                        }
                    }
                }
                match else_result {
                    Some(e) => e.eval(row),
                    None => Value::Null,
                }
            }
            Expr::Function(name, args) => {
                let uname = name.to_uppercase();
                match uname.as_str() {
                    "COALESCE" => {
                        for arg in args {
                            let val = arg.eval(row);
                            if !val.is_null() {
                                return val;
                            }
                        }
                        Value::Null
                    }
                    "NOW" => {
                        // Return current timestamp in microseconds since Unix epoch.
                        use std::time::{SystemTime, UNIX_EPOCH};
                        let us = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as i64;
                        Value::Timestamp(us)
                    }
                    "COSINE_DISTANCE" => apply_vector_distance(args, row, vec_cosine_distance),
                    "L2_DISTANCE" => apply_vector_distance(args, row, vec_l2_distance),
                    "DOT_PRODUCT" => apply_vector_distance(args, row, vec_dot_product),
                    _ => Value::Null,
                }
            }
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
    #[allow(clippy::should_implement_trait)]
    pub fn not(e: Expr) -> Self {
        Expr::Not(Box::new(e))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn add(l: Expr, r: Expr) -> Self {
        Expr::Add(Box::new(l), Box::new(r))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn sub(l: Expr, r: Expr) -> Self {
        Expr::Sub(Box::new(l), Box::new(r))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn mul(l: Expr, r: Expr) -> Self {
        Expr::Mul(Box::new(l), Box::new(r))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn div(l: Expr, r: Expr) -> Self {
        Expr::Div(Box::new(l), Box::new(r))
    }
    pub fn modulo(l: Expr, r: Expr) -> Self {
        Expr::Mod(Box::new(l), Box::new(r))
    }
    pub fn negate(e: Expr) -> Self {
        Expr::Negate(Box::new(e))
    }
    pub fn is_null(e: Expr) -> Self {
        Expr::IsNull(Box::new(e))
    }
    pub fn is_not_null(e: Expr) -> Self {
        Expr::IsNotNull(Box::new(e))
    }
    pub fn like(val: Expr, pattern: Expr) -> Self {
        Expr::Like(Box::new(val), Box::new(pattern), None)
    }
    pub fn like_escape(val: Expr, pattern: Expr, escape: char) -> Self {
        Expr::Like(Box::new(val), Box::new(pattern), Some(escape))
    }
    pub fn in_list(val: Expr, list: Vec<Expr>) -> Self {
        Expr::In(Box::new(val), list)
    }
    pub fn between(val: Expr, low: Expr, high: Expr) -> Self {
        Expr::Between(Box::new(val), Box::new(low), Box::new(high))
    }
    pub fn case(
        operand: Option<Expr>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Expr>,
    ) -> Self {
        Expr::Case {
            operand: operand.map(Box::new),
            when_clauses,
            else_result: else_result.map(Box::new),
        }
    }
    pub fn function(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::Function(name.into(), args)
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

enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn eval_arith(a: &Value, b: &Value, op: ArithOp) -> Value {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => match op {
            ArithOp::Add => Value::Int64(x.wrapping_add(*y)),
            ArithOp::Sub => Value::Int64(x.wrapping_sub(*y)),
            ArithOp::Mul => Value::Int64(x.wrapping_mul(*y)),
            ArithOp::Div => {
                if *y == 0 {
                    Value::Null
                } else {
                    Value::Int64(x / y)
                }
            }
            ArithOp::Mod => {
                if *y == 0 {
                    Value::Null
                } else {
                    Value::Int64(x % y)
                }
            }
        },
        (Value::Float64(x), Value::Float64(y)) => match op {
            ArithOp::Add => Value::Float64(x + y),
            ArithOp::Sub => Value::Float64(x - y),
            ArithOp::Mul => Value::Float64(x * y),
            ArithOp::Div => {
                if *y == 0.0 {
                    Value::Null
                } else {
                    Value::Float64(x / y)
                }
            }
            ArithOp::Mod => {
                if *y == 0.0 {
                    Value::Null
                } else {
                    Value::Float64(x % y)
                }
            }
        },
        (Value::Int64(x), Value::Float64(y)) => {
            eval_arith(&Value::Float64(*x as f64), &Value::Float64(*y), op)
        }
        (Value::Float64(x), Value::Int64(y)) => {
            eval_arith(&Value::Float64(*x), &Value::Float64(*y as f64), op)
        }
        // Date + Int64 = Date (add days), Date - Int64 = Date (subtract days)
        (Value::Date(d), Value::Int64(n)) => match op {
            ArithOp::Add => Value::Date(d.wrapping_add(*n as i32)),
            ArithOp::Sub => Value::Date(d.wrapping_sub(*n as i32)),
            _ => Value::Null,
        },
        // Date - Date = Int64 (difference in days)
        (Value::Date(a), Value::Date(b)) => match op {
            ArithOp::Sub => Value::Int64((*a as i64) - (*b as i64)),
            _ => Value::Null,
        },
        // Timestamp + Int64 = Timestamp (add microseconds)
        (Value::Timestamp(t), Value::Int64(n)) => match op {
            ArithOp::Add => Value::Timestamp(t.wrapping_add(*n)),
            ArithOp::Sub => Value::Timestamp(t.wrapping_sub(*n)),
            _ => Value::Null,
        },
        // Timestamp - Timestamp = Int64 (difference in microseconds)
        (Value::Timestamp(a), Value::Timestamp(b)) => match op {
            ArithOp::Sub => Value::Int64(*a - *b),
            _ => Value::Null,
        },
        // Decimal arithmetic
        (Value::Decimal(av, as_), Value::Decimal(bv, bs)) => {
            match op {
                ArithOp::Add | ArithOp::Sub => {
                    let max_s = (*as_).max(*bs);
                    let a_norm = *av * 10i128.pow((max_s - *as_) as u32);
                    let b_norm = *bv * 10i128.pow((max_s - *bs) as u32);
                    let result = match op {
                        ArithOp::Add => a_norm + b_norm,
                        ArithOp::Sub => a_norm - b_norm,
                        _ => unreachable!(),
                    };
                    Value::Decimal(result, max_s)
                }
                ArithOp::Mul => Value::Decimal(*av * *bv, *as_ + *bs),
                ArithOp::Div => {
                    if *bv == 0 {
                        Value::Null
                    } else {
                        // Scale the numerator up to preserve precision.
                        let extra_scale: u8 = 6; // extra digits of precision
                        let a_scaled = *av * 10i128.pow(extra_scale as u32);
                        let result = a_scaled / *bv;
                        let new_scale = as_.saturating_sub(*bs).saturating_add(extra_scale);
                        Value::Decimal(result, new_scale)
                    }
                }
                ArithOp::Mod => {
                    if *bv == 0 {
                        Value::Null
                    } else {
                        let max_s = (*as_).max(*bs);
                        let a_norm = *av * 10i128.pow((max_s - *as_) as u32);
                        let b_norm = *bv * 10i128.pow((max_s - *bs) as u32);
                        Value::Decimal(a_norm % b_norm, max_s)
                    }
                }
            }
        }
        _ => Value::Null,
    }
}

/// LIKE pattern matching: `%` matches any sequence of chars, `_` matches exactly one char.
/// Case-sensitive. If `escape` is `Some(e)`, the next char after `e` in the pattern is
/// matched literally (so `\%` matches a literal `%`).
fn like_match(value: &str, pattern: &str, escape: Option<char>) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_inner(&v, &p, escape)
}

fn like_match_inner(v: &[char], p: &[char], escape: Option<char>) -> bool {
    if p.is_empty() {
        return v.is_empty();
    }
    if let Some(e) = escape
        && p[0] == e
        && p.len() >= 2
    {
        // Literal match of the escaped character.
        return !v.is_empty() && v[0] == p[1] && like_match_inner(&v[1..], &p[2..], escape);
    }
    match p[0] {
        '%' => {
            let mut pi = 0;
            while pi < p.len() && p[pi] == '%' {
                pi += 1;
            }
            let rest_p = &p[pi..];
            if rest_p.is_empty() {
                return true;
            }
            for i in 0..=v.len() {
                if like_match_inner(&v[i..], rest_p, escape) {
                    return true;
                }
            }
            false
        }
        '_' => !v.is_empty() && like_match_inner(&v[1..], &p[1..], escape),
        c => !v.is_empty() && v[0] == c && like_match_inner(&v[1..], &p[1..], escape),
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

/// Partial comparison of two Values. Supports mixed Int64/Float64 via promotion.
fn partial_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x.partial_cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.partial_cmp(y),
        (Value::Int64(x), Value::Float64(y)) => (*x as f64).partial_cmp(y),
        (Value::Float64(x), Value::Int64(y)) => x.partial_cmp(&(*y as f64)),
        (Value::Text(x), Value::Text(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
        (Value::Bytes(x), Value::Bytes(y)) => x.partial_cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.partial_cmp(y),
        (Value::Date(x), Value::Date(y)) => x.partial_cmp(y),
        (Value::Uuid(x), Value::Uuid(y)) => x.partial_cmp(y),
        (Value::Decimal(av, as_), Value::Decimal(bv, bs)) => {
            let max_s = (*as_).max(*bs);
            let a_norm = *av * 10i128.pow((max_s - *as_) as u32);
            let b_norm = *bv * 10i128.pow((max_s - *bs) as u32);
            a_norm.partial_cmp(&b_norm)
        }
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

// ─── Vector distance helpers ─────────────────────────────────────────────────

fn apply_vector_distance(args: &[Expr], row: &Row, f: fn(&[f32], &[f32]) -> Option<f64>) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let a = match coerce_to_vector(args[0].eval(row)) {
        Some(v) => v,
        None => return Value::Null,
    };
    let b = match coerce_to_vector(args[1].eval(row)) {
        Some(v) => v,
        None => return Value::Null,
    };
    match f(&a, &b) {
        Some(d) => Value::Float64(d),
        None => Value::Null,
    }
}

/// Accept either Value::Vector directly or a `'[...]'` text literal that
/// distance-function callers commonly pass as a query parameter.
fn coerce_to_vector(v: Value) -> Option<Vec<f32>> {
    match v {
        Value::Vector(v) => Some(v),
        Value::Text(s) => parse_vector_str(&s),
        _ => None,
    }
}

/// Cosine distance = 1 - (a·b / (|a||b|)). Returns None on dim mismatch or zero norm.
pub fn vec_cosine_distance(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for i in 0..a.len() {
        let ai = a[i] as f64;
        let bi = b[i] as f64;
        dot += ai * bi;
        na += ai * ai;
        nb += bi * bi;
    }
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some(1.0 - dot / (na.sqrt() * nb.sqrt()))
}

/// Euclidean (L2) distance.
pub fn vec_l2_distance(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    let mut sum = 0.0f64;
    for i in 0..a.len() {
        let d = a[i] as f64 - b[i] as f64;
        sum += d * d;
    }
    Some(sum.sqrt())
}

/// Dot product. Note: not a distance — larger means more similar.
pub fn vec_dot_product(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    let mut sum = 0.0f64;
    for i in 0..a.len() {
        sum += a[i] as f64 * b[i] as f64;
    }
    Some(sum)
}
