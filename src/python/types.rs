#![cfg(feature = "python")]

use pyo3::prelude::*;

use crate::query::expr::{Column, Expr, Schema, Value, ValueType};

// ── PyValue ──────────────────────────────────────────────────────────────────

#[pyclass(name = "Value")]
pub struct PyValue {
    pub inner: Value,
}

#[pymethods]
impl PyValue {
    #[staticmethod]
    fn null() -> Self {
        Self { inner: Value::Null }
    }

    #[staticmethod]
    fn bool_(b: bool) -> Self {
        Self {
            inner: Value::Bool(b),
        }
    }

    #[staticmethod]
    fn int(n: i64) -> Self {
        Self {
            inner: Value::Int64(n),
        }
    }

    #[staticmethod]
    fn float(f: f64) -> Self {
        Self {
            inner: Value::Float64(f),
        }
    }

    #[staticmethod]
    fn text(s: String) -> Self {
        Self {
            inner: Value::Text(s),
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Value::Null => "Null".to_string(),
            Value::Bool(b) => format!("Bool({})", b),
            Value::Int64(n) => format!("Int64({})", n),
            Value::Float64(f) => format!("Float64({})", f),
            Value::Text(s) => format!("Text({:?})", s),
            Value::Bytes(b) => format!("Bytes({:?})", b),
        }
    }

    fn __eq__(&self, other: &PyValue) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        // Simple hash for use as dict key.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        self.__repr__().hash(&mut h);
        h.finish()
    }
}

// ── PyColumn ─────────────────────────────────────────────────────────────────

#[pyclass(name = "Column")]
pub struct PyColumn {
    pub inner: Column,
}

#[pymethods]
impl PyColumn {
    #[new]
    fn new(name: String, type_name: String) -> PyResult<Self> {
        let col_type = match type_name.as_str() {
            "int64" | "integer" | "int" => ValueType::Int64,
            "float64" | "float" | "double" => ValueType::Float64,
            "text" | "string" | "str" => ValueType::Text,
            "bool" | "boolean" => ValueType::Bool,
            "bytes" => ValueType::Bytes,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown type: {}",
                    type_name
                )));
            }
        };
        Ok(Self {
            inner: Column::new(name, col_type),
        })
    }

    fn not_null(mut slf: PyRefMut<'_, Self>) -> Py<PyColumn> {
        slf.inner = slf.inner.clone().not_null();
        slf.into()
    }
}

// ── PySchema ──────────────────────────────────────────────────────────────────

#[pyclass(name = "Schema")]
pub struct PySchema {
    pub inner: Schema,
}

#[pymethods]
impl PySchema {
    #[new]
    fn new(table: String, columns: Vec<PyRef<'_, PyColumn>>) -> Self {
        let cols: Vec<Column> = columns.iter().map(|c| c.inner.clone()).collect();
        Self {
            inner: Schema::new(table, cols),
        }
    }
}

// ── PyExpr ───────────────────────────────────────────────────────────────────

#[pyclass(name = "Expr")]
pub struct PyExpr {
    pub inner: Expr,
}

#[pymethods]
impl PyExpr {
    #[staticmethod]
    fn col(name: String) -> Self {
        Self {
            inner: Expr::col(name),
        }
    }

    #[staticmethod]
    fn lit_int(n: i64) -> Self {
        Self {
            inner: Expr::lit(Value::Int64(n)),
        }
    }

    #[staticmethod]
    fn lit_text(s: String) -> Self {
        Self {
            inner: Expr::lit(Value::Text(s)),
        }
    }

    #[staticmethod]
    fn lit_bool(b: bool) -> Self {
        Self {
            inner: Expr::lit(Value::Bool(b)),
        }
    }

    fn eq(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::eq(self.inner.clone(), other.inner.clone()),
        }
    }

    fn gt(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::gt(self.inner.clone(), other.inner.clone()),
        }
    }

    fn lt(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::lt(self.inner.clone(), other.inner.clone()),
        }
    }

    fn ge(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::ge(self.inner.clone(), other.inner.clone()),
        }
    }

    fn le(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::le(self.inner.clone(), other.inner.clone()),
        }
    }

    fn ne(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::ne(self.inner.clone(), other.inner.clone()),
        }
    }

    fn and_(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::and(self.inner.clone(), other.inner.clone()),
        }
    }

    fn or_(&self, other: &PyExpr) -> PyExpr {
        PyExpr {
            inner: Expr::or(self.inner.clone(), other.inner.clone()),
        }
    }
}
