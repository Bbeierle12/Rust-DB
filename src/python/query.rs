#![cfg(feature = "python")]

use std::collections::BTreeMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::python::types::{PyExpr, PySchema, PyValue};
use crate::query::builder::QueryBuilder;
use crate::query::executor::execute;
use crate::query::expr::{Row, Value};
use crate::query::plan::{LogicalPlan, SortOrder};
use crate::query::sql::sql_to_plan;

// ── PyPlan ────────────────────────────────────────────────────────────────────

#[pyclass(name = "Plan")]
pub struct PyPlan {
    pub inner: LogicalPlan,
}

// ── PyQueryBuilder ────────────────────────────────────────────────────────────

#[pyclass(name = "QueryBuilder")]
pub struct PyQueryBuilder {
    pub plan: LogicalPlan,
}

#[pymethods]
impl PyQueryBuilder {
    #[staticmethod]
    fn from_schema(schema: PyRef<'_, PySchema>) -> Self {
        Self {
            plan: QueryBuilder::from(schema.inner.clone()).build(),
        }
    }

    fn filter(&self, expr: PyRef<'_, PyExpr>) -> Self {
        let plan = QueryBuilder::from_plan(self.plan.clone())
            .filter(expr.inner.clone())
            .build();
        PyQueryBuilder { plan }
    }

    fn select(&self, columns: Vec<String>) -> Self {
        let plan = QueryBuilder::from_plan(self.plan.clone())
            .select(columns)
            .build();
        PyQueryBuilder { plan }
    }

    fn order_by(&self, col: String, asc: bool) -> Self {
        let order = if asc { SortOrder::Asc } else { SortOrder::Desc };
        let plan = QueryBuilder::from_plan(self.plan.clone())
            .order_by(col, order)
            .build();
        PyQueryBuilder { plan }
    }

    fn limit(&self, n: usize) -> Self {
        let plan = QueryBuilder::from_plan(self.plan.clone()).limit(n).build();
        PyQueryBuilder { plan }
    }

    fn build(&self) -> PyPlan {
        PyPlan {
            inner: self.plan.clone(),
        }
    }
}

// ── py_execute ────────────────────────────────────────────────────────────────

/// Execute a plan against a list of row dicts.
#[pyfunction]
#[pyo3(name = "execute")]
pub fn py_execute(
    py: Python<'_>,
    plan: PyRef<'_, PyPlan>,
    rows: &Bound<'_, PyList>,
) -> PyResult<Py<PyList>> {
    let rust_rows = py_rows_to_rust(rows)?;
    let result = execute(&plan.inner, rust_rows);
    rust_rows_to_py(py, result)
}

/// Parse SQL into a plan.
#[pyfunction]
#[pyo3(name = "sql_to_plan")]
pub fn py_sql_to_plan(sql: String, schema: PyRef<'_, PySchema>) -> PyResult<PyPlan> {
    let plan = sql_to_plan(&sql, schema.inner.clone())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;
    Ok(PyPlan { inner: plan })
}

// ── Conversion helpers ────────────────────────────────────────────────────────

fn py_rows_to_rust(rows: &Bound<'_, PyList>) -> PyResult<Vec<Row>> {
    let mut result = Vec::new();
    for item in rows.iter() {
        let dict = item.downcast::<PyDict>()?;
        let mut row: Row = BTreeMap::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            // Try extracting as PyValue first; fall back to primitive types.
            let val: Value = if let Ok(pv) = v.extract::<PyRef<'_, PyValue>>() {
                pv.inner.clone()
            } else if let Ok(n) = v.extract::<i64>() {
                Value::Int64(n)
            } else if let Ok(f) = v.extract::<f64>() {
                Value::Float64(f)
            } else if let Ok(s) = v.extract::<String>() {
                Value::Text(s)
            } else if let Ok(b) = v.extract::<bool>() {
                Value::Bool(b)
            } else {
                Value::Null
            };
            row.insert(key, val);
        }
        result.push(row);
    }
    Ok(result)
}

fn rust_rows_to_py(py: Python<'_>, rows: Vec<Row>) -> PyResult<Py<PyList>> {
    let list = PyList::empty_bound(py);
    for row in rows {
        let dict = PyDict::new_bound(py);
        for (k, v) in row {
            let py_val = Py::new(py, PyValue { inner: v })?;
            dict.set_item(k, py_val)?;
        }
        list.append(&dict)?;
    }
    Ok(list.unbind())
}
