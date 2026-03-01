#![cfg(feature = "python")]

pub mod query;
pub mod types;

use pyo3::prelude::*;

use crate::python::query::{py_execute, py_sql_to_plan, PyPlan, PyQueryBuilder};
use crate::python::types::{PyColumn, PyExpr, PySchema, PyValue};

#[pymodule]
fn rust_db(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyValue>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyExpr>()?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_class::<PyPlan>()?;
    m.add_function(wrap_pyfunction!(py_execute, m)?)?;
    m.add_function(wrap_pyfunction!(py_sql_to_plan, m)?)?;
    Ok(())
}
