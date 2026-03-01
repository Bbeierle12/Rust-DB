"""
Stage 10 Python binding tests.

Install with:
    maturin develop --features python
Run with:
    pytest tests/python/
"""

import rust_db


def make_schema():
    return rust_db.Schema("users", [
        rust_db.Column("id", "int64"),
        rust_db.Column("name", "text"),
        rust_db.Column("age", "int64"),
    ])


def sample_rows():
    return [
        {
            "id": rust_db.Value.int(1),
            "name": rust_db.Value.text("Alice"),
            "age": rust_db.Value.int(30),
        },
        {
            "id": rust_db.Value.int(2),
            "name": rust_db.Value.text("Bob"),
            "age": rust_db.Value.int(25),
        },
    ]


def test_select_all():
    schema = make_schema()
    plan = rust_db.sql_to_plan("SELECT * FROM users", schema)
    results = rust_db.execute(plan, sample_rows())
    assert len(results) == 2


def test_where_clause():
    schema = make_schema()
    plan = rust_db.sql_to_plan("SELECT * FROM users WHERE age > 27", schema)
    results = rust_db.execute(plan, sample_rows())
    assert len(results) == 1
    assert results[0]["name"] == rust_db.Value.text("Alice")


def test_query_builder():
    schema = make_schema()
    plan = (
        rust_db.QueryBuilder.from_schema(schema)
        .filter(rust_db.Expr.col("age").gt(rust_db.Expr.lit_int(27)))
        .select(["name", "age"])
        .build()
    )
    results = rust_db.execute(plan, sample_rows())
    assert len(results) == 1


def test_value_types():
    assert rust_db.Value.null().__repr__() == "Null"
    assert rust_db.Value.int(42).__repr__() == "Int64(42)"
    assert rust_db.Value.text("hi").__repr__() == 'Text("hi")'


def test_expr_eq():
    e = rust_db.Expr.col("name").eq(rust_db.Expr.lit_text("Alice"))
    schema = make_schema()
    plan = rust_db.QueryBuilder.from_schema(schema).filter(e).build()
    results = rust_db.execute(plan, sample_rows())
    assert len(results) == 1


def test_order_by():
    schema = make_schema()
    plan = (
        rust_db.QueryBuilder.from_schema(schema)
        .order_by("age", asc=True)
        .build()
    )
    results = rust_db.execute(plan, sample_rows())
    assert results[0]["age"] == rust_db.Value.int(25)


def test_limit():
    schema = make_schema()
    plan = rust_db.sql_to_plan("SELECT * FROM users LIMIT 1", schema)
    results = rust_db.execute(plan, sample_rows())
    assert len(results) == 1
