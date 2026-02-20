"""
tests.fixtures
--------------
Shared TableStats and Baseline builders for all tests.
No database. No I/O. Plain dataclass construction.
"""

from __future__ import annotations

from gaya.checks.base import (
    Baseline,
    ColumnStats,
    Layer,
    TableStats,
)


def make_column(
    name:           str,
    dtype:          str   = "string",
    row_count:      int   = 1000,
    null_count:     int   = 0,
    distinct_count: int   = None,
    min_value             = None,
    max_value             = None,
) -> ColumnStats:
    return ColumnStats(
        name           = name,
        dtype          = dtype,
        row_count      = row_count,
        null_count     = null_count,
        distinct_count = distinct_count if distinct_count is not None else row_count,
        min_value      = min_value,
        max_value      = max_value,
    )


def make_stats(
    table_name: str = "orders",
    layer:      Layer = Layer.STAGING,
    row_count:  int = 1000,
    columns:    list[ColumnStats] = None,
    schema:     dict = None,
) -> TableStats:
    cols = tuple(columns or [
        make_column("order_id",    dtype="int",    null_count=0,   distinct_count=1000),
        make_column("customer_id", dtype="int",    null_count=0,   distinct_count=750),
        make_column("email",       dtype="string", null_count=120, distinct_count=880),
        make_column("status",      dtype="string", null_count=0,   distinct_count=5),
    ])
    return TableStats(
        table_name   = table_name,
        layer        = layer,
        row_count    = row_count,
        columns      = cols,
        schema       = schema or {c.name: c.dtype for c in cols},
        collected_at = "2026-02-19T10:00:00+00:00",
    )


def make_baseline(
    table_name: str  = "orders",
    row_count:  int  = 1000,
    schema:     dict = None,
    run_count:  int  = 1,
) -> Baseline:
    return Baseline(
        table_name = table_name,
        row_count  = row_count,
        schema     = schema or {"order_id": "int", "customer_id": "int",
                                "email": "string", "status": "string"},
        run_at     = "2026-02-18T10:00:00+00:00",
        run_count  = run_count,
    )
