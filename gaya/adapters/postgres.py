"""
gaya.adapters.postgres
----------------------
Postgres connector for Gaya.

This is intentionally the most "boring" adapter possible.
It runs a small set of aggregate queries — never full scans —
and returns a frozen TableStats.

Query strategy (≤ 2 queries per table):
    Query 1 — column metadata from information_schema (no data scan)
    Query 2 — one aggregate query computing all stats in a single pass

This satisfies the performance contract: never more than 1–2 queries
per table per run.

Dependencies:
    psycopg2-binary

Auth:
    Connection string or individual params.
    Passwords must come from env vars — never plain text in gaya.yml.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from gaya.adapters.base import DataAdapter, GayaConnectionError, GayaQueryError
from gaya.checks.base import ColumnStats, Layer, TableStats

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore


# ---------------------------------------------------------------------------
# Type mapping — Postgres → Gaya normalized types
# ---------------------------------------------------------------------------

_PG_TYPE_MAP: dict[str, str] = {
    # Numeric
    "integer":            "int",
    "int":                "int",
    "int4":               "int",
    "int8":               "int",
    "bigint":             "int",
    "smallint":           "int",
    "numeric":            "numeric",
    "decimal":            "numeric",
    "real":               "float",
    "double precision":   "float",
    "float4":             "float",
    "float8":             "float",
    # Text
    "text":               "string",
    "varchar":            "string",
    "character varying":  "string",
    "char":               "string",
    "bpchar":             "string",
    # Boolean
    "boolean":            "boolean",
    "bool":               "boolean",
    # Temporal
    "date":               "date",
    "timestamp":          "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone":    "timestamp",
    "timestamptz":        "timestamp",
    # JSON
    "json":               "json",
    "jsonb":              "json",
    # UUID
    "uuid":               "string",
}

def _normalize_pg_type(pg_type: str) -> str:
    return _PG_TYPE_MAP.get(pg_type.lower(), pg_type.lower())


# ---------------------------------------------------------------------------
# PostgresAdapter
# ---------------------------------------------------------------------------

class PostgresAdapter(DataAdapter):
    """
    Connects to a Postgres database and collects TableStats.

    Usage:
        adapter = PostgresAdapter.from_config({
            "host":     "localhost",
            "port":     5432,
            "database": "app_db",
            "user":     "app_user",
            "password": "env:DB_PASSWORD",   # resolved from env
        })
        stats = adapter.collect("orders", "staging")
    """

    def __init__(self, dsn: str):
        if psycopg2 is None:
            raise GayaConnectionError(
                "psycopg2 is not installed. Run: pip install psycopg2-binary"
            )
        self._dsn = dsn

    # -- construction --------------------------------------------------------

    @classmethod
    def from_config(cls, config: dict) -> "PostgresAdapter":
        """
        Build a PostgresAdapter from a config dict.
        Resolves 'env:VAR_NAME' references for secrets.
        """
        resolved = {k: _resolve_env(v) for k, v in config.items()}

        host     = resolved.get("host", "localhost")
        port     = resolved.get("port", 5432)
        database = resolved["database"]
        user     = resolved["user"]
        password = resolved.get("password", "")

        dsn = f"host={host} port={port} dbname={database} user={user} password={password}"
        return cls(dsn)

    # -- public API ----------------------------------------------------------

    def test_connection(self) -> bool:
        """Ping the database. Raises GayaConnectionError on failure."""
        try:
            conn = psycopg2.connect(self._dsn, connect_timeout=5)
            conn.close()
            return True
        except Exception as exc:
            raise GayaConnectionError(
                f"Cannot connect to Postgres: {exc}\n"
                f"Check your host, port, credentials, and network access."
            ) from exc

    def collect(self, table: str, layer: str) -> TableStats:
        """
        Collect stats for a table in ≤ 2 queries.

        Query 1: column metadata (information_schema — no data scan)
        Query 2: per-column aggregates in a single pass over the table
        """
        schema_name, table_name = _parse_table(table)

        try:
            conn   = psycopg2.connect(self._dsn)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Query 1 — column metadata
            column_meta = _fetch_column_meta(cursor, schema_name, table_name)

            if not column_meta:
                raise GayaQueryError(
                    f"Table '{table}' not found or has no columns. "
                    f"Check the table name and schema in gaya.yml."
                )

            # Query 2 — aggregate stats in one pass
            agg_stats = _fetch_agg_stats(cursor, schema_name, table_name, column_meta)

            cursor.close()
            conn.close()

        except GayaQueryError:
            raise
        except Exception as exc:
            raise GayaQueryError(
                f"Failed to collect stats for '{table}': {exc}"
            ) from exc

        return _build_table_stats(table, layer, column_meta, agg_stats)


# ---------------------------------------------------------------------------
# Internal query helpers — pure SQL, no business logic
# ---------------------------------------------------------------------------

def _parse_table(table: str) -> tuple[str, str]:
    """Split 'schema.table' or default to 'public'."""
    parts = table.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])


def _fetch_column_meta(cursor, schema: str, table: str) -> list[dict]:
    """
    Query 1: Column names and types from information_schema.
    Zero data scanned.
    """
    cursor.execute("""
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return cursor.fetchall()


def _fetch_agg_stats(cursor, schema: str, table: str, column_meta: list[dict]) -> dict:
    """
    Query 2: All per-column aggregates in a single pass.

    Builds one SELECT with COUNT(*) + per-column NULL and DISTINCT counts.
    Min/max collected only for numeric and date columns (cheap on indexed cols).

    Returns a flat dict: {col_name: {null_count, distinct_count, min, max}}
    """
    col_names = [row["column_name"] for row in column_meta]
    col_types = {row["column_name"]: row["data_type"] for row in column_meta}

    qualified = f'"{schema}"."{table}"'

    # Build per-column aggregate expressions
    agg_exprs = ["COUNT(*) AS _row_count"]

    for col in col_names:
        safe = col.replace('"', '""')
        agg_exprs.append(f'COUNT(*) - COUNT("{safe}") AS "_null_{safe}"')
        agg_exprs.append(f'COUNT(DISTINCT "{safe}") AS "_distinct_{safe}"')

        # Min/max only for types that benefit from it
        if _supports_minmax(col_types[col]):
            agg_exprs.append(f'MIN("{safe}") AS "_min_{safe}"')
            agg_exprs.append(f'MAX("{safe}") AS "_max_{safe}"')

    query = f"SELECT {', '.join(agg_exprs)} FROM {qualified}"
    cursor.execute(query)
    row = dict(cursor.fetchone())

    # Unpack into per-column dicts
    result = {"_row_count": row["_row_count"]}
    for col in col_names:
        safe = col.replace('"', '""')
        result[col] = {
            "null_count":     row.get(f"_null_{safe}", 0),
            "distinct_count": row.get(f"_distinct_{safe}", 0),
            "min":            row.get(f"_min_{safe}"),
            "max":            row.get(f"_max_{safe}"),
        }

    return result


def _supports_minmax(pg_type: str) -> bool:
    """Only compute min/max for types where it's meaningful and cheap."""
    normalized = _normalize_pg_type(pg_type)
    return normalized in ("int", "numeric", "float", "date", "timestamp")


# ---------------------------------------------------------------------------
# TableStats builder — converts raw query results to frozen dataclasses
# ---------------------------------------------------------------------------

def _build_table_stats(
    table:       str,
    layer:       str,
    column_meta: list[dict],
    agg_stats:   dict,
) -> TableStats:
    row_count = agg_stats["_row_count"]

    columns = []
    schema  = {}

    for meta in column_meta:
        col_name  = meta["column_name"]
        pg_type   = meta["data_type"]
        norm_type = _normalize_pg_type(pg_type)
        col_stats = agg_stats.get(col_name, {})

        columns.append(ColumnStats(
            name           = col_name,
            dtype          = norm_type,
            row_count      = row_count,
            null_count     = col_stats.get("null_count", 0),
            distinct_count = col_stats.get("distinct_count", 0),
            min_value      = col_stats.get("min"),
            max_value      = col_stats.get("max"),
        ))
        schema[col_name] = norm_type

    return TableStats(
        table_name   = table,
        layer        = Layer(layer),
        row_count    = row_count,
        columns      = tuple(columns),
        schema       = schema,
        collected_at = datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Env var resolution — secrets never in plain text
# ---------------------------------------------------------------------------

def _resolve_env(value) -> str:
    """
    Resolve 'env:VAR_NAME' references.
    'env:DB_PASSWORD' → os.environ['DB_PASSWORD']
    """
    if isinstance(value, str) and value.startswith("env:"):
        var_name = value[4:]
        resolved = os.environ.get(var_name)
        if resolved is None:
            raise GayaConnectionError(
                f"Environment variable '{var_name}' is not set. "
                f"Set it before running gaya: export {var_name}=..."
            )
        return resolved
    return str(value) if value is not None else ""
