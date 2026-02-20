"""
gaya.config.loader
------------------
Parses gaya.yml into typed config objects.

Keeps all YAML handling in one place — nothing else in Gaya
ever reads the config file directly.

gaya.yml shape:

    datasources:
      main_db:
        type: postgres
        host: localhost
        port: 5432
        database: app_db
        user: app_user
        password: env:DB_PASSWORD

    defaults:
      volume_warn_pct: 20
      volume_fail_pct: 40
      null_warn_pct:   10
      null_fail_pct:   25

    tables:
      orders:
        source: main_db
        layer: staging
        primary_key: order_id
        not_null:
          - order_id
          - customer_id
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from gaya.checks.base import (
    NullConfig,
    RequiredConfig,
    RowCountConfig,
    UniqueConfig,
    VolumeChangeConfig,
)
from gaya.runner import TableConfig


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load(path: str = "gaya.yml") -> tuple[dict, list[TableConfig]]:
    """
    Parse gaya.yml and return:
        - datasources dict  (passed to adapter factory)
        - list of TableConfig (one per table)

    Raises FileNotFoundError if gaya.yml doesn't exist.
    Raises ValueError on invalid config shape.
    """
    raw = _read(path)
    defaults    = raw.get("defaults", {})
    datasources = raw.get("datasources", {})
    tables      = raw.get("tables", {})

    if not tables:
        raise ValueError(
            "No tables defined in gaya.yml. "
            "Add at least one table under the 'tables:' key."
        )

    configs = [
        _parse_table(name, cfg, defaults)
        for name, cfg in tables.items()
    ]

    return datasources, configs


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"'{path}' not found. Run `gaya init` to create one."
        )
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _parse_table(name: str, cfg: dict, defaults: dict) -> TableConfig:
    layer  = cfg.get("layer", "staging")
    source = cfg.get("source", "default")

    # Null thresholds
    null_config = NullConfig(
        warn_pct=defaults.get("null_warn_pct", 10) / 100,
        fail_pct=defaults.get("null_fail_pct", 25) / 100,
    )

    # Required (not_null) columns
    required_cols = cfg.get("not_null", [])
    required = RequiredConfig(columns=tuple(required_cols)) if required_cols else None

    # Primary key uniqueness
    pk     = cfg.get("primary_key")
    unique = UniqueConfig(columns=(pk,)) if pk else None

    # Row count bounds (optional explicit config)
    min_rows = cfg.get("min_rows")
    max_rows = cfg.get("max_rows")
    row_count = RowCountConfig(min_rows=min_rows, max_rows=max_rows) if (min_rows or max_rows) else None

    # Volume change thresholds
    volume = VolumeChangeConfig(
        warn_pct=defaults.get("volume_warn_pct", 20) / 100,
        fail_pct=defaults.get("volume_fail_pct", 40) / 100,
    )

    return TableConfig(
        table     = name,
        layer     = layer,
        source    = source,
        null      = null_config,
        required  = required,
        unique    = unique,
        row_count = row_count,
        volume    = volume,
    )
