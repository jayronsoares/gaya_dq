"""
gaya.baseline.store
-------------------
Reads and writes baselines to .gaya/baselines/.

This is one of three permitted impure zones in Gaya:
    1. Connectors  — read from data sources
    2. BaselineStore — read/write .gaya/ directory   ← this file
    3. Reporter    — write to stdout / exit

Design rules:
    - load() returns None cleanly on first run — never raises on missing file
    - save() is conditional — only writes if stats differ from current baseline
    - Baseline files are human-readable JSON (auditable, git-friendly)
    - No migration logic — schema changes in baseline format get a new version key

File layout:
    .gaya/
        baselines/
            {table_name}.json    ← one file per table

Baseline JSON format:
    {
        "table_name": "orders",
        "row_count": 1000,
        "schema": {"order_id": "int", "email": "string"},
        "run_at": "2026-02-19T10:00:00+00:00",
        "run_count": 7,
        "gaya_version": "0.1.0"
    }
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from gaya.checks.base import Baseline, TableStats

GAYA_VERSION = "0.1.0"
BASELINE_DIR = ".gaya/baselines"


# ---------------------------------------------------------------------------
# BaselineStore
# ---------------------------------------------------------------------------

class BaselineStore:
    """
    Reads and writes per-table baselines to a local directory.

    root: the project root where .gaya/ lives (defaults to cwd)
    """

    def __init__(self, root: Optional[str] = None):
        self._root = Path(root or os.getcwd()) / BASELINE_DIR

    # -- public API ----------------------------------------------------------

    def load(self, table_name: str) -> Optional[Baseline]:
        """
        Load the baseline for a table.
        Returns None on first run — never raises on missing file.
        """
        path = self._path(table_name)

        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return Baseline(
                table_name = data["table_name"],
                row_count  = data["row_count"],
                schema     = data["schema"],
                run_at     = data["run_at"],
                run_count  = data.get("run_count", 1),
            )
        except (KeyError, json.JSONDecodeError) as exc:
            # Corrupted baseline — treat as no baseline, don't crash
            # The runner will overwrite it on next save
            return None

    def save(self, stats: TableStats) -> bool:
        """
        Save a new baseline from the current run's TableStats.

        Conditional write:
            - Only writes if row_count or schema has changed
            - Always writes on first run (no existing baseline)
            - Increments run_count on every write

        Returns True if the file was written, False if skipped (no change).
        """
        self._root.mkdir(parents=True, exist_ok=True)
        path     = self._path(stats.table_name)
        existing = self.load(stats.table_name)

        if existing and not _has_changed(existing, stats):
            return False   # Nothing to write

        run_count = (existing.run_count + 1) if existing else 1

        data = {
            "table_name":   stats.table_name,
            "row_count":    stats.row_count,
            "schema":       stats.schema,
            "run_at":       stats.collected_at,
            "run_count":    run_count,
            "gaya_version": GAYA_VERSION,
        }

        path.write_text(
            json.dumps(data, indent=2, default=str),
            encoding="utf-8",
        )
        return True

    def exists(self, table_name: str) -> bool:
        return self._path(table_name).exists()

    def delete(self, table_name: str) -> bool:
        """Remove a baseline. Used by `gaya reset <table>`."""
        path = self._path(table_name)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_tables(self) -> list[str]:
        """List all tables with stored baselines."""
        if not self._root.exists():
            return []
        return [
            p.stem
            for p in self._root.glob("*.json")
        ]

    # -- internal ------------------------------------------------------------

    def _path(self, table_name: str) -> Path:
        # Sanitize table name for use as filename
        safe = table_name.replace(".", "_").replace("/", "_")
        return self._root / f"{safe}.json"


# ---------------------------------------------------------------------------
# Pure helper — no I/O
# ---------------------------------------------------------------------------

def _has_changed(baseline: Baseline, stats: TableStats) -> bool:
    """
    Determine if the current stats differ meaningfully from the baseline.
    Schema changes and row count changes both trigger a write.
    """
    return (
        baseline.row_count != stats.row_count
        or baseline.schema != stats.schema
    )
