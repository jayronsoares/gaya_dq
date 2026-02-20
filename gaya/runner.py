"""
gaya.runner
-----------
Orchestrates a full Gaya run for a single table.

Flow:
    1. Load baseline from store (None on first run)
    2. Run all configured checks (pure functions)
    3. Save updated baseline (conditional — only if stats changed)
    4. Return RunResult

Rules:
    - Runner never touches stdout — that's the reporter's job
    - Runner never raises on check failures — it collects them
    - Runner raises only on infrastructure failures (connection, config)
    - Order of checks is deterministic: schema → completeness → uniqueness → volume
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from gaya.baseline.store import BaselineStore
from gaya.checks.base import (
    Baseline,
    CheckResult,
    NullConfig,
    RequiredConfig,
    RowCountConfig,
    SchemaConfig,
    SchemaDriftConfig,
    Status,
    TableStats,
    UniqueConfig,
    VolumeChangeConfig,
)
from gaya.checks.completeness import check_null_rate, check_required_columns
from gaya.checks.schema import check_schema, check_schema_drift
from gaya.checks.uniqueness import check_unique
from gaya.checks.volume import check_row_count, check_volume_change


# ---------------------------------------------------------------------------
# TableConfig — what checks to run for a given table
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TableConfig:
    """
    Parsed configuration for a single table.
    Populated by config/loader.py from gaya.yml.
    """
    table:     str
    layer:     str
    source:    str                                      # datasource key from gaya.yml

    # Optional checks — None means "use defaults"
    null:      Optional[NullConfig]       = None
    required:  Optional[RequiredConfig]   = None
    unique:    Optional[UniqueConfig]     = None
    row_count: Optional[RowCountConfig]   = None
    volume:    Optional[VolumeChangeConfig] = None
    schema:    Optional[SchemaConfig]     = None
    drift:     Optional[SchemaDriftConfig]  = None

    # Global defaults (can be overridden per table in gaya.yml)
    run_null_check:   bool = True
    run_volume_check: bool = True
    run_drift_check:  bool = True


# ---------------------------------------------------------------------------
# RunResult — the output of a single table run
# ---------------------------------------------------------------------------

@dataclass
class RunResult:
    table:    str
    layer:    str
    results:  list[CheckResult] = field(default_factory=list)
    baseline_updated: bool = False
    error:    Optional[str] = None          # set if infra failure occurred

    @property
    def passed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == Status.PASS]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == Status.WARN]

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == Status.FAIL]

    @property
    def has_failures(self) -> bool:
        return bool(self.failures)

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)

    @property
    def overall_status(self) -> Status:
        if self.failures:
            return Status.FAIL
        if self.warnings:
            return Status.WARN
        return Status.PASS


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class Runner:
    """
    Runs all configured checks for a single table.

    Usage:
        runner = Runner(store=BaselineStore())
        result = runner.run(stats, config)
    """

    def __init__(self, store: Optional[BaselineStore] = None):
        self._store = store or BaselineStore()

    def run(self, stats: TableStats, config: TableConfig) -> RunResult:
        """
        Execute all checks for a table and return a RunResult.

        Never raises on check failures — only on infrastructure errors
        (those should be caught by the caller / CLI layer).
        """
        baseline = self._store.load(stats.table_name)
        results  = []

        # -- 1. Schema checks (run first — structural issues compound others)
        if config.schema:
            results.extend(check_schema(stats, config.schema))

        if config.run_drift_check:
            drift_config = config.drift or SchemaDriftConfig(
                baseline_columns=tuple(baseline.schema.keys()) if baseline else ()
            )
            results.extend(check_schema_drift(stats, drift_config, baseline))

        # -- 2. Completeness checks
        if config.run_null_check:
            null_config = config.null or NullConfig()
            results.extend(check_null_rate(stats, null_config))

        if config.required:
            results.extend(check_required_columns(stats, config.required))

        # -- 3. Uniqueness checks
        if config.unique:
            results.extend(check_unique(stats, config.unique))

        # -- 4. Volume checks
        if config.row_count:
            results.extend(check_row_count(stats, config.row_count))

        if config.run_volume_check:
            volume_config = config.volume or VolumeChangeConfig()
            results.extend(check_volume_change(stats, volume_config, baseline))

        # -- 5. Update baseline (conditional — only if stats changed)
        updated = self._store.save(stats)

        return RunResult(
            table=stats.table_name,
            layer=stats.layer.value,
            results=results,
            baseline_updated=updated,
        )
