"""
gaya.checks.base
----------------
The pure foundation of Gaya's check system.

Design contract:
  - CheckResult is immutable data. No methods that mutate.
  - A check is a plain function: (TableStats, CheckConfig) -> CheckResult
  - No I/O, no logging, no globals anywhere in this file.

If you can't unit-test it with a plain dict, it doesn't belong here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

class Status(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class Severity(str, Enum):
    """
    Default severity per check type.
    Can be overridden per-table in gaya.yml.
    """
    INFO = "info"
    WARN = "warn"
    FAIL = "fail"


class Layer(str, Enum):
    UPSTREAM   = "upstream"
    STAGING    = "staging"
    DOWNSTREAM = "downstream"


# ---------------------------------------------------------------------------
# TableStats — the pure input to every check
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ColumnStats:
    """
    Pre-aggregated stats for a single column.
    Produced by the connector. Never mutated after creation.
    """
    name:           str
    dtype:          str
    row_count:      int
    null_count:     int
    distinct_count: int
    min_value:      Optional[Any] = None
    max_value:      Optional[Any] = None
    sample_values:  tuple         = field(default_factory=tuple)

    @property
    def null_pct(self) -> float:
        if self.row_count == 0:
            return 0.0
        return self.null_count / self.row_count


@dataclass(frozen=True)
class TableStats:
    """
    Everything a check needs to know about a table.
    Produced once by the connector. Passed to all checks unchanged.
    """
    table_name:  str
    layer:       Layer
    row_count:   int
    columns:     tuple[ColumnStats, ...]   # ordered, immutable
    schema:      dict[str, str]            # {col_name: dtype}
    collected_at: str                      # ISO timestamp, informational only

    def column(self, name: str) -> Optional[ColumnStats]:
        """Lookup a column by name. Returns None if not present."""
        return next((c for c in self.columns if c.name == name), None)

    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


# ---------------------------------------------------------------------------
# Baseline — the comparison anchor for volume + schema drift checks
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Baseline:
    """
    Snapshot of a previous run stored in .gaya/baselines/.
    Used only for comparison — never mutated.
    """
    table_name:    str
    row_count:     int
    schema:        dict[str, str]          # {col_name: dtype}
    run_at:        str                     # ISO timestamp of the run that produced it
    run_count:     int = 1                 # how many runs this baseline has seen


# ---------------------------------------------------------------------------
# CheckResult — the pure output of every check
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CheckResult:
    """
    Immutable result of a single check.

    This is the only thing that crosses the boundary between
    the pure check logic and the impure output/reporter layer.

    Rule: if a field isn't populated by the check itself, it's None.
    The reporter decides how to display optional fields.
    """
    check:    str                  # e.g. "null_rate", "row_count_change"
    table:    str
    layer:    Layer
    status:   Status
    message:  str                  # human-readable, complete sentence

    column:   Optional[str] = None # populated for column-level checks
    expected: Optional[Any] = None # what was expected
    actual:   Optional[Any] = None # what was found
    hint:     Optional[str] = None # optional next-action suggestion

    @property
    def passed(self) -> bool:
        return self.status == Status.PASS

    @property
    def failed(self) -> bool:
        return self.status == Status.FAIL

    @property
    def warned(self) -> bool:
        return self.status == Status.WARN


# ---------------------------------------------------------------------------
# Result builders — small helpers to keep check functions readable
# ---------------------------------------------------------------------------

def passed(check: str, table: str, layer: Layer, message: str, **kwargs) -> CheckResult:
    return CheckResult(check=check, table=table, layer=layer,
                       status=Status.PASS, message=message, **kwargs)


def warned(check: str, table: str, layer: Layer, message: str, **kwargs) -> CheckResult:
    return CheckResult(check=check, table=table, layer=layer,
                       status=Status.WARN, message=message, **kwargs)


def failed(check: str, table: str, layer: Layer, message: str, **kwargs) -> CheckResult:
    return CheckResult(check=check, table=table, layer=layer,
                       status=Status.FAIL, message=message, **kwargs)


# ---------------------------------------------------------------------------
# CheckConfig — typed config passed into each check function
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class NullConfig:
    columns:   Optional[tuple[str, ...]] = None  # None = check all columns
    warn_pct:  float = 0.10   # warn at 10% nulls
    fail_pct:  float = 0.25   # fail at 25% nulls


@dataclass(frozen=True)
class RequiredConfig:
    columns: tuple[str, ...]  # these must have zero nulls


@dataclass(frozen=True)
class UniqueConfig:
    columns: tuple[str, ...]  # these must be unique (PK or composite)


@dataclass(frozen=True)
class RowCountConfig:
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None


@dataclass(frozen=True)
class VolumeChangeConfig:
    warn_pct: float = 0.20   # warn at ±20% change
    fail_pct: float = 0.40   # fail at ±40% change


@dataclass(frozen=True)
class SchemaConfig:
    expected: dict[str, str]  # {col_name: expected_dtype}


@dataclass(frozen=True)
class SchemaDriftConfig:
    baseline_columns: tuple[str, ...]
