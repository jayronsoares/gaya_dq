"""
gaya.checks.completeness
------------------------
Pure functions for null and required-column checks.

Signature contract:
    check_fn(stats: TableStats, config: *Config) -> list[CheckResult]

No I/O. No logging. No mutation.
Unit-testable with plain dataclass instances.
"""

from __future__ import annotations

from gaya.checks.base import (
    CheckResult,
    ColumnStats,
    NullConfig,
    RequiredConfig,
    TableStats,
    failed,
    passed,
    warned,
)


# ---------------------------------------------------------------------------
# check_null_rate
# ---------------------------------------------------------------------------

def check_null_rate(stats: TableStats, config: NullConfig) -> list[CheckResult]:
    """
    Checks null percentage per column against warn/fail thresholds.

    - Checks all columns if config.columns is None
    - Returns one CheckResult per column checked
    - If a configured column doesn't exist, returns a FAIL for that column
    """
    columns_to_check = (
        [stats.column(name) for name in config.columns]
        if config.columns
        else list(stats.columns)
    )

    results = []

    for col in columns_to_check:
        if col is None:
            # Column was specified in config but doesn't exist in the table
            results.append(failed(
                check="null_rate",
                table=stats.table_name,
                layer=stats.layer,
                column="unknown",
                message="Column specified in null check does not exist in table.",
                hint="Check your gaya.yml — column name may be misspelled.",
            ))
            continue

        pct = col.null_pct

        if pct == 0.0:
            results.append(passed(
                check="null_rate",
                table=stats.table_name,
                layer=stats.layer,
                column=col.name,
                message=f"'{col.name}' has no nulls.",
                actual=0.0,
            ))

        elif pct >= config.fail_pct:
            results.append(failed(
                check="null_rate",
                table=stats.table_name,
                layer=stats.layer,
                column=col.name,
                message=(
                    f"'{col.name}' is {pct:.1%} null "
                    f"— exceeds fail threshold of {config.fail_pct:.1%}."
                ),
                expected=f"< {config.fail_pct:.1%}",
                actual=f"{pct:.1%}",
            ))

        elif pct >= config.warn_pct:
            results.append(warned(
                check="null_rate",
                table=stats.table_name,
                layer=stats.layer,
                column=col.name,
                message=(
                    f"'{col.name}' is {pct:.1%} null "
                    f"— above warn threshold of {config.warn_pct:.1%}."
                ),
                expected=f"< {config.warn_pct:.1%}",
                actual=f"{pct:.1%}",
            ))

        else:
            results.append(passed(
                check="null_rate",
                table=stats.table_name,
                layer=stats.layer,
                column=col.name,
                message=f"'{col.name}' null rate {pct:.1%} is within threshold.",
                actual=f"{pct:.1%}",
            ))

    return results


# ---------------------------------------------------------------------------
# check_required_columns
# ---------------------------------------------------------------------------

def check_required_columns(stats: TableStats, config: RequiredConfig) -> list[CheckResult]:
    """
    Checks that required columns exist and contain zero nulls.

    - Missing column → FAIL (schema problem, not data problem)
    - Column exists but has nulls → FAIL
    - Column exists and is complete → PASS
    """
    results = []

    for col_name in config.columns:
        col = stats.column(col_name)

        if col is None:
            results.append(failed(
                check="required_columns",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=(
                    f"Required column '{col_name}' is missing from the table entirely."
                ),
                hint="Verify the column exists in the source and hasn't been renamed.",
            ))
            continue

        if col.null_count > 0:
            results.append(failed(
                check="required_columns",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=(
                    f"Required column '{col_name}' has {col.null_count:,} null(s) "
                    f"({col.null_pct:.1%} of {stats.row_count:,} rows)."
                ),
                expected=0,
                actual=col.null_count,
                hint="This column is marked required — nulls here indicate an upstream issue.",
            ))
        else:
            results.append(passed(
                check="required_columns",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=f"Required column '{col_name}' is complete.",
            ))

    return results
