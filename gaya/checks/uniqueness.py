"""
gaya.checks.uniqueness
----------------------
Pure functions for duplicate and primary key uniqueness checks.

Same contract as all other checks:
    (TableStats, *Config) -> list[CheckResult]

Note on adapter design:
    Uniqueness checks require distinct_count at the column level,
    which ColumnStats already carries. For composite key checks
    (multi-column uniqueness), the adapter must compute the combined
    distinct count and surface it via a synthetic ColumnStats entry
    named by joining the key columns: e.g. "order_id|customer_id".

    This keeps the check function pure — it never needs to know
    how the distinct count was computed.
"""

from __future__ import annotations

from gaya.checks.base import (
    CheckResult,
    TableStats,
    UniqueConfig,
    failed,
    passed,
)


# ---------------------------------------------------------------------------
# check_unique
# ---------------------------------------------------------------------------

def check_unique(stats: TableStats, config: UniqueConfig) -> list[CheckResult]:
    """
    Checks that one or more columns contain only unique values.

    Single column:
        Uses ColumnStats.distinct_count vs row_count directly.

    Composite key (multiple columns):
        Expects a synthetic ColumnStats entry named by joining
        column names with '|', e.g. "order_id|customer_id".
        The adapter is responsible for computing this.

    Duplicate logic:
        duplicates = row_count - distinct_count
        Any duplicates → FAIL (uniqueness is binary — no WARN)
    """
    results = []

    if len(config.columns) == 1:
        col_name = config.columns[0]
        col      = stats.column(col_name)

        if col is None:
            results.append(failed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=f"Column '{col_name}' specified for uniqueness check does not exist.",
                hint="Check your gaya.yml for a misspelled column name.",
            ))
            return results

        duplicates = col.row_count - col.distinct_count

        if duplicates > 0:
            results.append(failed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=(
                    f"'{col_name}' has {duplicates:,} duplicate value(s) "
                    f"across {col.row_count:,} rows "
                    f"({col.distinct_count:,} distinct)."
                ),
                expected="all distinct",
                actual=f"{duplicates:,} duplicates",
                hint=(
                    "Duplicates in a key column usually indicate a pipeline "
                    "loading the same records more than once."
                ),
            ))
        else:
            results.append(passed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=f"'{col_name}' is fully unique ({col.distinct_count:,} distinct values).",
            ))

    else:
        # Composite key — look for the synthetic combined column
        composite_name = "|".join(config.columns)
        col = stats.column(composite_name)

        if col is None:
            results.append(failed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=composite_name,
                message=(
                    f"Composite key uniqueness check requires columns "
                    f"{list(config.columns)} — "
                    f"one or more are missing from the table."
                ),
                hint="Verify all composite key columns exist in the table.",
            ))
            return results

        duplicates = col.row_count - col.distinct_count

        if duplicates > 0:
            results.append(failed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=composite_name,
                message=(
                    f"Composite key {list(config.columns)} has {duplicates:,} "
                    f"duplicate combination(s) across {col.row_count:,} rows."
                ),
                expected="all distinct",
                actual=f"{duplicates:,} duplicate combinations",
                hint=(
                    "Composite key duplicates may indicate a missing deduplication "
                    "step or an unintended cross-join upstream."
                ),
            ))
        else:
            results.append(passed(
                check="unique",
                table=stats.table_name,
                layer=stats.layer,
                column=composite_name,
                message=(
                    f"Composite key {list(config.columns)} is fully unique "
                    f"({col.distinct_count:,} distinct combinations)."
                ),
            ))

    return results
