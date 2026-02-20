"""
gaya.checks.schema
------------------
Pure functions for schema existence, dtype, and drift checks.

Same contract as all other checks:
    (TableStats, *Config) -> list[CheckResult]

No I/O. No mutation. No surprises.

Drift detection compares current schema against a stored Baseline.
If no baseline exists (first run), drift check passes with an
informational message — same pattern as volume_change.
"""

from __future__ import annotations

from typing import Optional

from gaya.checks.base import (
    Baseline,
    CheckResult,
    SchemaConfig,
    SchemaDriftConfig,
    TableStats,
    failed,
    passed,
    warned,
)


# ---------------------------------------------------------------------------
# check_schema
# ---------------------------------------------------------------------------

def check_schema(stats: TableStats, config: SchemaConfig) -> list[CheckResult]:
    """
    Validates that expected columns exist and have the correct dtype.

    Per column:
        - Column missing from table  → FAIL
        - Column dtype mismatch      → FAIL
        - Column exists, dtype match → PASS

    Use this when you have an explicit contract for a table's shape.
    Use check_schema_drift when you want to detect unplanned changes.
    """
    results = []

    for col_name, expected_dtype in config.expected.items():
        actual_dtype = stats.schema.get(col_name)

        if actual_dtype is None:
            results.append(failed(
                check="schema",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=(
                    f"Expected column '{col_name}' ({expected_dtype}) "
                    f"is missing from the table."
                ),
                expected=expected_dtype,
                actual=None,
                hint="Column may have been dropped or renamed upstream.",
            ))
            continue

        # Normalize for comparison — connectors may return 'VARCHAR' vs 'varchar'
        if actual_dtype.lower() != expected_dtype.lower():
            results.append(failed(
                check="schema",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=(
                    f"'{col_name}' type mismatch: "
                    f"expected '{expected_dtype}', found '{actual_dtype}'."
                ),
                expected=expected_dtype,
                actual=actual_dtype,
                hint=(
                    "A dtype change can silently break downstream queries. "
                    "Verify this was intentional."
                ),
            ))
        else:
            results.append(passed(
                check="schema",
                table=stats.table_name,
                layer=stats.layer,
                column=col_name,
                message=f"'{col_name}' is '{actual_dtype}' as expected.",
                expected=expected_dtype,
                actual=actual_dtype,
            ))

    return results


# ---------------------------------------------------------------------------
# check_schema_drift
# ---------------------------------------------------------------------------

def check_schema_drift(
    stats:    TableStats,
    config:   SchemaDriftConfig,
    baseline: Optional[Baseline],
) -> list[CheckResult]:
    """
    Detects unplanned schema changes vs the last known baseline.

    Added columns   → WARN  (non-breaking, but worth knowing)
    Removed columns → FAIL  (breaking — downstream queries will error)
    No changes      → PASS

    First run contract (no baseline):
        Returns PASS with an informational message.
        Never fails on first run.
    """
    current_cols  = set(stats.column_names())

    # First run — nothing to compare against
    if baseline is None:
        return [passed(
            check="schema_drift",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"No baseline schema found for '{stats.table_name}'. "
                f"Schema recorded with {len(current_cols)} columns. "
                f"Run again to detect drift."
            ),
            actual=sorted(current_cols),
        )]

    baseline_cols = set(baseline.schema.keys())
    added         = current_cols - baseline_cols
    removed       = baseline_cols - current_cols
    results       = []

    if not added and not removed:
        return [passed(
            check="schema_drift",
            table=stats.table_name,
            layer=stats.layer,
            message=f"Schema unchanged. {len(current_cols)} columns match baseline.",
        )]

    if removed:
        results.append(failed(
            check="schema_drift",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"{len(removed)} column(s) removed since last run: "
                f"{sorted(removed)}."
            ),
            expected=sorted(baseline_cols),
            actual=sorted(current_cols),
            hint=(
                "Removed columns break downstream SELECT * queries and "
                "any explicit column references. Verify this was intentional."
            ),
        ))

    if added:
        results.append(warned(
            check="schema_drift",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"{len(added)} new column(s) detected since last run: "
                f"{sorted(added)}."
            ),
            expected=sorted(baseline_cols),
            actual=sorted(current_cols),
            hint=(
                "New columns are usually safe, but verify they're "
                "intentional and documented."
            ),
        ))

    return results
