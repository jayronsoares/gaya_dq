"""
gaya.checks.volume
------------------
Pure functions for row count and volume change checks.

Volume change requires a Baseline. If no baseline exists,
the check returns a neutral INFO result — never a false failure.

First-run contract:
    No baseline → PASS with message "Baseline established. Run again to detect changes."
    This is intentional. Never punish a first run.
"""

from __future__ import annotations

from typing import Optional

from gaya.checks.base import (
    Baseline,
    CheckResult,
    RowCountConfig,
    TableStats,
    VolumeChangeConfig,
    failed,
    passed,
    warned,
)


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------

def check_row_count(stats: TableStats, config: RowCountConfig) -> list[CheckResult]:
    """
    Validates row count is within an absolute expected range.

    Use this when you know roughly how many rows to expect.
    Use check_volume_change when you want relative comparison.
    """
    count = stats.row_count
    results = []

    if config.min_rows is not None and count < config.min_rows:
        results.append(failed(
            check="row_count",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"Row count {count:,} is below minimum {config.min_rows:,}."
            ),
            expected=f">= {config.min_rows:,}",
            actual=f"{count:,}",
            hint="The table may not have loaded correctly or the source is empty.",
        ))

    elif config.max_rows is not None and count > config.max_rows:
        results.append(failed(
            check="row_count",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"Row count {count:,} exceeds maximum {config.max_rows:,}."
            ),
            expected=f"<= {config.max_rows:,}",
            actual=f"{count:,}",
            hint="This may indicate duplicate rows or an unintended full reload.",
        ))

    else:
        results.append(passed(
            check="row_count",
            table=stats.table_name,
            layer=stats.layer,
            message=f"Row count {count:,} is within expected range.",
            actual=f"{count:,}",
        ))

    return results


# ---------------------------------------------------------------------------
# check_volume_change
# ---------------------------------------------------------------------------

def check_volume_change(
    stats:    TableStats,
    config:   VolumeChangeConfig,
    baseline: Optional[Baseline],
) -> list[CheckResult]:
    """
    Compares current row count against the last known baseline.

    If no baseline exists (first run), returns PASS with an informational
    message. The caller is responsible for persisting the new baseline
    after checks complete — this function never writes anything.

    Thresholds:
        warn_pct: default 20% change triggers a warning
        fail_pct: default 40% change triggers a failure
    """
    current = stats.row_count

    # First run — no baseline to compare against
    if baseline is None:
        return [passed(
            check="volume_change",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"No baseline found for '{stats.table_name}'. "
                f"Baseline set at {current:,} rows. "
                f"Run again to detect volume changes."
            ),
            actual=f"{current:,}",
        )]

    previous  = baseline.row_count
    delta     = current - previous
    change_pct = abs(delta) / max(previous, 1)
    direction  = "increased" if delta > 0 else "dropped"
    sign       = "+" if delta > 0 else "-"

    if change_pct >= config.fail_pct:
        return [failed(
            check="volume_change",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"Row count {direction} {change_pct:.1%} "
                f"({previous:,} → {current:,}, {sign}{abs(delta):,} rows)."
            ),
            expected=f"< {config.fail_pct:.1%} change",
            actual=f"{change_pct:.1%} {direction}",
            hint=(
                "A drop this large usually means a failed upstream load "
                "or a filter was applied unintentionally."
                if delta < 0 else
                "A spike this large may indicate duplicate rows or a full reload."
            ),
        )]

    if change_pct >= config.warn_pct:
        return [warned(
            check="volume_change",
            table=stats.table_name,
            layer=stats.layer,
            message=(
                f"Row count {direction} {change_pct:.1%} "
                f"({previous:,} → {current:,}, {sign}{abs(delta):,} rows)."
            ),
            expected=f"< {config.warn_pct:.1%} change",
            actual=f"{change_pct:.1%} {direction}",
        )]

    return [passed(
        check="volume_change",
        table=stats.table_name,
        layer=stats.layer,
        message=(
            f"Row count stable: {previous:,} → {current:,} "
            f"({sign}{abs(delta):,} rows, {change_pct:.1%} change)."
        ),
        actual=f"{change_pct:.1%} change",
    )]
