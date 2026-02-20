"""
gaya.output.reporter
--------------------
The only place in Gaya that writes to stdout.

Three output modes:
    default  — human-readable, designed to be read in a terminal
    --quiet  — one line per failure, greppable, CI-friendly
    --json   — structured, for custom tooling and GitHub Actions

Exit code contract (enforced by CLI, signaled here via RunSummary):
    0 — all checks passed
    1 — warnings only
    2 — one or more failures
    3 — Gaya infrastructure error (connection, config, crash)

Design rules:
    - Reporter never makes decisions — it displays what Runner returned
    - No color codes by default (works in all CI environments)
    - Output is deterministic — same results always produce same output
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Optional

from gaya.checks.base import Status
from gaya.runner import RunResult


# ---------------------------------------------------------------------------
# RunSummary — aggregates results across all tables
# ---------------------------------------------------------------------------

@dataclass
class RunSummary:
    results:       list[RunResult]
    duration_secs: float

    @property
    def total_checks(self) -> int:
        return sum(len(r.results) for r in self.results)

    @property
    def total_passed(self) -> int:
        return sum(len(r.passed) for r in self.results)

    @property
    def total_warnings(self) -> int:
        return sum(len(r.warnings) for r in self.results)

    @property
    def total_failures(self) -> int:
        return sum(len(r.failures) for r in self.results)

    @property
    def exit_code(self) -> int:
        if any(r.error for r in self.results):
            return 3
        if any(r.has_failures for r in self.results):
            return 2
        if any(r.has_warnings for r in self.results):
            return 1
        return 0

    @property
    def has_failures(self) -> bool:
        return self.total_failures > 0

    @property
    def has_warnings(self) -> bool:
        return self.total_warnings > 0


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class Reporter:
    """
    Formats and writes RunSummary to stdout.

    Usage:
        reporter = Reporter(quiet=False, json_mode=False)
        reporter.print(summary)
        sys.exit(summary.exit_code)
    """

    def __init__(self, quiet: bool = False, json_mode: bool = False):
        self.quiet     = quiet
        self.json_mode = json_mode

    def print(self, summary: RunSummary) -> None:
        if self.json_mode:
            self._print_json(summary)
        elif self.quiet:
            self._print_quiet(summary)
        else:
            self._print_human(summary)

    # -- Human mode ----------------------------------------------------------

    def _print_human(self, summary: RunSummary) -> None:
        _line()

        # Per-table blocks — only show tables with issues first
        issues = [r for r in summary.results if r.has_failures or r.has_warnings]
        clean  = [r for r in summary.results if not r.has_failures and not r.has_warnings]

        for result in issues:
            _print_table_block(result)

        # Summary footer
        _line()

        if summary.total_failures == 0 and summary.total_warnings == 0:
            _out(f"  All checks passed ({summary.total_checks} checks across {len(summary.results)} table(s))")
        else:
            parts = []
            if summary.total_failures:
                parts.append(f"{summary.total_failures} failed")
            if summary.total_warnings:
                parts.append(f"{summary.total_warnings} warned")
            if summary.total_passed:
                parts.append(f"{summary.total_passed} passed")
            _out(f"  {len(summary.results)} table(s) · {' · '.join(parts)}")

        _out(f"  Finished in {summary.duration_secs:.1f}s")
        _line()

        # Clean tables listed compactly at the end
        if clean:
            for r in clean:
                _out(f"  ✔  {r.layer}.{r.table}  ({len(r.results)} checks passed)")
            _line()

    # -- Quiet mode ----------------------------------------------------------

    def _print_quiet(self, summary: RunSummary) -> None:
        """One line per non-passing result. Greppable."""
        for result in summary.results:
            if result.error:
                _out(f"ERROR {result.table} {result.error}")
                continue
            for r in result.failures:
                col = f"[{r.column}] " if r.column else ""
                _out(f"FAIL {result.layer}.{result.table} {col}{r.check}")
            for r in result.warnings:
                col = f"[{r.column}] " if r.column else ""
                _out(f"WARN {result.layer}.{result.table} {col}{r.check}")

    # -- JSON mode -----------------------------------------------------------

    def _print_json(self, summary: RunSummary) -> None:
        output = {
            "exit_code":      summary.exit_code,
            "total_checks":   summary.total_checks,
            "total_passed":   summary.total_passed,
            "total_warnings": summary.total_warnings,
            "total_failures": summary.total_failures,
            "duration_secs":  round(summary.duration_secs, 2),
            "tables": [
                {
                    "table":   r.table,
                    "layer":   r.layer,
                    "status":  r.overall_status.value,
                    "error":   r.error,
                    "checks": [
                        {
                            "check":    c.check,
                            "status":   c.status.value,
                            "column":   c.column,
                            "message":  c.message,
                            "expected": str(c.expected) if c.expected is not None else None,
                            "actual":   str(c.actual)   if c.actual   is not None else None,
                            "hint":     c.hint,
                        }
                        for c in r.results
                    ],
                }
                for r in summary.results
            ],
        }
        _out(json.dumps(output, indent=2))


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

_STATUS_ICON = {
    Status.PASS: "✔",
    Status.WARN: "⚠",
    Status.FAIL: "✖",
}

def _print_table_block(result: RunResult) -> None:
    icon = "✖" if result.has_failures else "⚠"
    status_label = "FAILED" if result.has_failures else "WARN"
    _out(f"\n  {icon}  {result.layer}.{result.table}  {status_label}")

    if result.error:
        _out(f"     Error: {result.error}")
        return

    # Show failures first, then warnings
    for check_result in result.failures + result.warnings:
        col_str = f" [{check_result.column}]" if check_result.column else ""
        icon    = _STATUS_ICON[check_result.status]
        _out(f"     {icon}{col_str}  {check_result.message}")
        if check_result.hint:
            _out(f"          → {check_result.hint}")


def _out(text: str) -> None:
    print(text, file=sys.stdout)


def _line() -> None:
    _out("  " + "─" * 54)
