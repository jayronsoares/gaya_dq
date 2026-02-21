"""
gaya.cli
--------
The CLI entrypoint. Two commands:

    gaya init   — generate a starter gaya.yml
    gaya run    — run all configured checks

Exit codes:
    0  all passed
    1  warnings only
    2  one or more failures
    3  infrastructure error (connection, config, crash)

The CLI is intentionally thin:
    - parse args
    - call pure core via config.loader
    - display via reporter
    - exit with code

No business logic here.
"""

from __future__ import annotations

import argparse
import sys
import time

from gaya.baseline.store import BaselineStore
from gaya.output.reporter import Reporter, RunSummary
from gaya.runner import Runner, RunResult, TableConfig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="gaya",
        description="Simple data quality checks that just work.",
    )
    sub = parser.add_subparsers(dest="command")

    # gaya run
    run_parser = sub.add_parser("run", help="Run all configured checks")
    run_parser.add_argument("--quiet",   action="store_true", help="One line per issue (CI-friendly)")
    run_parser.add_argument("--json",    action="store_true", help="JSON output")
    run_parser.add_argument("--config",  default="gaya.yml",  help="Path to config file")
    run_parser.add_argument("--dry-run", action="store_true", help="Run checks, do not update baselines")

    # gaya init
    sub.add_parser("init", help="Generate a starter gaya.yml")

    args = parser.parse_args()

    if args.command == "init":
        _cmd_init()
    elif args.command == "run":
        _cmd_run(args)
    else:
        parser.print_help()
        sys.exit(0)


# ---------------------------------------------------------------------------
# gaya init
# ---------------------------------------------------------------------------

def _cmd_init() -> None:
    from pathlib import Path

    target = Path("gaya.yml")
    if target.exists():
        print("gaya.yml already exists. Remove it to re-initialise.")
        sys.exit(0)

    starter = """\
# gaya.yml — data quality configuration
# Run `gaya run` to execute all checks.

datasources:
  main_db:
    type: postgres
    host: localhost
    port: 5432
    database: your_database
    user: your_user
    password: env:DB_PASSWORD     # export DB_PASSWORD=...

tables:
  orders:
    source: main_db
    layer: staging
    primary_key: order_id
    not_null:
      - order_id
      - customer_id
      - order_date

  customers:
    source: main_db
    layer: staging
    primary_key: customer_id

# Global defaults (override per table if needed)
defaults:
  volume_warn_pct: 20
  volume_fail_pct: 40
  null_warn_pct:   10
  null_fail_pct:   25
"""
    target.write_text(starter, encoding="utf-8")
    print("gaya.yml created. Edit it, then run: gaya run")


# ---------------------------------------------------------------------------
# gaya run
# ---------------------------------------------------------------------------

def _cmd_run(args: argparse.Namespace) -> None:
    reporter = Reporter(quiet=args.quiet, json_mode=args.json)
    store    = BaselineStore()
    runner   = Runner(store=store)

    try:
        from gaya.config.loader import load
        datasources, table_configs = load(args.config)
    except FileNotFoundError as exc:
        _fatal(str(exc), exit_code=3)
    except Exception as exc:
        _fatal(f"Config error: {exc}", exit_code=3)

    run_results: list[RunResult] = []
    start = time.monotonic()

    for table_config in table_configs:
        result = _run_table(table_config, datasources, runner, dry_run=args.dry_run)
        run_results.append(result)

    duration = time.monotonic() - start
    summary  = RunSummary(results=run_results, duration_secs=duration)

    reporter.print(summary)
    sys.exit(summary.exit_code)


def _run_table(config: TableConfig, datasources: dict, runner: Runner, dry_run: bool) -> RunResult:
    try:
        adapter = _build_adapter(config, datasources)
        stats   = adapter.collect(config.table, config.layer)
    except Exception as exc:
        return RunResult(table=config.table, layer=config.layer, error=str(exc))

    if dry_run:
        class _DryRunStore(BaselineStore):
            def save(self, *a, **kw) -> bool:
                return False
        result = Runner(store=_DryRunStore()).run(stats, config)
    else:
        result = runner.run(stats, config)

    return result


def _build_adapter(config: TableConfig, datasources: dict):
    ds_cfg  = datasources.get(config.source, {})
    ds_type = ds_cfg.get("type", "postgres")

    if ds_type == "postgres":
        from gaya.adapters.postgres import PostgresAdapter
        return PostgresAdapter.from_config(ds_cfg)

    raise ValueError(
        f"Unknown datasource type '{ds_type}' for source '{config.source}'. "
        f"Supported: postgres"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fatal(message: str, exit_code: int = 3) -> None:
    print(f"\n  ✖  {message}\n", file=sys.stderr)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
