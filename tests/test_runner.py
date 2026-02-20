"""
tests.test_runner
-----------------
Tests for Runner orchestration — checks fire in the right order,
baselines update conditionally, RunResult aggregates correctly.
"""

import tempfile

import pytest
from gaya.baseline.store import BaselineStore
from gaya.checks.base import NullConfig, RequiredConfig, Status, UniqueConfig, VolumeChangeConfig
from gaya.runner import Runner, RunResult, TableConfig
from tests.fixtures import make_column, make_stats


def make_config(**kwargs) -> TableConfig:
    defaults = dict(
        table="orders",
        layer="staging",
        source="main_db",
        null=NullConfig(warn_pct=0.10, fail_pct=0.25),
        volume=VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40),
    )
    defaults.update(kwargs)
    return TableConfig(**defaults)


@pytest.fixture
def store(tmp_path):
    return BaselineStore(root=str(tmp_path))


class TestRunner:

    def test_first_run_establishes_baseline(self, store):
        stats  = make_stats()
        config = make_config()
        result = Runner(store=store).run(stats, config)
        assert result.baseline_updated is True
        assert store.exists("orders")

    def test_second_run_same_data_does_not_update_baseline(self, store):
        stats  = make_stats()
        config = make_config()
        runner = Runner(store=store)
        runner.run(stats, config)
        result = runner.run(stats, config)
        assert result.baseline_updated is False

    def test_required_column_failure_captured(self, store):
        stats = make_stats(columns=[
            make_column("order_id",    null_count=10),  # should be required, has nulls
            make_column("customer_id", null_count=0),
        ])
        config = make_config(
            required=RequiredConfig(columns=("order_id",)),
            null=None,
            run_null_check=False,
            run_volume_check=False,
            run_drift_check=False,
        )
        result = Runner(store=store).run(stats, config)
        assert result.has_failures

    def test_unique_failure_captured(self, store):
        stats = make_stats(columns=[
            make_column("order_id", row_count=1000, distinct_count=900)  # 100 dupes
        ])
        config = make_config(
            unique=UniqueConfig(columns=("order_id",)),
            null=None,
            run_null_check=False,
            run_volume_check=False,
            run_drift_check=False,
        )
        result = Runner(store=store).run(stats, config)
        assert result.has_failures

    def test_overall_status_is_fail_when_any_check_fails(self, store):
        stats = make_stats(columns=[
            make_column("order_id", null_count=300)  # 30% null — above fail threshold
        ])
        config = make_config(
            null=NullConfig(fail_pct=0.25),
            run_volume_check=False,
            run_drift_check=False,
        )
        result = Runner(store=store).run(stats, config)
        assert result.overall_status == Status.FAIL

    def test_overall_status_is_warn_when_only_warnings(self, store):
        stats = make_stats(columns=[
            make_column("email", null_count=150)  # 15% — above warn, below fail
        ])
        config = make_config(
            null=NullConfig(warn_pct=0.10, fail_pct=0.25),
            run_volume_check=False,
            run_drift_check=False,
        )
        result = Runner(store=store).run(stats, config)
        assert result.overall_status == Status.WARN

    def test_clean_run_is_pass(self, store):
        stats = make_stats(columns=[
            make_column("order_id", null_count=0, distinct_count=1000)
        ])
        config = make_config(
            null=NullConfig(warn_pct=0.10, fail_pct=0.25),
            run_volume_check=False,
            run_drift_check=False,
        )
        result = Runner(store=store).run(stats, config)
        assert result.overall_status == Status.PASS
