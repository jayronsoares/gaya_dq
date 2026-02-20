"""
tests.test_completeness
-----------------------
Tests for check_null_rate and check_required_columns.
Zero I/O — all inputs are plain dataclasses.
"""

import pytest
from gaya.checks.base import NullConfig, RequiredConfig, Status
from gaya.checks.completeness import check_null_rate, check_required_columns
from tests.fixtures import make_column, make_stats


class TestNullRate:

    def test_no_nulls_passes(self):
        stats = make_stats(columns=[make_column("id", null_count=0)])
        results = check_null_rate(stats, NullConfig(warn_pct=0.10, fail_pct=0.25))
        assert len(results) == 1
        assert results[0].status == Status.PASS

    def test_below_warn_threshold_passes(self):
        stats = make_stats(columns=[make_column("email", null_count=50)])  # 5%
        results = check_null_rate(stats, NullConfig(warn_pct=0.10, fail_pct=0.25))
        assert results[0].status == Status.PASS

    def test_above_warn_threshold_warns(self):
        stats = make_stats(columns=[make_column("email", null_count=120)])  # 12%
        results = check_null_rate(stats, NullConfig(warn_pct=0.10, fail_pct=0.25))
        assert results[0].status == Status.WARN

    def test_above_fail_threshold_fails(self):
        stats = make_stats(columns=[make_column("phone", null_count=280)])  # 28%
        results = check_null_rate(stats, NullConfig(warn_pct=0.10, fail_pct=0.25))
        assert results[0].status == Status.FAIL

    def test_checks_all_columns_by_default(self):
        stats = make_stats()  # 4 columns from fixtures
        results = check_null_rate(stats, NullConfig())
        assert len(results) == 4

    def test_checks_specified_columns_only(self):
        stats = make_stats()
        results = check_null_rate(stats, NullConfig(columns=("order_id", "email")))
        assert len(results) == 2
        assert {r.column for r in results} == {"order_id", "email"}

    def test_missing_specified_column_fails(self):
        stats = make_stats()
        results = check_null_rate(stats, NullConfig(columns=("ghost_col",)))
        assert results[0].status == Status.FAIL

    def test_result_is_immutable(self):
        stats = make_stats(columns=[make_column("id")])
        results = check_null_rate(stats, NullConfig())
        with pytest.raises((AttributeError, TypeError)):
            results[0].status = Status.FAIL


class TestRequiredColumns:

    def test_complete_required_column_passes(self):
        stats = make_stats(columns=[make_column("order_id", null_count=0)])
        results = check_required_columns(stats, RequiredConfig(columns=("order_id",)))
        assert results[0].status == Status.PASS

    def test_null_in_required_column_fails(self):
        stats = make_stats(columns=[make_column("order_id", null_count=5)])
        results = check_required_columns(stats, RequiredConfig(columns=("order_id",)))
        assert results[0].status == Status.FAIL
        assert "5" in results[0].message

    def test_missing_required_column_fails(self):
        stats = make_stats()
        results = check_required_columns(stats, RequiredConfig(columns=("nonexistent",)))
        assert results[0].status == Status.FAIL
        assert "missing" in results[0].message.lower()

    def test_multiple_columns_independent_results(self):
        stats = make_stats(columns=[
            make_column("order_id",    null_count=0),
            make_column("customer_id", null_count=10),
        ])
        results = check_required_columns(
            stats, RequiredConfig(columns=("order_id", "customer_id"))
        )
        assert len(results) == 2
        assert results[0].status == Status.PASS
        assert results[1].status == Status.FAIL

    def test_hint_present_on_null_failure(self):
        stats = make_stats(columns=[make_column("order_id", null_count=1)])
        results = check_required_columns(stats, RequiredConfig(columns=("order_id",)))
        assert results[0].hint is not None
