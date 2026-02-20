"""
tests.test_volume
-----------------
Tests for check_row_count and check_volume_change.
"""

import pytest
from gaya.checks.base import RowCountConfig, Status, VolumeChangeConfig
from gaya.checks.volume import check_row_count, check_volume_change
from tests.fixtures import make_baseline, make_stats


class TestRowCount:

    def test_within_range_passes(self):
        stats   = make_stats(row_count=1000)
        results = check_row_count(stats, RowCountConfig(min_rows=500, max_rows=2000))
        assert results[0].status == Status.PASS

    def test_below_min_fails(self):
        stats   = make_stats(row_count=400)
        results = check_row_count(stats, RowCountConfig(min_rows=500))
        assert results[0].status == Status.FAIL
        assert "400" in results[0].message

    def test_above_max_fails(self):
        stats   = make_stats(row_count=3000)
        results = check_row_count(stats, RowCountConfig(max_rows=2000))
        assert results[0].status == Status.FAIL
        assert "3,000" in results[0].message

    def test_no_bounds_always_passes(self):
        stats   = make_stats(row_count=1)
        results = check_row_count(stats, RowCountConfig())
        assert results[0].status == Status.PASS

    def test_hint_present_on_failure(self):
        stats   = make_stats(row_count=0)
        results = check_row_count(stats, RowCountConfig(min_rows=100))
        assert results[0].hint is not None


class TestVolumeChange:

    def test_first_run_no_baseline_passes(self):
        stats   = make_stats(row_count=1000)
        results = check_volume_change(stats, VolumeChangeConfig(), baseline=None)
        assert results[0].status == Status.PASS
        assert "baseline" in results[0].message.lower()

    def test_stable_volume_passes(self):
        stats    = make_stats(row_count=1000)
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert results[0].status == Status.PASS

    def test_small_change_passes(self):
        stats    = make_stats(row_count=1050)   # +5%
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert results[0].status == Status.PASS

    def test_medium_drop_warns(self):
        stats    = make_stats(row_count=750)    # -25%
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert results[0].status == Status.WARN

    def test_large_drop_fails(self):
        stats    = make_stats(row_count=500)    # -50%
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert results[0].status == Status.FAIL
        assert "dropped" in results[0].message

    def test_large_spike_fails(self):
        stats    = make_stats(row_count=2000)   # +100%
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert results[0].status == Status.FAIL
        assert "increased" in results[0].message

    def test_message_includes_actual_counts(self):
        stats    = make_stats(row_count=600)
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(warn_pct=0.20, fail_pct=0.40), baseline)
        assert "1,000" in results[0].message
        assert "600"   in results[0].message

    def test_hint_present_on_large_drop(self):
        stats    = make_stats(row_count=100)
        baseline = make_baseline(row_count=1000)
        results  = check_volume_change(stats, VolumeChangeConfig(fail_pct=0.40), baseline)
        assert results[0].hint is not None
