"""
tests.test_uniqueness
---------------------
Tests for check_unique (single and composite key).
"""

from gaya.checks.base import Status, UniqueConfig
from gaya.checks.uniqueness import check_unique
from tests.fixtures import make_column, make_stats


class TestUnique:

    def test_fully_unique_column_passes(self):
        stats = make_stats(columns=[
            make_column("order_id", row_count=1000, distinct_count=1000)
        ])
        results = check_unique(stats, UniqueConfig(columns=("order_id",)))
        assert results[0].status == Status.PASS

    def test_duplicate_column_fails(self):
        stats = make_stats(columns=[
            make_column("order_id", row_count=1000, distinct_count=980)  # 20 dupes
        ])
        results = check_unique(stats, UniqueConfig(columns=("order_id",)))
        assert results[0].status == Status.FAIL
        assert "20" in results[0].message

    def test_missing_column_fails(self):
        stats   = make_stats()
        results = check_unique(stats, UniqueConfig(columns=("nonexistent",)))
        assert results[0].status == Status.FAIL
        assert "does not exist" in results[0].message

    def test_hint_present_on_duplicate_failure(self):
        stats = make_stats(columns=[
            make_column("order_id", row_count=1000, distinct_count=900)
        ])
        results = check_unique(stats, UniqueConfig(columns=("order_id",)))
        assert results[0].hint is not None

    def test_composite_key_unique_passes(self):
        composite = make_column(
            "order_id|customer_id",
            row_count=1000, distinct_count=1000
        )
        stats   = make_stats(columns=[composite])
        results = check_unique(stats, UniqueConfig(columns=("order_id", "customer_id")))
        assert results[0].status == Status.PASS

    def test_composite_key_with_duplicates_fails(self):
        composite = make_column(
            "order_id|customer_id",
            row_count=1000, distinct_count=990
        )
        stats   = make_stats(columns=[composite])
        results = check_unique(stats, UniqueConfig(columns=("order_id", "customer_id")))
        assert results[0].status == Status.FAIL
        assert "10" in results[0].message

    def test_composite_key_missing_fails(self):
        stats   = make_stats()   # no composite column present
        results = check_unique(stats, UniqueConfig(columns=("order_id", "customer_id")))
        assert results[0].status == Status.FAIL
