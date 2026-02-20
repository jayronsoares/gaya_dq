"""
tests.test_baseline
-------------------
Tests for BaselineStore read/write/conditional logic.
Uses a temp directory — no permanent filesystem side effects.
"""

import tempfile

import pytest
from gaya.baseline.store import BaselineStore, _has_changed
from gaya.checks.base import Layer
from tests.fixtures import make_baseline, make_stats


@pytest.fixture
def store(tmp_path):
    return BaselineStore(root=str(tmp_path))


class TestBaselineStore:

    def test_load_returns_none_on_first_run(self, store):
        assert store.load("orders") is None

    def test_save_writes_on_first_run(self, store):
        stats   = make_stats()
        written = store.save(stats)
        assert written is True

    def test_load_returns_correct_baseline_after_save(self, store):
        stats = make_stats(row_count=1000)
        store.save(stats)
        baseline = store.load("orders")
        assert baseline is not None
        assert baseline.table_name == "orders"
        assert baseline.row_count  == 1000
        assert baseline.run_count  == 1

    def test_save_skips_write_when_stats_unchanged(self, store):
        stats = make_stats()
        store.save(stats)
        written = store.save(stats)   # same stats, second save
        assert written is False

    def test_save_writes_and_increments_run_count_on_change(self, store):
        store.save(make_stats(row_count=1000))
        store.save(make_stats(row_count=1200))
        baseline = store.load("orders")
        assert baseline.row_count == 1200
        assert baseline.run_count == 2

    def test_save_writes_on_schema_change(self, store):
        store.save(make_stats(schema={"id": "int"}))
        written = store.save(make_stats(schema={"id": "int", "name": "string"}))
        assert written is True

    def test_exists_returns_false_before_save(self, store):
        assert store.exists("orders") is False

    def test_exists_returns_true_after_save(self, store):
        store.save(make_stats())
        assert store.exists("orders") is True

    def test_delete_removes_baseline(self, store):
        store.save(make_stats())
        store.delete("orders")
        assert store.load("orders") is None

    def test_list_tables_returns_saved_tables(self, store):
        store.save(make_stats(table_name="orders"))
        store.save(make_stats(table_name="customers"))
        tables = store.list_tables()
        assert "orders"    in tables
        assert "customers" in tables

    def test_schema_dot_notation_table_name_safe(self, store):
        stats = make_stats(table_name="public.orders")
        store.save(stats)
        assert store.exists("public.orders")

    def test_load_returns_none_on_corrupted_file(self, store, tmp_path):
        # Write a corrupt JSON file
        bl_dir = tmp_path / ".gaya" / "baselines"
        bl_dir.mkdir(parents=True)
        (bl_dir / "orders.json").write_text("{ not valid json }", encoding="utf-8")
        assert store.load("orders") is None


class TestHasChanged:

    def test_same_row_count_and_schema_not_changed(self):
        b = make_baseline(row_count=1000, schema={"id": "int"})
        s = make_stats(row_count=1000,   schema={"id": "int"})
        assert _has_changed(b, s) is False

    def test_different_row_count_is_changed(self):
        b = make_baseline(row_count=1000)
        s = make_stats(row_count=999)
        assert _has_changed(b, s) is True

    def test_different_schema_is_changed(self):
        b = make_baseline(schema={"id": "int"})
        s = make_stats(schema={"id": "int", "name": "string"})
        assert _has_changed(b, s) is True
