"""
tests.test_schema
-----------------
Tests for check_schema and check_schema_drift.
"""

from gaya.checks.base import SchemaConfig, SchemaDriftConfig, Status
from gaya.checks.schema import check_schema, check_schema_drift
from tests.fixtures import make_baseline, make_stats


class TestSchema:

    def test_matching_schema_passes(self):
        stats   = make_stats()
        results = check_schema(stats, SchemaConfig(expected={"order_id": "int", "email": "string"}))
        assert all(r.status == Status.PASS for r in results)

    def test_dtype_mismatch_fails(self):
        stats   = make_stats()
        results = check_schema(stats, SchemaConfig(expected={"order_id": "varchar"}))
        assert results[0].status == Status.FAIL
        assert "int" in results[0].message
        assert "varchar" in results[0].message

    def test_missing_column_fails(self):
        stats   = make_stats()
        results = check_schema(stats, SchemaConfig(expected={"ghost_col": "int"}))
        assert results[0].status == Status.FAIL
        assert "missing" in results[0].message.lower()

    def test_case_insensitive_dtype_match(self):
        stats   = make_stats()
        # schema has 'int', config says 'INT' — should pass
        results = check_schema(stats, SchemaConfig(expected={"order_id": "INT"}))
        assert results[0].status == Status.PASS

    def test_hint_present_on_dtype_mismatch(self):
        stats   = make_stats()
        results = check_schema(stats, SchemaConfig(expected={"order_id": "text"}))
        assert results[0].hint is not None

    def test_multiple_columns_independent(self):
        stats   = make_stats()
        results = check_schema(stats, SchemaConfig(expected={
            "order_id":    "int",      # match
            "email":       "int",      # mismatch
            "nonexistent": "string",   # missing
        }))
        statuses = {r.column: r.status for r in results}
        assert statuses["order_id"]    == Status.PASS
        assert statuses["email"]       == Status.FAIL
        assert statuses["nonexistent"] == Status.FAIL


class TestSchemaDrift:

    def test_first_run_no_baseline_passes(self):
        stats   = make_stats()
        results = check_schema_drift(stats, SchemaDriftConfig(baseline_columns=()), baseline=None)
        assert results[0].status == Status.PASS
        assert "baseline" in results[0].message.lower()

    def test_no_drift_passes(self):
        stats    = make_stats()
        baseline = make_baseline()   # same schema as make_stats default
        results  = check_schema_drift(stats, SchemaDriftConfig(baseline_columns=()), baseline=baseline)
        assert results[0].status == Status.PASS

    def test_removed_columns_fail(self):
        stats    = make_stats()
        baseline = make_baseline(schema={
            "order_id": "int", "customer_id": "int",
            "email": "string", "status": "string",
            "removed_col": "string",     # was here, now gone
        })
        results = check_schema_drift(stats, SchemaDriftConfig(baseline_columns=()), baseline=baseline)
        failures = [r for r in results if r.status == Status.FAIL]
        assert len(failures) == 1
        assert "removed_col" in failures[0].message

    def test_added_columns_warn(self):
        stats    = make_stats()
        baseline = make_baseline(schema={"order_id": "int"})  # missing many cols
        results  = check_schema_drift(stats, SchemaDriftConfig(baseline_columns=()), baseline=baseline)
        warnings = [r for r in results if r.status == Status.WARN]
        assert len(warnings) == 1

    def test_both_added_and_removed_returns_two_results(self):
        stats    = make_stats()
        baseline = make_baseline(schema={
            "order_id":   "int",
            "old_column": "string",    # removed
            # customer_id, email, status are new → warn
        })
        results = check_schema_drift(stats, SchemaDriftConfig(baseline_columns=()), baseline=baseline)
        assert any(r.status == Status.FAIL for r in results)
        assert any(r.status == Status.WARN for r in results)
