"""Tests for DataQualityEngine."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.monitoring.quality_rules import QualityRule


class TestNullChecks:
    def test_fails_above_threshold(self, quality_engine):
        df = pd.DataFrame({"amount": [None] * 20 + [100.0] * 80})
        result = quality_engine.check_nulls(df, "amount", threshold=0.1)
        assert not result.passed
        assert abs(result.metric_value - 0.20) < 0.01
        assert "FAILED" in result.message

    def test_passes_below_threshold(self, quality_engine):
        df = pd.DataFrame({"amount": [None] * 2 + [100.0] * 98})
        result = quality_engine.check_nulls(df, "amount", threshold=0.1)
        assert result.passed
        assert result.metric_value < 0.1

    def test_missing_column_fails(self, quality_engine, clean_dataframe):
        result = quality_engine.check_nulls(clean_dataframe, "nonexistent_col", threshold=0.05)
        assert not result.passed


class TestDuplicateChecks:
    def test_detects_duplicates(self, quality_engine):
        df = pd.DataFrame({"id": [1, 2, 3, 1, 2], "val": ["a", "b", "c", "a", "b"]})
        result = quality_engine.check_duplicates(df, key_columns=["id"])
        assert not result.passed
        assert result.details["duplicate_count"] == 2

    def test_no_duplicates_passes(self, quality_engine, clean_dataframe):
        result = quality_engine.check_duplicates(clean_dataframe, key_columns=["id"])
        assert result.passed
        assert result.details["duplicate_count"] == 0

    def test_sample_dataframe_has_duplicates(self, quality_engine, sample_dataframe):
        result = quality_engine.check_duplicates(sample_dataframe, key_columns=["id"])
        assert not result.passed


class TestSchemaValidation:
    def test_correct_schema_passes(self, quality_engine):
        df = pd.DataFrame({"amount": [1.0, 2.0], "status": ["ok", "fail"]})
        result = quality_engine.check_schema(
            df, expected_schema={"amount": "float64", "status": "object"}
        )
        assert result.passed

    def test_wrong_dtype_fails(self, quality_engine):
        df = pd.DataFrame({"amount": ["not_a_number", "also_not"]})
        result = quality_engine.check_schema(df, expected_schema={"amount": "float64"})
        assert not result.passed
        assert "amount" in result.details.get("mismatched_columns", {})

    def test_missing_column_fails(self, quality_engine):
        df = pd.DataFrame({"amount": [1.0]})
        result = quality_engine.check_schema(
            df, expected_schema={"amount": "float64", "customer_id": "int64"}
        )
        assert not result.passed


class TestValueRange:
    def test_in_range_passes(self, quality_engine):
        df = pd.DataFrame({"amount": [10.0, 50.0, 99.0]})
        result = quality_engine.check_value_range(df, "amount", min_val=0, max_val=100)
        assert result.passed

    def test_out_of_range_fails(self, quality_engine):
        df = pd.DataFrame({"amount": [10.0, 50.0, 5000.0, -5.0]})
        result = quality_engine.check_value_range(df, "amount", min_val=0, max_val=1000)
        assert not result.passed
        assert result.details["violation_count"] >= 1


class TestStatisticalDrift:
    def test_detects_significant_drift(self, quality_engine):
        # baseline: mean=100, std=10 → inject values at mean=200 (10 std devs away)
        df = pd.DataFrame({"amount": [200.0] * 50 + [105.0] * 50})
        result = quality_engine.check_statistical_drift(
            df, "amount", baseline_mean=100.0, baseline_std=10.0, z_threshold=3.0
        )
        assert not result.passed

    def test_no_drift_passes(self, quality_engine):
        rng = np.random.default_rng(7)
        values = rng.normal(loc=100.0, scale=10.0, size=200)
        df = pd.DataFrame({"amount": values})
        result = quality_engine.check_statistical_drift(
            df, "amount", baseline_mean=100.0, baseline_std=10.0, z_threshold=3.0
        )
        assert result.passed


class TestRunAllChecks:
    def test_returns_results_for_all_rules(self, quality_engine, sample_dataframe):
        rules = [
            QualityRule(column="amount", check_type="nulls", params={"threshold": 0.1}),
            QualityRule(column="id", check_type="duplicates", params={"key_columns": ["id"]}),
            QualityRule(
                column="amount", check_type="range", params={"min_val": 0, "max_val": 100000}
            ),
        ]
        results = quality_engine.run_all_checks(sample_dataframe, rules)
        assert len(results) == 3
        assert all(hasattr(r, "passed") for r in results)
