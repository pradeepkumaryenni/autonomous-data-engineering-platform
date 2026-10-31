from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class QualityResult:
    check_type: str
    column: str
    passed: bool
    metric_value: float
    threshold: float | None
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityRule:
    column: str
    check_type: str  # "nulls", "duplicates", "schema", "range", "referential", "drift"
    params: dict[str, Any] = field(default_factory=dict)


class DataQualityEngine:
    """Engine for running data quality checks on pandas DataFrames."""

    def check_nulls(self, df: pd.DataFrame, column: str, threshold: float) -> QualityResult:
        """Fail if null percentage exceeds threshold (0.0-1.0)."""
        if column not in df.columns:
            return QualityResult(
                check_type="nulls",
                column=column,
                passed=False,
                metric_value=1.0,
                threshold=threshold,
                message=f"Column '{column}' not found in DataFrame",
            )
        null_count = int(df[column].isna().sum())
        total = len(df)
        null_pct = null_count / total if total > 0 else 0.0
        passed = null_pct <= threshold
        return QualityResult(
            check_type="nulls",
            column=column,
            passed=passed,
            metric_value=null_pct,
            threshold=threshold,
            message=(
                f"Null check {'passed' if passed else 'FAILED'}: "
                f"{null_pct:.2%} nulls in '{column}' (threshold: {threshold:.2%})"
            ),
            details={"null_count": null_count, "total_rows": total},
        )

    def check_duplicates(self, df: pd.DataFrame, key_columns: list[str]) -> QualityResult:
        """Count duplicate rows based on key columns."""
        valid_cols = [c for c in key_columns if c in df.columns]
        if not valid_cols:
            return QualityResult(
                check_type="duplicates",
                column=str(key_columns),
                passed=False,
                metric_value=0.0,
                threshold=0.0,
                message=f"None of the key columns {key_columns} found in DataFrame",
            )
        duplicate_mask = df.duplicated(subset=valid_cols, keep="first")
        dup_count = int(duplicate_mask.sum())
        total = len(df)
        dup_pct = dup_count / total if total > 0 else 0.0
        passed = dup_count == 0
        return QualityResult(
            check_type="duplicates",
            column=", ".join(valid_cols),
            passed=passed,
            metric_value=dup_pct,
            threshold=0.0,
            message=(
                f"Duplicate check {'passed' if passed else 'FAILED'}: "
                f"{dup_count} duplicates found ({dup_pct:.2%}) on columns {valid_cols}"
            ),
            details={"duplicate_count": dup_count, "total_rows": total, "key_columns": valid_cols},
        )

    def check_schema(self, df: pd.DataFrame, expected_schema: dict[str, str]) -> QualityResult:
        """Validate column dtypes against expected schema. expected_schema: {col: dtype_str}."""
        mismatches: dict[str, dict[str, str]] = {}
        missing_cols = []
        for col, expected_dtype in expected_schema.items():
            if col not in df.columns:
                missing_cols.append(col)
                continue
            actual_dtype = str(df[col].dtype)
            # Flexible matching: int64 matches int, float64 matches float, etc.
            if not (expected_dtype in actual_dtype or actual_dtype.startswith(expected_dtype)):
                mismatches[col] = {"expected": expected_dtype, "actual": actual_dtype}

        passed = len(mismatches) == 0 and len(missing_cols) == 0
        return QualityResult(
            check_type="schema",
            column="*",
            passed=passed,
            metric_value=float(len(mismatches) + len(missing_cols)),
            threshold=0.0,
            message=(
                f"Schema check {'passed' if passed else 'FAILED'}: "
                f"{len(mismatches)} type mismatches, {len(missing_cols)} missing columns"
            ),
            details={"mismatched_columns": mismatches, "missing_columns": missing_cols},
        )

    def check_value_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_val: Any,
        max_val: Any,
    ) -> QualityResult:
        """Fail if any value in column is outside [min_val, max_val]."""
        if column not in df.columns:
            return QualityResult(
                check_type="range",
                column=column,
                passed=False,
                metric_value=0.0,
                threshold=None,
                message=f"Column '{column}' not found in DataFrame",
            )
        series = df[column].dropna()
        if len(series) == 0:
            return QualityResult(
                check_type="range",
                column=column,
                passed=True,
                metric_value=0.0,
                threshold=None,
                message=f"Column '{column}' has no non-null values to check",
            )
        violations = series[(series < min_val) | (series > max_val)]
        violation_count = len(violations)
        violation_pct = violation_count / len(series)
        passed = violation_count == 0
        return QualityResult(
            check_type="range",
            column=column,
            passed=passed,
            metric_value=violation_pct,
            threshold=0.0,
            message=(
                f"Range check {'passed' if passed else 'FAILED'}: "
                f"{violation_count} values outside [{min_val}, {max_val}] in '{column}'"
            ),
            details={
                "violation_count": violation_count,
                "min_val": min_val,
                "max_val": max_val,
                "actual_min": float(series.min()),
                "actual_max": float(series.max()),
            },
        )

    def check_referential_integrity(
        self, df: pd.DataFrame, column: str, valid_values: set
    ) -> QualityResult:
        """Fail if any value in column is not in valid_values."""
        if column not in df.columns:
            return QualityResult(
                check_type="referential",
                column=column,
                passed=False,
                metric_value=0.0,
                threshold=None,
                message=f"Column '{column}' not found in DataFrame",
            )
        series = df[column].dropna()
        invalid = series[~series.isin(valid_values)]
        invalid_count = len(invalid)
        invalid_pct = invalid_count / len(series) if len(series) > 0 else 0.0
        passed = invalid_count == 0
        return QualityResult(
            check_type="referential",
            column=column,
            passed=passed,
            metric_value=invalid_pct,
            threshold=0.0,
            message=(
                f"Referential integrity check {'passed' if passed else 'FAILED'}: "
                f"{invalid_count} invalid values in '{column}'"
            ),
            details={
                "invalid_count": invalid_count,
                "sample_invalid_values": list(invalid.unique()[:10]),
                "valid_values_count": len(valid_values),
            },
        )

    def check_statistical_drift(
        self,
        df: pd.DataFrame,
        column: str,
        baseline_mean: float,
        baseline_std: float,
        z_threshold: float = 3.0,
    ) -> QualityResult:
        """Z-score drift detection: fail if current mean deviates > z_threshold std devs."""
        if column not in df.columns:
            return QualityResult(
                check_type="drift",
                column=column,
                passed=False,
                metric_value=0.0,
                threshold=z_threshold,
                message=f"Column '{column}' not found in DataFrame",
            )
        series = df[column].dropna()
        if len(series) == 0 or baseline_std == 0:
            return QualityResult(
                check_type="drift",
                column=column,
                passed=True,
                metric_value=0.0,
                threshold=z_threshold,
                message=f"Cannot compute drift for '{column}' (empty or zero std)",
            )
        current_mean = float(series.mean())
        z_score = abs((current_mean - baseline_mean) / baseline_std)
        passed = z_score <= z_threshold
        return QualityResult(
            check_type="drift",
            column=column,
            passed=passed,
            metric_value=z_score,
            threshold=z_threshold,
            message=(
                f"Statistical drift check {'passed' if passed else 'FAILED'}: "
                f"Z-score={z_score:.2f} (threshold={z_threshold}) for '{column}'. "
                f"Current mean={current_mean:.4f}, baseline mean={baseline_mean:.4f}"
            ),
            details={
                "current_mean": current_mean,
                "baseline_mean": baseline_mean,
                "baseline_std": baseline_std,
                "z_score": z_score,
                "current_std": float(series.std()),
            },
        )

    def run_all_checks(self, df: pd.DataFrame, rules: list[QualityRule]) -> list[QualityResult]:
        """Run all quality rules and return results."""
        results: list[QualityResult] = []
        for rule in rules:
            ct = rule.check_type
            try:
                if ct == "nulls":
                    result = self.check_nulls(df, rule.column, rule.params.get("threshold", 0.05))
                elif ct == "duplicates":
                    key_cols = rule.params.get("key_columns", [rule.column])
                    result = self.check_duplicates(df, key_cols)
                elif ct == "schema":
                    result = self.check_schema(df, rule.params.get("expected_schema", {}))
                elif ct == "range":
                    result = self.check_value_range(
                        df,
                        rule.column,
                        rule.params.get("min_val"),
                        rule.params.get("max_val"),
                    )
                elif ct == "referential":
                    result = self.check_referential_integrity(
                        df, rule.column, set(rule.params.get("valid_values", []))
                    )
                elif ct == "drift":
                    result = self.check_statistical_drift(
                        df,
                        rule.column,
                        rule.params.get("baseline_mean", 0.0),
                        rule.params.get("baseline_std", 1.0),
                        rule.params.get("z_threshold", 3.0),
                    )
                else:
                    result = QualityResult(
                        check_type=ct,
                        column=rule.column,
                        passed=False,
                        metric_value=0.0,
                        threshold=None,
                        message=f"Unknown check type: {ct}",
                    )
            except Exception as exc:
                result = QualityResult(
                    check_type=ct,
                    column=rule.column,
                    passed=False,
                    metric_value=0.0,
                    threshold=None,
                    message=f"Check failed with exception: {exc}",
                )
            results.append(result)
        return results
