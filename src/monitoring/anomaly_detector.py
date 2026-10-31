from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np


@dataclass
class AnomalyReport:
    anomaly_type: str
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    detected: bool
    detected_value: float
    expected_range: tuple[float, float]
    description: str


class AnomalyDetector:
    """Detects anomalies in data pipeline metrics using statistical methods."""

    def detect_volume_anomaly(
        self, current_count: int, historical_counts: list[int]
    ) -> AnomalyReport:
        """Detect volume anomalies using IQR method."""
        if len(historical_counts) < 4:
            return AnomalyReport(
                anomaly_type="volume",
                severity="LOW",
                detected=False,
                detected_value=float(current_count),
                expected_range=(0.0, float(max(historical_counts, default=0))),
                description="Insufficient historical data for IQR analysis",
            )

        arr = np.array(historical_counts, dtype=float)
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        detected = current_count < lower_bound or current_count > upper_bound

        if not detected:
            severity = "LOW"
        else:
            # Determine severity by how far outside the fence we are
            median = float(np.median(arr))
            if median == 0:
                pct_deviation = 1.0
            else:
                pct_deviation = abs(current_count - median) / median

            if pct_deviation > 0.8:
                severity = "CRITICAL"
            elif pct_deviation > 0.5:
                severity = "HIGH"
            elif pct_deviation > 0.25:
                severity = "MEDIUM"
            else:
                severity = "LOW"

        return AnomalyReport(
            anomaly_type="volume",
            severity=severity,
            detected=detected,
            detected_value=float(current_count),
            expected_range=(max(0.0, lower_bound), upper_bound),
            description=(
                f"Row count {current_count} is {'within' if not detected else 'OUTSIDE'} "
                f"expected range [{max(0, lower_bound):.0f}, {upper_bound:.0f}] "
                f"(IQR method, historical median={np.median(arr):.0f})"
            ),
        )

    def detect_latency_anomaly(
        self, current_latency_ms: float, historical_latency: list[float]
    ) -> AnomalyReport:
        """Detect latency anomalies using Z-score method."""
        if len(historical_latency) < 3:
            return AnomalyReport(
                anomaly_type="latency",
                severity="LOW",
                detected=False,
                detected_value=current_latency_ms,
                expected_range=(0.0, float("inf")),
                description="Insufficient historical data for Z-score analysis",
            )

        arr = np.array(historical_latency, dtype=float)
        mean = float(np.mean(arr))
        # Noise floor: a perfectly flat history (std=0) would otherwise make every
        # deviation either invisible (z=0) or infinitely anomalous.
        std = max(float(np.std(arr)), 0.05 * abs(mean))

        if std == 0:
            z_score = 0.0 if current_latency_ms == mean else float("inf")
        else:
            z_score = abs(current_latency_ms - mean) / std

        detected = z_score > 3.0
        lower_bound = max(0.0, mean - 3 * std)
        upper_bound = mean + 3 * std

        if z_score > 6.0:
            severity = "CRITICAL"
        elif z_score > 4.0:
            severity = "HIGH"
        elif z_score > 3.0:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        return AnomalyReport(
            anomaly_type="latency",
            severity=severity,
            detected=detected,
            detected_value=current_latency_ms,
            expected_range=(lower_bound, upper_bound),
            description=(
                f"Latency {current_latency_ms:.1f}ms "
                f"{'is normal' if not detected else 'ANOMALOUS'} "
                f"(Z-score={z_score:.2f}, "
                f"expected range=[{lower_bound:.1f}, {upper_bound:.1f}] ms, "
                f"historical mean={mean:.1f}ms)"
            ),
        )

    def detect_schema_change(
        self, current_columns: list[str], expected_columns: list[str]
    ) -> AnomalyReport:
        """Detect schema changes by comparing column sets."""
        current_set = set(current_columns)
        expected_set = set(expected_columns)

        added = current_set - expected_set
        removed = expected_set - current_set
        detected = len(added) > 0 or len(removed) > 0

        if not detected:
            severity = "LOW"
        elif len(removed) > 0:
            # Removed columns are more severe than added
            severity = "CRITICAL" if len(removed) > 2 else "HIGH"
        else:
            severity = "MEDIUM" if len(added) > 2 else "LOW"

        return AnomalyReport(
            anomaly_type="schema_change",
            severity=severity,
            detected=detected,
            detected_value=float(len(added) + len(removed)),
            expected_range=(0.0, 0.0),
            description=(
                f"Schema {'changed' if detected else 'unchanged'}: "
                f"added={sorted(added)}, removed={sorted(removed)}"
            ),
        )

    def detect_freshness_violation(
        self, last_updated: datetime, max_age_hours: float
    ) -> AnomalyReport:
        """Detect data freshness violations."""
        now = datetime.utcnow()
        age_hours = (now - last_updated).total_seconds() / 3600.0
        detected = age_hours > max_age_hours
        overdue_ratio = age_hours / max_age_hours if max_age_hours > 0 else 0.0

        if not detected:
            severity = "LOW"
        elif overdue_ratio > 3.0:
            severity = "CRITICAL"
        elif overdue_ratio > 2.0:
            severity = "HIGH"
        elif overdue_ratio > 1.5:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        return AnomalyReport(
            anomaly_type="freshness",
            severity=severity,
            detected=detected,
            detected_value=age_hours,
            expected_range=(0.0, max_age_hours),
            description=(
                f"Data {'is STALE' if detected else 'is fresh'}: "
                f"last updated {age_hours:.1f}h ago, max allowed={max_age_hours:.1f}h. "
                f"Last updated at {last_updated.isoformat()}"
            ),
        )
