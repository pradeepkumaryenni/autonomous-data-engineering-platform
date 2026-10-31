"""Tests for AnomalyDetector."""

from __future__ import annotations

from datetime import datetime, timedelta


class TestVolumeAnomaly:
    def test_critical_volume_drop(self, anomaly_detector):
        historical = [10000] * 20
        report = anomaly_detector.detect_volume_anomaly(
            current_count=50, historical_counts=historical
        )
        assert report is not None
        assert report.severity in ("HIGH", "CRITICAL")

    def test_normal_volume_no_anomaly(self, anomaly_detector):
        historical = [10000] * 20
        report = anomaly_detector.detect_volume_anomaly(
            current_count=9800, historical_counts=historical
        )
        assert report is None or report.severity == "LOW"

    def test_volume_spike_detected(self, anomaly_detector):
        historical = [1000] * 20
        report = anomaly_detector.detect_volume_anomaly(
            current_count=50000, historical_counts=historical
        )
        assert report is not None
        assert report.severity in ("HIGH", "CRITICAL")

    def test_insufficient_history_not_detected(self, anomaly_detector):
        # The detector always returns a report; `detected` says whether it is an anomaly
        report = anomaly_detector.detect_volume_anomaly(current_count=100, historical_counts=[100])
        assert report.detected is False
        assert "Insufficient historical data" in report.description


class TestLatencyAnomaly:
    def test_critical_latency_spike(self, anomaly_detector):
        historical = [100.0] * 30
        report = anomaly_detector.detect_latency_anomaly(
            current_latency_ms=8000.0, historical_latency=historical
        )
        assert report is not None
        assert report.severity in ("HIGH", "CRITICAL")

    def test_normal_latency_no_anomaly(self, anomaly_detector):
        historical = [100.0] * 30
        report = anomaly_detector.detect_latency_anomaly(
            current_latency_ms=105.0, historical_latency=historical
        )
        assert report is None or report.severity == "LOW"


class TestSchemaChange:
    def test_added_column_detected(self, anomaly_detector):
        current = ["id", "amount", "customer_id", "new_unexpected_column"]
        expected = ["id", "amount", "customer_id"]
        report = anomaly_detector.detect_schema_change(
            current_columns=current, expected_columns=expected
        )
        assert report is not None
        assert "new_unexpected_column" in report.description

    def test_dropped_column_detected(self, anomaly_detector):
        current = ["id", "amount"]
        expected = ["id", "amount", "customer_id"]
        report = anomaly_detector.detect_schema_change(
            current_columns=current, expected_columns=expected
        )
        assert report is not None
        assert report.severity in ("HIGH", "CRITICAL")

    def test_no_change_not_detected(self, anomaly_detector):
        cols = ["id", "amount", "customer_id"]
        report = anomaly_detector.detect_schema_change(current_columns=cols, expected_columns=cols)
        assert report.detected is False
        assert report.severity == "LOW"


class TestFreshnessViolation:
    def test_stale_data_detected(self, anomaly_detector):
        last_updated = datetime.utcnow() - timedelta(hours=50)
        report = anomaly_detector.detect_freshness_violation(
            last_updated=last_updated, max_age_hours=24.0
        )
        assert report is not None
        assert report.severity in ("HIGH", "CRITICAL")

    def test_fresh_data_passes(self, anomaly_detector):
        last_updated = datetime.utcnow() - timedelta(hours=2)
        report = anomaly_detector.detect_freshness_violation(
            last_updated=last_updated, max_age_hours=24.0
        )
        assert report.detected is False
        assert report.severity == "LOW"
