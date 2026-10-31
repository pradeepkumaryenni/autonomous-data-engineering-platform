"""Integration tests for FastAPI endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.monitoring.anomaly_detector import AnomalyDetector
from src.monitoring.quality_rules import DataQualityEngine


@pytest.fixture
def client():
    """Create a test client with mocked dependencies."""
    from src.api.main import create_app

    mock_db_client = MagicMock()
    mock_db_client.get_cluster_state = AsyncMock(
        return_value={"state": "RUNNING", "cluster_id": "test"}
    )
    mock_db_client.close = AsyncMock()

    app = create_app(testing=True)
    app.state.db_client = mock_db_client
    app.state.quality_engine = DataQualityEngine()
    app.state.anomaly_detector = AnomalyDetector()
    app.state.pipeline_runs = {}

    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/api/v1/health/health")
        assert resp.status_code == 200

    def test_health_has_status_field(self, client):
        resp = client.get("/api/v1/health/health")
        data = resp.json()
        assert "status" in data

    def test_metrics_returns_200(self, client):
        resp = client.get("/api/v1/health/metrics")
        assert resp.status_code == 200


class TestQualityEndpoints:
    def test_rules_endpoint_returns_list(self, client):
        resp = client.get("/api/v1/quality/rules")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_rules_have_required_fields(self, client):
        resp = client.get("/api/v1/quality/rules")
        rules = resp.json()
        for rule in rules:
            assert "check_type" in rule
            assert "description" in rule


class TestPipelineEndpoints:
    def test_list_runs_empty_initially(self, client):
        resp = client.get("/api/v1/pipelines/")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_get_nonexistent_run_returns_404(self, client):
        resp = client.get("/api/v1/pipelines/nonexistent-run-id/status")
        assert resp.status_code == 404

    def test_pipeline_run_requires_body(self, client):
        resp = client.post("/api/v1/pipelines/run", json={})
        assert resp.status_code == 422  # Unprocessable Entity

    def test_pipeline_run_returns_run_id(self, client):
        payload = {
            "pipeline_name": "test_pipeline",
            "table_names": ["catalog.bronze.transactions"],
            "warehouse_id": "abc123",
            "cluster_id": "0101-test-abc",
        }
        if True:  # no patch needed
            resp = client.post("/api/v1/pipelines/run", json=payload)
        assert resp.status_code == 202
        data = resp.json()
        assert "run_id" in data
        assert data["pipeline_name"] == "test_pipeline"
        assert data["status"] == "PENDING"
