"""End-to-end runs of the full LangGraph network in offline demo mode.

No network, no LLM: DemoDatabricksClient serves data/samples, the agents use their
rule-based paths. These tests exercise every node and both routing branches.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.agents.cost_optimizer_agent import CostOptimizerAgent
from src.agents.data_quality_agent import DataQualityAgent
from src.agents.orchestrator import build_graph, run_pipeline_analysis
from src.agents.remediation_agent import RemediationAgent
from src.agents.reporter_agent import ReporterAgent
from src.agents.root_cause_agent import RootCauseAgent
from src.config.settings import Settings
from src.databricks.delta_utils import DeltaUtils
from src.databricks.demo_client import (
    DEMO_TABLE,
    SAMPLES_DIR,
    DemoDatabricksClient,
    load_quality_contracts,
)
from src.monitoring.anomaly_detector import AnomalyDetector
from src.monitoring.quality_rules import DataQualityEngine

DEMO_REQUEST = {
    "pipeline_name": "bronze_transactions",
    "table_names": [DEMO_TABLE],
    "warehouse_id": "demo-warehouse",
    "cluster_id": "0101-demo-etl01",
    "job_id": 1001,
}


def make_graph(db_client, contracts=None):
    return build_graph(
        quality_agent=DataQualityAgent(
            llm=None, db_client=db_client, quality_engine=DataQualityEngine(), contracts=contracts
        ),
        rca_agent=RootCauseAgent(llm=None, db_client=db_client, anomaly_detector=AnomalyDetector()),
        cost_agent=CostOptimizerAgent(llm=None, db_client=db_client, delta_utils=DeltaUtils()),
        remediation_agent=RemediationAgent(llm=None, db_client=db_client),
        reporter_agent=ReporterAgent(llm=None),
    )


async def run(graph, **overrides):
    req = {**DEMO_REQUEST, **overrides}
    return await run_pipeline_analysis(
        pipeline_name=req["pipeline_name"],
        table_names=req["table_names"],
        warehouse_id=req["warehouse_id"],
        graph=graph,
        cluster_id=req["cluster_id"],
        job_id=req["job_id"],
        demo_mode=True,
    )


async def test_demo_run_escalates_drift_and_skips_remediation():
    client = DemoDatabricksClient.from_samples()
    result = await run(make_graph(client, load_quality_contracts()))

    failed = {r["check_type"] for r in result["quality_results"] if not r["passed"]}
    assert failed == {"duplicates", "range", "drift"}

    (issue,) = result["pipeline_issues"]
    assert issue["severity"] == "HIGH"  # drift makes it HIGH

    # HIGH is never auto-remediated: the run is escalated and no SQL is executed
    assert result["should_escalate"] is True
    assert result["remediation_actions"] == []
    rows = await client.execute_sql("demo", f"SELECT * FROM {DEMO_TABLE} LIMIT 100000")
    assert len(rows) == 10_000

    causes = {rc["check_type"]: rc for rc in result["root_causes"]}
    assert set(causes) == {"drift", "duplicates"}
    assert all(rc["method"] == "rule-based" for rc in causes.values())
    # job-history fixture has a failed run followed by a retry -> idempotency hypothesis
    assert "non-idempotent" in causes["duplicates"]["root_cause"]
    assert any("RETRY" in e or "retried" in e for e in causes["duplicates"]["evidence"])

    actions = [r["action"] for r in result["cost_recommendations"]]
    assert "OPTIMIZE" in actions and "VACUUM" in actions
    assert any("autoscaling" in a for a in actions)

    report = result["final_report"]
    assert report.startswith("# Pipeline Analysis Report: bronze_transactions")
    assert "Escalated for human review" in report
    assert "mode: offline demo" in report


async def test_demo_run_auto_remediates_low_severity_duplicates():
    df = pd.read_csv(SAMPLES_DIR / "transactions.csv")
    clean = df[pd.to_datetime(df["timestamp"]) < "2024-12-01"].dropna().drop_duplicates("id")
    clean = clean.head(500)
    with_dups = pd.concat([clean, clean.head(7)], ignore_index=True)
    fixtures = json.loads((SAMPLES_DIR / "demo_fixtures.json").read_text(encoding="utf-8"))
    client = DemoDatabricksClient({DEMO_TABLE: with_dups}, fixtures)

    result = await run(make_graph(client))  # no contract: generic rules only

    (issue,) = result["pipeline_issues"]
    assert issue["severity"] == "LOW"
    assert result["should_escalate"] is False

    dedup = [a for a in result["remediation_actions"] if a["type"] == "quality"]
    assert dedup[0]["action_taken"] == "deduplication"
    assert dedup[0]["success"] is True
    assert dedup[0]["rows_affected"] == 7
    assert "SELECT * EXCEPT (_rn)" in dedup[0]["sql_executed"]

    rows = await client.execute_sql("demo", f"SELECT * FROM {DEMO_TABLE} LIMIT 100000")
    assert len(rows) == 500

    # Only LOW/MEDIUM storage actions run automatically; HIGH-priority OPTIMIZE does not
    cost_actions = {a["action"] for a in result["remediation_actions"] if a["type"] == "cost"}
    assert cost_actions == {"VACUUM"}
    assert "Auto-remediation ran" in result["final_report"]


async def test_clean_table_skips_remediation():
    df = pd.read_csv(SAMPLES_DIR / "transactions.csv")
    clean = df[pd.to_datetime(df["timestamp"]) < "2024-12-01"].dropna().drop_duplicates("id")
    fixtures = json.loads((SAMPLES_DIR / "demo_fixtures.json").read_text(encoding="utf-8"))
    client = DemoDatabricksClient({DEMO_TABLE: clean.head(300)}, fixtures)

    result = await run(make_graph(client))

    assert result["pipeline_issues"] == []
    assert result["root_causes"] == []
    assert result["remediation_actions"] == []
    assert "No issues detected." in result["final_report"]


def test_demo_api_end_to_end():
    from src.api.main import create_app

    app = create_app(settings=Settings(demo_mode=True, log_level="WARNING"))
    with TestClient(app) as client:
        health = client.get("/api/v1/health/health").json()
        assert health["status"] == "ok" and health["demo_mode"] is True

        resp = client.post("/api/v1/pipelines/run", json=DEMO_REQUEST)
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]

        # TestClient runs background tasks before returning, so the run is finished
        status = client.get(f"/api/v1/pipelines/{run_id}/status").json()
        assert status["status"] == "ESCALATED", status.get("error")
        assert status["report"].startswith("# Pipeline Analysis Report")
        assert len(status["quality_results"]) == 11

        metrics = client.get("/api/v1/health/metrics").text
        assert (
            'adep_pipeline_runs_total{pipeline_name="bronze_transactions",status="ESCALATED"}'
            in metrics
        )


@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "t WHERE 1=1", "a.b.c.d", "`t`"])
def test_rejects_unsafe_table_names(bad):
    from src.api.main import create_app

    with TestClient(create_app(testing=True)) as client:
        resp = client.post("/api/v1/pipelines/run", json={**DEMO_REQUEST, "table_names": [bad]})
        assert resp.status_code == 422


async def test_unreadable_table_is_escalated_not_reported_clean():
    client = DemoDatabricksClient.from_samples()
    result = await run(make_graph(client), table_names=["catalog.bronze.does_not_exist"])

    (issue,) = result["pipeline_issues"]
    assert issue["issue_type"] == "access" and issue["severity"] == "HIGH"
    assert result["should_escalate"] is True
    assert "No issues detected." not in result["final_report"]
