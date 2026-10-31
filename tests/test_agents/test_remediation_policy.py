"""Safety rules of the remediation agent and the graph router."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from src.agents.orchestrator import should_escalate
from src.agents.remediation_agent import RemediationAgent


def issue(severity: str, failed_checks: list[dict] | None = None) -> dict:
    return {
        "pipeline_name": "cat.sch.tbl",
        "issue_type": "quality",
        "severity": severity,
        "description": "test",
        "metadata": {"table_name": "cat.sch.tbl", "failed_checks": failed_checks or []},
    }


def test_high_and_critical_are_never_auto_remediated():
    agent = RemediationAgent(llm=None, db_client=MagicMock())
    assert not agent.can_auto_remediate(issue("HIGH"))
    assert not agent.can_auto_remediate(issue("CRITICAL"))
    assert agent.can_auto_remediate(issue("MEDIUM"))


def test_router_escalates_high_even_when_low_issues_exist():
    state = {"pipeline_issues": [issue("LOW"), issue("HIGH")]}
    assert should_escalate(state) == "escalate"
    assert should_escalate({"pipeline_issues": [issue("MEDIUM")]}) == "remediate"
    assert should_escalate({"pipeline_issues": []}) == "skip"


async def test_null_rows_are_only_deleted_for_key_columns():
    db = MagicMock()
    db.execute_sql = AsyncMock(return_value=[{"num_affected_rows": 3}])
    agent = RemediationAgent(llm=None, db_client=db)

    non_key = issue("LOW", [{"check_type": "nulls", "column": "amount"}])
    result = await agent.remediate_data_quality(non_key, "wh")
    assert result.action_taken == "no_sql_generated"
    db.execute_sql.assert_not_called()

    key = issue("LOW", [{"check_type": "nulls", "column": "customer_id"}])
    result = await agent.remediate_data_quality(key, "wh")
    assert result.sql_executed == "DELETE FROM cat.sch.tbl WHERE customer_id IS NULL"
    assert result.rows_affected == 3
