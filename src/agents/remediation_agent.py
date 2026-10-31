from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from src.agents.data_quality_agent import is_key_column
from src.agents.state import AgentState, PipelineIssue

logger = structlog.get_logger(__name__)


@dataclass
class RemediationResult:
    action_taken: str
    success: bool
    sql_executed: str
    rows_affected: int
    error_message: str = ""


class RemediationAgent:
    """LangGraph agent node that applies auto-remediations for safe issues."""

    # Severity levels that can be auto-remediated
    AUTO_REMEDIABLE_SEVERITIES = {"LOW", "MEDIUM"}

    def __init__(self, llm, db_client: Any) -> None:
        self.llm = llm
        self.db_client = db_client

    def can_auto_remediate(self, issue: PipelineIssue) -> bool:
        """Return True only for LOW/MEDIUM quality issues and cost optimizations.

        CRITICAL issues always require human intervention.
        """
        severity = issue.get("severity", "CRITICAL")
        issue_type = issue.get("issue_type", "")

        if severity == "CRITICAL":
            return False
        if severity not in self.AUTO_REMEDIABLE_SEVERITIES:
            return False
        if issue_type in ("quality", "cost"):
            return True
        return False

    async def remediate_data_quality(
        self, issue: PipelineIssue, warehouse_id: str
    ) -> RemediationResult:
        """Generate and execute SQL fix for data quality issues."""
        table_name = issue.get("metadata", {}).get("table_name", "")
        if not table_name:
            return RemediationResult(
                action_taken="no_action",
                success=False,
                sql_executed="",
                rows_affected=0,
                error_message="No table_name found in issue metadata",
            )

        failed_checks = issue.get("metadata", {}).get("failed_checks", [])
        sql_statements = []

        for check in failed_checks:
            check_type = check.get("check_type", "")
            column = check.get("column", "")

            if check_type == "nulls" and column and is_key_column(column):
                # Rows without a key cannot be joined or deduplicated, so they are removed.
                # Nulls in other columns are left for a human: deleting them loses data.
                sql = f"DELETE FROM {table_name} WHERE {column} IS NULL"
                sql_statements.append(("null_cleanup", column, sql))
            elif check_type == "duplicates":
                # Keep one row per key. EXCEPT (_rn) stops the helper column from being
                # written back into the table. Which duplicate survives is arbitrary.
                cols_raw = check.get("details", {}).get("key_columns", [column])
                if cols_raw:
                    key_cols = ", ".join(str(c) for c in cols_raw)
                    sql = (
                        f"CREATE OR REPLACE TABLE {table_name} AS "
                        "SELECT * EXCEPT (_rn) FROM ("
                        "SELECT *, ROW_NUMBER() OVER "
                        f"(PARTITION BY {key_cols} ORDER BY {key_cols}) AS _rn "
                        f"FROM {table_name}) WHERE _rn = 1"
                    )
                    sql_statements.append(("deduplication", column, sql))

        if not sql_statements:
            return RemediationResult(
                action_taken="no_sql_generated",
                success=True,
                sql_executed="",
                rows_affected=0,
                error_message="No auto-remediable SQL patterns found for this issue",
            )

        total_rows_affected = 0
        executed_sqls = []
        errors = []

        for action_name, col, sql in sql_statements:
            try:
                logger.info("Executing remediation SQL", action=action_name, column=col)
                result_rows = await self.db_client.execute_sql(warehouse_id, sql)
                executed_sqls.append(sql)
                # Count affected rows if result provides it
                if result_rows and "num_affected_rows" in result_rows[0]:
                    total_rows_affected += int(result_rows[0]["num_affected_rows"])
            except Exception as exc:
                logger.error("Remediation SQL failed", sql=sql, error=str(exc))
                errors.append(f"{action_name}: {exc}")

        success = len(errors) == 0
        return RemediationResult(
            action_taken=", ".join(s[0] for s in sql_statements),
            success=success,
            sql_executed="; ".join(executed_sqls),
            rows_affected=total_rows_affected,
            error_message="; ".join(errors),
        )

    async def remediate_cost(self, recommendation: dict, warehouse_id: str) -> RemediationResult:
        """Execute OPTIMIZE or VACUUM SQL for cost recommendations."""
        sql = recommendation.get("sql_command_or_api_call", "")
        action = recommendation.get("action", "")

        if not sql or not sql.strip().upper().startswith(("OPTIMIZE", "VACUUM")):
            return RemediationResult(
                action_taken=action,
                success=False,
                sql_executed=sql,
                rows_affected=0,
                error_message="Only OPTIMIZE and VACUUM SQL commands are auto-executed",
            )

        try:
            logger.info("Executing cost remediation SQL", action=action, sql=sql)
            result_rows = await self.db_client.execute_sql(warehouse_id, sql)
            return RemediationResult(
                action_taken=action,
                success=True,
                sql_executed=sql,
                rows_affected=len(result_rows),
                error_message="",
            )
        except Exception as exc:
            logger.error("Cost remediation failed", sql=sql, error=str(exc))
            return RemediationResult(
                action_taken=action,
                success=False,
                sql_executed=sql,
                rows_affected=0,
                error_message=str(exc),
            )

    async def __call__(self, state: AgentState) -> AgentState:
        """LangGraph node — applies safe auto-remediations and updates state."""
        logger.info("RemediationAgent invoked", iteration=state.get("iteration_count", 0))

        remediation_actions = list(state.get("remediation_actions", []))
        pipeline_issues = state.get("pipeline_issues", [])
        cost_recommendations = state.get("cost_recommendations", [])

        warehouse_id = state.get("run_context", {}).get("warehouse_id", "")

        if not warehouse_id:
            logger.warning("No warehouse_id found; skipping remediation")
            new_state = dict(state)
            new_state["remediation_actions"] = remediation_actions
            return new_state  # type: ignore[return-value]

        # Remediate quality issues
        for issue in pipeline_issues:
            if self.can_auto_remediate(issue):
                result = await self.remediate_data_quality(issue, warehouse_id)
                remediation_actions.append(
                    {
                        "type": "quality",
                        "pipeline": issue.get("pipeline_name", ""),
                        "action_taken": result.action_taken,
                        "success": result.success,
                        "sql_executed": result.sql_executed,
                        "rows_affected": result.rows_affected,
                        "error_message": result.error_message,
                    }
                )

        # Run storage maintenance (OPTIMIZE/VACUUM) for LOW/MEDIUM priority recommendations
        for rec in cost_recommendations:
            if rec.get("category") == "storage" and rec.get("priority") in ("LOW", "MEDIUM"):
                result = await self.remediate_cost(rec, warehouse_id)
                remediation_actions.append(
                    {
                        "type": "cost",
                        "action": rec.get("action", ""),
                        "table": rec.get("table_name", ""),
                        "action_taken": result.action_taken,
                        "success": result.success,
                        "sql_executed": result.sql_executed,
                        "rows_affected": result.rows_affected,
                        "error_message": result.error_message,
                    }
                )

        new_state = dict(state)
        new_state["remediation_actions"] = remediation_actions
        new_state["iteration_count"] = state.get("iteration_count", 0) + 1
        return new_state  # type: ignore[return-value]
