from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd
import structlog

from src.agents.state import AgentState, PipelineIssue
from src.monitoring.quality_rules import DataQualityEngine, QualityResult, QualityRule

logger = structlog.get_logger(__name__)


def _short_name(table_name: str) -> str:
    return table_name.split(".")[-1].strip("`").lower()


def is_key_column(column: str) -> bool:
    name = column.lower()
    return name == "id" or name.endswith("_id")


class DataQualityAgent:
    """LangGraph agent node that performs data quality analysis on Databricks tables.

    Rules come from two places:
    - generic rules derived from the data (null rate per column, duplicates on the key)
    - declared rules ("contracts") per table, e.g. range, allowed values and a drift
      baseline. A declared rule replaces the generic rule for the same column/check.
    """

    def __init__(
        self,
        llm,
        db_client: Any,
        quality_engine: DataQualityEngine,
        contracts: dict[str, list[dict]] | None = None,
    ) -> None:
        self.llm = llm
        self.db_client = db_client
        self.quality_engine = quality_engine
        self.contracts = {k.lower(): v for k, v in (contracts or {}).items()}

    def build_rules(self, table_name: str, df: pd.DataFrame) -> list[QualityRule]:
        rules: dict[tuple[str, str], QualityRule] = {}
        for col in df.columns:
            rules[(col, "nulls")] = QualityRule(
                column=col, check_type="nulls", params={"threshold": 0.05}
            )

        key_cols = [c for c in df.columns if is_key_column(c)]
        if key_cols:
            rules[(key_cols[0], "duplicates")] = QualityRule(
                column=key_cols[0], check_type="duplicates", params={"key_columns": key_cols[:1]}
            )

        for declared in self.contracts.get(_short_name(table_name), []):
            rule = QualityRule(
                column=declared["column"],
                check_type=declared["check_type"],
                params=declared.get("params", {}),
            )
            rules[(rule.column, rule.check_type)] = rule

        return list(rules.values())

    async def analyze_table(self, table_name: str, warehouse_id: str) -> list[QualityResult]:
        """Fetch sample data from a table and run quality checks.

        Raises if the table cannot be read; returns [] for an empty table.
        """
        logger.info("Analyzing table quality", table=table_name)

        sql = f"SELECT * FROM {table_name} LIMIT 10000"
        rows = await self.db_client.execute_sql(warehouse_id, sql)

        if not rows:
            logger.warning("No rows returned from table", table=table_name)
            return []

        df = pd.DataFrame(rows)
        logger.info("Fetched rows for analysis", table=table_name, rows=len(df))

        results = self.quality_engine.run_all_checks(df, self.build_rules(table_name, df))
        logger.info(
            "Quality checks complete",
            table=table_name,
            total=len(results),
            failed=sum(1 for r in results if not r.passed),
        )
        return results

    def assess_severity(self, results: list[QualityResult]) -> str:
        """Determine overall severity based on quality results."""
        failed = [r for r in results if not r.passed]
        if not failed:
            return "LOW"

        failure_rate = len(failed) / len(results)
        has_drift = any(r.check_type == "drift" for r in failed)
        has_schema = any(r.check_type == "schema" for r in failed)

        if has_schema or failure_rate > 0.5:
            return "CRITICAL"
        elif has_drift or failure_rate > 0.3:
            return "HIGH"
        elif failure_rate > 0.15:
            return "MEDIUM"
        else:
            return "LOW"

    @staticmethod
    def _table_issue(
        table_name: str, warehouse_id: str, issue_type: str, severity: str, reason: str
    ) -> PipelineIssue:
        return {
            "pipeline_name": table_name,
            "issue_type": issue_type,
            "severity": severity,
            "description": f"Table '{table_name}' was not checked: {reason}",
            "metadata": {"table_name": table_name, "warehouse_id": warehouse_id, "reason": reason},
        }

    async def __call__(self, state: AgentState) -> AgentState:
        """LangGraph node function — runs quality analysis and updates state."""
        logger.info("DataQualityAgent invoked", iteration=state.get("iteration_count", 0))

        ctx = state.get("run_context", {})
        warehouse_id = ctx.get("warehouse_id", "")
        pipeline_issues = list(state.get("pipeline_issues", []))
        quality_results = list(state.get("quality_results", []))

        for table_name in ctx.get("tables", []):
            try:
                results = await self.analyze_table(table_name, warehouse_id)
            except Exception as exc:
                # An unreadable table must not look like a clean one
                logger.error("Failed to fetch table data", table=table_name, error=str(exc))
                pipeline_issues.append(
                    self._table_issue(table_name, warehouse_id, "access", "HIGH", str(exc))
                )
                continue
            if not results:
                pipeline_issues.append(
                    self._table_issue(
                        table_name, warehouse_id, "volume", "MEDIUM", "query returned no rows"
                    )
                )
                continue
            severity = self.assess_severity(results)

            for r in results:
                quality_results.append({"table_name": table_name, **asdict(r)})

            failed = [r for r in results if not r.passed]
            if failed:
                issue: PipelineIssue = {
                    "pipeline_name": table_name,
                    "issue_type": "quality",
                    "severity": severity,
                    "description": (
                        f"Table '{table_name}' failed {len(failed)}/{len(results)} "
                        "quality checks: " + "; ".join(r.message for r in failed[:3])
                    ),
                    "metadata": {
                        "table_name": table_name,
                        "warehouse_id": warehouse_id,
                        "failed_checks": [asdict(r) for r in failed],
                        "total_checks": len(results),
                    },
                }
                pipeline_issues.append(issue)

        new_state = dict(state)
        new_state["pipeline_issues"] = pipeline_issues
        new_state["quality_results"] = quality_results
        new_state["iteration_count"] = state.get("iteration_count", 0) + 1
        return new_state  # type: ignore[return-value]
