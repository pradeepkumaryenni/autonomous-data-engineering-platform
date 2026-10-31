from __future__ import annotations

import json
from typing import Any

import structlog

from src.agents.state import AgentState
from src.databricks.delta_utils import DeltaUtils

logger = structlog.get_logger(__name__)

# Cost model assumptions. These are heuristics for ranking recommendations,
# not measured savings.
MONTHLY_HOURS = 730.0  # cluster running 24x7
DBU_RATE_USD = 0.55  # flat list-price style rate per DBU-hour
AUTOSCALE_SAVINGS_PCT = 0.35  # assumed reduction in worker-hours with autoscaling
IDLE_SAVINGS_PCT = 0.20  # assumed idle share of an always-on cluster
STORAGE_USD_PER_GB_MONTH = 0.023  # ADLS/Blob hot tier, approximate
VACUUM_RECLAIM_PCT = 0.10  # assumed share of table size held by unreferenced files


class CostOptimizerAgent:
    """LangGraph agent node that analyzes and recommends cost optimizations."""

    def __init__(
        self,
        llm,
        db_client: Any,
        delta_utils: DeltaUtils,
    ) -> None:
        self.llm = llm
        self.db_client = db_client
        self.delta_utils = delta_utils

    async def analyze_cluster_costs(self, cluster_id: str) -> dict:
        """Fetch cluster config and identify cost inefficiencies."""
        logger.info("Analyzing cluster costs", cluster_id=cluster_id)
        try:
            metrics = await self.db_client.get_cluster_metrics(cluster_id)
        except Exception as exc:
            logger.error("Failed to get cluster metrics", error=str(exc))
            return {
                "cluster_id": cluster_id,
                "error": str(exc),
                "monthly_cost_usd": 0.0,
                "recommendations": [],
            }

        monthly_cost = metrics.dbus_per_hour * MONTHLY_HOURS * DBU_RATE_USD
        recommendations = []

        if not metrics.autoscale_enabled:
            potential_savings = monthly_cost * AUTOSCALE_SAVINGS_PCT
            recommendations.append(
                {
                    "issue": (
                        f"Autoscale not enabled ({metrics.num_workers} fixed workers); "
                        f"saving assumes {AUTOSCALE_SAVINGS_PCT:.0%} fewer worker-hours"
                    ),
                    "recommendation": "Enable autoscaling to reduce idle compute costs",
                    "estimated_monthly_savings_usd": round(potential_savings, 2),
                    "priority": "HIGH",
                    "action": "api",
                    "api_call": {
                        "endpoint": "/api/2.0/clusters/edit",
                        "payload": {
                            "cluster_id": cluster_id,
                            "autoscale": {
                                "min_workers": max(1, metrics.num_workers // 2),
                                "max_workers": metrics.num_workers * 2,
                            },
                        },
                    },
                }
            )

        # Always-on risk: auto-termination disabled (0 means "never terminate")
        if metrics.autotermination_minutes == 0:
            idle_savings = monthly_cost * IDLE_SAVINGS_PCT
            recommendations.append(
                {
                    "issue": (
                        "Auto-termination is disabled; saving assumes "
                        f"{IDLE_SAVINGS_PCT:.0%} of cluster hours are idle"
                    ),
                    "recommendation": "Set auto-termination to 30 minutes to reduce idle costs",
                    "estimated_monthly_savings_usd": round(idle_savings, 2),
                    "priority": "MEDIUM",
                    "action": "api",
                    "api_call": {
                        "endpoint": "/api/2.0/clusters/edit",
                        "payload": {
                            "cluster_id": cluster_id,
                            "autotermination_minutes": 30,
                        },
                    },
                }
            )

        return {
            "cluster_id": cluster_id,
            "state": metrics.state,
            "num_workers": metrics.num_workers,
            "dbus_per_hour": metrics.dbus_per_hour,
            "monthly_cost_usd": round(monthly_cost, 2),
            "autoscale_enabled": metrics.autoscale_enabled,
            "autotermination_minutes": metrics.autotermination_minutes,
            "recommendations": recommendations,
        }

    async def analyze_storage_costs(self, tables: list[str], warehouse_id: str) -> list[dict]:
        """Analyze Delta tables for storage optimization opportunities."""
        results = []
        for table_name in tables:
            logger.info("Analyzing storage for table", table=table_name)
            try:
                detail_rows = await self.db_client.execute_sql(
                    warehouse_id,
                    f"DESCRIBE DETAIL {table_name}",
                )
                stats = self.delta_utils.parse_describe_detail(detail_rows)

                needs_optimize = self.delta_utils.estimate_compaction_needed(
                    stats.num_files, stats.avg_file_size_mb
                )
                skew_score = 0.0
                if stats.partitioned_by:
                    try:
                        partition_rows = await self.db_client.execute_sql(
                            warehouse_id,
                            f"SELECT COUNT(*) AS cnt FROM {table_name} "
                            f"GROUP BY {stats.partitioned_by[0]} LIMIT 100",
                        )
                        partition_counts = {
                            str(i): int(r.get("cnt", 0)) for i, r in enumerate(partition_rows)
                        }
                        skew_score = self.delta_utils.compute_partition_skew(partition_counts)
                    except Exception:
                        skew_score = 0.0

                recommendations = []
                size_gb = stats.size_bytes / (1024**3)

                if needs_optimize:
                    # OPTIMIZE rewrites small files into larger ones. It speeds up reads but
                    # does not reduce storage by itself (old files stay until VACUUM).
                    recommendations.append(
                        {
                            "action": "OPTIMIZE",
                            "reason": (
                                f"{stats.num_files} files, avg {stats.avg_file_size_mb:.1f} MB "
                                "(target ~128 MB): small files slow down scans"
                            ),
                            "sql_command": self.delta_utils.generate_optimize_command(table_name),
                            "estimated_monthly_savings_usd": 0.0,
                            "priority": "HIGH" if stats.num_files > 1000 else "MEDIUM",
                        }
                    )

                vacuum_savings = size_gb * STORAGE_USD_PER_GB_MONTH * VACUUM_RECLAIM_PCT
                recommendations.append(
                    {
                        "action": "VACUUM",
                        "reason": (
                            "Remove data files no longer referenced by the Delta log "
                            f"(7-day retention). Saving assumes {VACUUM_RECLAIM_PCT:.0%} of "
                            "table size is unreferenced; VACUUM ... DRY RUN gives the real figure"
                        ),
                        "sql_command": self.delta_utils.generate_vacuum_command(table_name),
                        "estimated_monthly_savings_usd": round(vacuum_savings, 4),
                        "priority": "LOW",
                    }
                )
                estimated_savings_usd = vacuum_savings

                results.append(
                    {
                        "table_name": table_name,
                        "num_files": stats.num_files,
                        "size_mb": round(stats.size_mb, 2),
                        "avg_file_size_mb": round(stats.avg_file_size_mb, 2),
                        "partition_skew": round(skew_score, 4),
                        "needs_optimize": needs_optimize,
                        "recommendations": recommendations,
                        "estimated_savings_usd": round(estimated_savings_usd, 4),
                    }
                )
            except Exception as exc:
                logger.error("Failed to analyze table storage", table=table_name, error=str(exc))
                results.append(
                    {
                        "table_name": table_name,
                        "error": str(exc),
                        "recommendations": [],
                        "estimated_savings_usd": 0.0,
                    }
                )
        return results

    def generate_recommendations(
        self, cluster_analysis: dict, storage_analysis: list[dict]
    ) -> list[dict]:
        """Combine cluster and storage recommendations into a unified list."""
        recommendations = []

        # Cluster recommendations
        for rec in cluster_analysis.get("recommendations", []):
            recommendations.append(
                {
                    "category": "compute",
                    "cluster_id": cluster_analysis.get("cluster_id", ""),
                    "action": rec.get("recommendation", ""),
                    "issue": rec.get("issue", ""),
                    "estimated_monthly_savings_usd": rec.get("estimated_monthly_savings_usd", 0.0),
                    "priority": rec.get("priority", "MEDIUM"),
                    "sql_command_or_api_call": json.dumps(rec.get("api_call", {})),
                }
            )

        # Storage recommendations
        for table_result in storage_analysis:
            for rec in table_result.get("recommendations", []):
                recommendations.append(
                    {
                        "category": "storage",
                        "action": rec.get("action", ""),
                        "issue": rec.get("reason", ""),
                        "table_name": table_result.get("table_name", ""),
                        "estimated_monthly_savings_usd": rec.get(
                            "estimated_monthly_savings_usd", 0.0
                        ),
                        "priority": rec.get("priority", "LOW"),
                        "sql_command_or_api_call": rec.get("sql_command", ""),
                    }
                )

        # Sort by priority
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        recommendations.sort(key=lambda r: priority_order.get(r.get("priority", "LOW"), 3))
        return recommendations

    async def __call__(self, state: AgentState) -> AgentState:
        """LangGraph node — runs cost analysis and updates state."""
        logger.info("CostOptimizerAgent invoked", iteration=state.get("iteration_count", 0))

        ctx = state.get("run_context", {})
        cluster_id = ctx.get("cluster_id", "")
        warehouse_id = ctx.get("warehouse_id", "")
        tables: list[str] = list(dict.fromkeys(ctx.get("tables", [])))

        cluster_analysis: dict = {
            "cluster_id": cluster_id,
            "recommendations": [],
            "monthly_cost_usd": 0.0,
        }
        storage_analysis: list[dict] = []

        if cluster_id:
            cluster_analysis = await self.analyze_cluster_costs(cluster_id)

        if tables and warehouse_id:
            storage_analysis = await self.analyze_storage_costs(tables, warehouse_id)

        recommendations = self.generate_recommendations(cluster_analysis, storage_analysis)

        new_state = dict(state)
        new_state["cost_recommendations"] = recommendations
        new_state["iteration_count"] = state.get("iteration_count", 0) + 1
        return new_state  # type: ignore[return-value]
