"""Offline stand-in for DatabricksClient, used when DEMO_MODE=true.

It serves the bundled sample data as a table and answers only the handful of SQL
statements the agents issue. Cluster, table-detail and job-history responses come
from data/samples/demo_fixtures.json. Remediation SQL (DELETE / dedup) is applied
to an in-memory copy of the table so reported row counts are real for the sample;
nothing leaves the process and no file on disk is modified.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.databricks.client import ClusterMetrics, cluster_metrics_from_state

logger = structlog.get_logger(__name__)

SAMPLES_DIR = Path(__file__).resolve().parents[2] / "data" / "samples"
DEMO_TABLE = "demo.bronze.transactions"

_SELECT = re.compile(r"^SELECT \* FROM (\S+) LIMIT (\d+)$", re.IGNORECASE)
_DETAIL = re.compile(r"^DESCRIBE DETAIL (\S+)$", re.IGNORECASE)
_GROUP_COUNT = re.compile(
    r"^SELECT COUNT\(\*\) AS cnt FROM (\S+) GROUP BY (\w+)(?: LIMIT \d+)?$", re.IGNORECASE
)
_DELETE_NULLS = re.compile(r"^DELETE FROM (\S+) WHERE (\w+) IS NULL$", re.IGNORECASE)
_DEDUP = re.compile(
    r"^CREATE OR REPLACE TABLE (\S+) AS .*PARTITION BY ([\w, ]+?) ORDER BY", re.IGNORECASE
)
_MAINTENANCE = re.compile(r"^(OPTIMIZE|VACUUM)\b", re.IGNORECASE)


def _short_name(table_name: str) -> str:
    return table_name.split(".")[-1].strip("`").lower()


def load_sample_transactions(samples_dir: Path = SAMPLES_DIR) -> pd.DataFrame:
    """Read transactions.csv, generating it (deterministically) first if it is missing."""
    csv_path = samples_dir / "transactions.csv"
    if not csv_path.exists():
        logger.info("Sample data not found, generating it", path=str(csv_path))
        script = samples_dir / "generate_sample_data.py"
        spec = importlib.util.spec_from_file_location("generate_sample_data", script)
        if spec is None or spec.loader is None:
            raise FileNotFoundError(f"Cannot load sample data generator at {script}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.build(samples_dir)
    return pd.read_csv(csv_path)


def load_quality_contracts(samples_dir: Path = SAMPLES_DIR) -> dict[str, list[dict]]:
    """Declared quality rules from schema.json, keyed by short table name."""
    schema = json.loads((samples_dir / "schema.json").read_text(encoding="utf-8"))
    return {schema["table_name"].lower(): schema.get("quality_rules", [])}


class DemoDatabricksClient:
    """Implements the subset of DatabricksClient the agents and routers call."""

    def __init__(self, tables: dict[str, pd.DataFrame], fixtures: dict[str, Any]) -> None:
        # Copy so remediation in one run never leaks into the caller's dataframe
        self._tables = {_short_name(name): df.copy() for name, df in tables.items()}
        self._fixtures = fixtures

    @classmethod
    def from_samples(cls, samples_dir: Path = SAMPLES_DIR) -> DemoDatabricksClient:
        fixtures = json.loads((samples_dir / "demo_fixtures.json").read_text(encoding="utf-8"))
        return cls({DEMO_TABLE: load_sample_transactions(samples_dir)}, fixtures)

    def _table(self, table_name: str) -> pd.DataFrame:
        key = _short_name(table_name)
        if key not in self._tables:
            raise LookupError(
                f"Demo mode only serves {sorted(self._tables)}; got table '{table_name}'"
            )
        return self._tables[key]

    async def close(self) -> None:
        return None

    async def get_cluster_state(self, cluster_id: str) -> dict:
        return {**self._fixtures["cluster"], "cluster_id": cluster_id}

    async def get_cluster_metrics(self, cluster_id: str) -> ClusterMetrics:
        return cluster_metrics_from_state(cluster_id, await self.get_cluster_state(cluster_id))

    async def list_jobs(self) -> list[dict]:
        return list(self._fixtures.get("jobs", []))

    async def list_job_runs(self, job_id: int, limit: int = 20) -> list[dict]:
        runs = [r for r in self._fixtures.get("job_runs", []) if r.get("job_id") == job_id]
        return runs[:limit]

    async def execute_sql(self, warehouse_id: str, sql: str) -> list[dict]:
        statement = " ".join(sql.split()).rstrip(";")

        if m := _SELECT.match(statement):
            df = self._table(m.group(1)).head(int(m.group(2)))
            return df.astype(object).where(df.notna(), None).to_dict("records")

        if m := _DETAIL.match(statement):
            self._table(m.group(1))  # raise for unknown tables, like Databricks would
            detail = self._fixtures["table_details"][_short_name(m.group(1))]
            return [{"name": m.group(1), **detail}]

        if m := _GROUP_COUNT.match(statement):
            counts = self._table(m.group(1))[m.group(2)].value_counts(dropna=False)
            return [{"cnt": int(c)} for c in counts.to_numpy()]

        if m := _DELETE_NULLS.match(statement):
            key = _short_name(m.group(1))
            df = self._table(m.group(1))
            mask = df[m.group(2)].isna()
            self._tables[key] = df[~mask].reset_index(drop=True)
            return [{"num_affected_rows": int(mask.sum())}]

        if m := _DEDUP.match(statement):
            key = _short_name(m.group(1))
            df = self._table(m.group(1))
            key_cols = [c.strip() for c in m.group(2).split(",")]
            deduped = df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
            self._tables[key] = deduped
            return [{"num_affected_rows": len(df) - len(deduped)}]

        if _MAINTENANCE.match(statement):
            # OPTIMIZE / VACUUM have no meaning for an in-memory table
            return []

        raise NotImplementedError(f"Demo client does not support this statement: {statement}")
