from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


@dataclass
class ClusterMetrics:
    cluster_id: str
    state: str
    num_workers: int
    driver_node_type: str
    worker_node_type: str
    dbus_per_hour: float
    spark_version: str
    autoscale_enabled: bool
    autoscale_min_workers: int
    autoscale_max_workers: int
    autotermination_minutes: int = 0


# Rough DBU/hour per node for common Azure node types (simplified lookup, not a price list)
_DBU_PER_NODE = {
    "Standard_DS3_v2": 0.75,
    "Standard_DS4_v2": 1.5,
    "Standard_DS5_v2": 3.0,
    "Standard_D8s_v3": 2.0,
    "Standard_D16s_v3": 4.0,
}


def cluster_metrics_from_state(cluster_id: str, data: dict) -> ClusterMetrics:
    """Turn a /api/2.0/clusters/get response into ClusterMetrics."""
    autoscale = data.get("autoscale") or {}
    worker_node_type = data.get("node_type_id", "Standard_DS3_v2")
    driver_node_type = data.get("driver_node_type_id", worker_node_type)
    num_workers = data.get("num_workers", 0)

    dbu_per_worker = _DBU_PER_NODE.get(worker_node_type, 1.0)
    total_dbus = dbu_per_worker * max(num_workers, 1) + dbu_per_worker  # +driver

    return ClusterMetrics(
        cluster_id=cluster_id,
        state=data.get("state", "UNKNOWN"),
        num_workers=num_workers,
        driver_node_type=driver_node_type,
        worker_node_type=worker_node_type,
        dbus_per_hour=total_dbus,
        spark_version=data.get("spark_version", "unknown"),
        autoscale_enabled=bool(autoscale),
        autoscale_min_workers=autoscale.get("min_workers", 0),
        autoscale_max_workers=autoscale.get("max_workers", 0),
        autotermination_minutes=int(data.get("autotermination_minutes", 0) or 0),
    )


_INT_TYPES = {"BYTE", "SHORT", "INT", "LONG"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "DECIMAL"}


def _coerce(value: Any, type_name: str) -> Any:
    """The Statement Execution API returns every value as a string; restore numeric types."""
    if value is None:
        return None
    try:
        if type_name in _INT_TYPES:
            return int(value)
        if type_name in _FLOAT_TYPES:
            return float(value)
        if type_name == "BOOLEAN":
            return str(value).lower() == "true"
    except (TypeError, ValueError):
        return value
    return value


def _retry_decorator():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )


class DatabricksClient:
    """Async HTTP client for the Databricks REST API."""

    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self._client = httpx.AsyncClient(
            base_url=self.host,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=60.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    @_retry_decorator()
    async def get_cluster_state(self, cluster_id: str) -> dict:
        """GET /api/2.0/clusters/get — returns cluster info dict."""
        response = await self._client.get(
            "/api/2.0/clusters/get", params={"cluster_id": cluster_id}
        )
        response.raise_for_status()
        return response.json()

    @_retry_decorator()
    async def list_jobs(self) -> list[dict]:
        """GET /api/2.1/jobs/list — returns list of job definitions."""
        jobs = []
        offset = 0
        limit = 25
        while True:
            response = await self._client.get(
                "/api/2.1/jobs/list",
                params={"limit": limit, "offset": offset, "expand_tasks": False},
            )
            response.raise_for_status()
            data = response.json()
            batch = data.get("jobs", [])
            jobs.extend(batch)
            if not data.get("has_more", False):
                break
            offset += limit
        return jobs

    @_retry_decorator()
    async def get_job_run(self, run_id: int) -> dict:
        """GET /api/2.1/jobs/runs/get — returns run details."""
        response = await self._client.get("/api/2.1/jobs/runs/get", params={"run_id": run_id})
        response.raise_for_status()
        return response.json()

    @_retry_decorator()
    async def trigger_job(self, job_id: int, params: dict | None = None) -> int:
        """POST /api/2.1/jobs/run-now — triggers a job run, returns run_id."""
        payload: dict[str, Any] = {"job_id": job_id}
        if params:
            payload["notebook_params"] = params
        response = await self._client.post("/api/2.1/jobs/run-now", json=payload)
        response.raise_for_status()
        return response.json()["run_id"]

    @_retry_decorator()
    async def cancel_run(self, run_id: int) -> None:
        """POST /api/2.1/jobs/runs/cancel — cancels a running job."""
        response = await self._client.post("/api/2.1/jobs/runs/cancel", json={"run_id": run_id})
        response.raise_for_status()

    async def get_cluster_metrics(self, cluster_id: str) -> ClusterMetrics:
        """Fetch cluster configuration and extract key metrics."""
        data = await self.get_cluster_state(cluster_id)
        return cluster_metrics_from_state(cluster_id, data)

    @_retry_decorator()
    async def list_job_runs(self, job_id: int, limit: int = 20) -> list[dict]:
        """GET /api/2.1/jobs/runs/list — most recent runs of one job."""
        response = await self._client.get(
            "/api/2.1/jobs/runs/list",
            params={"job_id": job_id, "limit": limit, "expand_tasks": False},
        )
        response.raise_for_status()
        return response.json().get("runs", [])

    @_retry_decorator()
    async def list_tables(self, catalog: str, schema: str) -> list[str]:
        """GET /api/2.1/unity-catalog/tables — lists table names."""
        response = await self._client.get(
            "/api/2.1/unity-catalog/tables",
            params={"catalog_name": catalog, "schema_name": schema},
        )
        response.raise_for_status()
        data = response.json()
        return [t["name"] for t in data.get("tables", [])]

    @_retry_decorator()
    async def execute_sql(self, warehouse_id: str, sql: str) -> list[dict]:
        """Execute SQL via /api/2.0/sql/statements and return rows as list of dicts."""
        # Submit statement
        response = await self._client.post(
            "/api/2.0/sql/statements",
            json={
                "warehouse_id": warehouse_id,
                "statement": sql,
                "wait_timeout": "30s",
                "on_wait_timeout": "CONTINUE",
            },
        )
        response.raise_for_status()
        result = response.json()
        statement_id = result["statement_id"]

        # Poll until complete
        max_polls = 60
        for _ in range(max_polls):
            status = result.get("status", {}).get("state", "")
            if status == "SUCCEEDED":
                break
            elif status in ("FAILED", "CANCELED", "CLOSED"):
                error_msg = (
                    result.get("status", {}).get("error", {}).get("message", "Unknown error")
                )
                raise RuntimeError(f"SQL statement failed: {error_msg}")

            await asyncio.sleep(2)
            poll_resp = await self._client.get(f"/api/2.0/sql/statements/{statement_id}")
            poll_resp.raise_for_status()
            result = poll_resp.json()

        if result.get("status", {}).get("state", "") != "SUCCEEDED":
            raise TimeoutError(f"SQL statement {statement_id} did not finish after polling")

        # Extract column names and rows (only the first result chunk is read)
        manifest = result.get("manifest", {})
        columns = manifest.get("schema", {}).get("columns", [])
        rows_data = result.get("result", {}).get("data_array", [])

        if not columns:
            return []

        names = [col["name"] for col in columns]
        types = [str(col.get("type_name", "STRING")).upper() for col in columns]
        return [
            {name: _coerce(value, type_name) for name, type_name, value in zip(names, types, row)}
            for row in rows_data
        ]

    def get_dbu_cost(self, cluster_id: str, hours: float, dbu_rate: float = 0.55) -> float:
        """Calculate estimated DBU cost. Uses a fixed dbu_rate per DBU-hour."""
        # Standard DS3_v2 = 0.75 DBU/hr worker, simplified for estimation
        estimated_dbus_per_hour = 4.0  # typical small cluster
        return estimated_dbus_per_hour * hours * dbu_rate
