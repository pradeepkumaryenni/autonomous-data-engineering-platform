"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest

from src.monitoring.anomaly_detector import AnomalyDetector
from src.monitoring.quality_rules import DataQualityEngine


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """1000-row synthetic transaction DataFrame with 5% nulls and 3% duplicates."""
    rng = np.random.default_rng(42)
    n = 1000
    amounts = rng.exponential(scale=150, size=n).round(2)
    base_ts = datetime(2024, 1, 1)
    df = pd.DataFrame(
        {
            "id": range(1, n + 1),
            "amount": amounts,
            "customer_id": rng.integers(1000, 9999, size=n),
            "status": rng.choice(
                ["completed", "pending", "failed", "refunded"], size=n, p=[0.7, 0.15, 0.1, 0.05]
            ),
            "timestamp": [
                base_ts + timedelta(minutes=int(m)) for m in rng.integers(0, 525600, size=n)
            ],
            "product_id": [f"PROD-{i:04d}" for i in rng.integers(1, 201, size=n)],
        }
    )
    null_indices = rng.choice(n, size=int(n * 0.05), replace=False)
    df.loc[null_indices, "amount"] = None
    dup_indices = rng.choice(n, size=int(n * 0.03), replace=False)
    df = pd.concat([df, df.iloc[dup_indices].copy()], ignore_index=True)
    return df


@pytest.fixture
def clean_dataframe() -> pd.DataFrame:
    rng = np.random.default_rng(99)
    n = 500
    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "amount": rng.uniform(1.0, 1000.0, size=n).round(2),
            "customer_id": rng.integers(1000, 9999, size=n),
            "status": rng.choice(["completed", "pending", "failed"], size=n),
            "timestamp": [
                datetime(2024, 6, 1) + timedelta(minutes=int(m))
                for m in rng.integers(0, 43200, size=n)
            ],
            "product_id": [f"PROD-{i:04d}" for i in rng.integers(1, 201, size=n)],
        }
    )


@pytest.fixture
def quality_engine() -> DataQualityEngine:
    return DataQualityEngine()


@pytest.fixture
def anomaly_detector() -> AnomalyDetector:
    return AnomalyDetector()


@pytest.fixture
def mock_db_client():
    client = MagicMock()
    client.get_cluster_state = AsyncMock(
        return_value={
            "cluster_id": "0101-test-abc123",
            "state": "RUNNING",
            "num_workers": 4,
            "driver_node_type_id": "Standard_DS3_v2",
            "node_type_id": "Standard_DS3_v2",
            "spark_version": "14.3.x-scala2.12",
            "autoscale": {"min_workers": 2, "max_workers": 8},
        }
    )
    client.execute_sql = AsyncMock(
        return_value=[
            {
                "id": i,
                "amount": round(random.uniform(10, 500), 2),
                "customer_id": random.randint(1000, 9999),
                "status": random.choice(["completed", "pending"]),
                "product_id": f"PROD-{random.randint(1, 200):04d}",
            }
            for i in range(100)
        ]
    )
    client.get_dbu_cost = MagicMock(return_value=12.45)
    client.close = AsyncMock()
    return client
