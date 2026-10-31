"""
Generate synthetic sample data for ADEP testing and the offline demo.

Outputs:
    data/samples/transactions.csv  — 10,000-row transaction dataset
    data/samples/schema.json       — Expected schema + declared quality rules

Injected problems (deterministic, seed=42):
    * nulls       — 3% of `amount`, 1% of `country`
    * duplicates  — 1% of rows are exact copies of other rows
    * drift       — from 2024-12-01 `amount` arrives 100x larger, simulating an
                    upstream switch from major to minor currency units
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
N_ROWS = 10_000
NULL_PCT = 0.03  # 3% nulls
DUPLICATE_PCT = 0.01  # 1% duplicates
DRIFT_START = pd.Timestamp("2024-12-01")
DRIFT_FACTOR = 100.0
MAX_AMOUNT = 50_000.0

COUNTRIES = [
    "US", "GB", "DE", "FR", "CA", "AU", "IN", "BR", "JP", "SG",
    "NL", "SE", "NO", "CH", "IT", "ES", "MX", "AR", "ZA", "KR",
]  # fmt: skip
STATUSES = ["completed", "pending", "failed", "refunded", "processing"]
STATUS_PROBS = [0.65, 0.15, 0.08, 0.07, 0.05]


def generate_transactions(n: int, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    """Generate transaction data. Returns (dataframe, baseline stats for `amount`)."""
    base_n = int(n * (1 - DUPLICATE_PCT))

    ids = [f"TXN{str(i).zfill(8)}" for i in range(1, base_n + 1)]

    # Amount: log-normal distribution (realistic e-commerce distribution)
    amounts = np.exp(rng.normal(loc=4.5, scale=1.2, size=base_n))
    amounts = np.clip(amounts, 0.01, MAX_AMOUNT).round(2)

    customer_ids = [f"CUST{str(i).zfill(6)}" for i in rng.integers(1, 5000, size=base_n)]
    statuses = rng.choice(STATUSES, size=base_n, p=STATUS_PROBS)
    countries = rng.choice(COUNTRIES, size=base_n)
    product_ids = [f"PROD{str(i).zfill(5)}" for i in rng.integers(1, 2000, size=base_n)]

    # Timestamps: spread over 2024
    base_ts = pd.Timestamp("2024-01-01 00:00:00")
    offsets_seconds = rng.integers(0, 366 * 24 * 3600, size=base_n)
    timestamps = [base_ts + pd.Timedelta(seconds=int(s)) for s in offsets_seconds]

    df = pd.DataFrame(
        {
            "id": ids,
            "amount": amounts,
            "customer_id": customer_ids,
            "status": statuses,
            "timestamp": timestamps,
            "product_id": product_ids,
            "country": countries,
        }
    )

    # Baseline = the healthy period before the drift starts
    healthy = df.loc[df["timestamp"] < DRIFT_START, "amount"]
    baseline = {
        "mean": round(float(healthy.mean()), 2),
        "std": round(float(healthy.std()), 2),
        "rows": int(len(healthy)),
    }

    # Inject drift: December amounts arrive in minor units (x100)
    drift_mask = df["timestamp"] >= DRIFT_START
    df.loc[drift_mask, "amount"] = (df.loc[drift_mask, "amount"] * DRIFT_FACTOR).round(2)

    # Inject nulls in amount (3% null rate)
    null_indices = rng.choice(base_n, size=int(base_n * NULL_PCT), replace=False)
    df.loc[null_indices, "amount"] = np.nan

    # Inject nulls in country (1% null rate)
    null_country_indices = rng.choice(base_n, size=int(base_n * 0.01), replace=False)
    df.loc[null_country_indices, "country"] = np.nan

    # Add 1% duplicate rows
    dup_n = n - base_n
    if dup_n > 0:
        dup_indices = rng.choice(base_n, size=dup_n, replace=True)
        duplicates = df.iloc[dup_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)

    # Shuffle
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    return df, baseline


def generate_schema(baseline: dict) -> dict:
    """Return the expected schema and declared quality rules for the transactions table."""
    return {
        "table_name": "transactions",
        "description": "E-commerce transaction records for 2024 (synthetic)",
        "version": "1.1.0",
        "columns": [
            {
                "name": "id",
                "dtype": "string",
                "nullable": False,
                "description": "Unique transaction identifier (TXN########)",
                "constraints": {"pattern": "^TXN[0-9]{8}$"},
            },
            {
                "name": "amount",
                "dtype": "double",
                "nullable": True,
                "description": "Transaction amount in major currency units",
                "constraints": {"min": 0.01, "max": MAX_AMOUNT},
            },
            {
                "name": "customer_id",
                "dtype": "string",
                "nullable": False,
                "description": "Customer identifier (CUST######)",
                "constraints": {"pattern": "^CUST[0-9]{6}$"},
            },
            {
                "name": "status",
                "dtype": "string",
                "nullable": False,
                "description": "Transaction status",
                "constraints": {"enum": STATUSES},
            },
            {
                "name": "timestamp",
                "dtype": "timestamp",
                "nullable": False,
                "description": "Transaction creation timestamp (UTC)",
            },
            {
                "name": "product_id",
                "dtype": "string",
                "nullable": False,
                "description": "Product identifier (PROD#####)",
                "constraints": {"pattern": "^PROD[0-9]{5}$"},
            },
            {
                "name": "country",
                "dtype": "string",
                "nullable": True,
                "description": "ISO 3166-1 alpha-2 country code",
                "constraints": {"enum": COUNTRIES},
            },
        ],
        "quality_rules": [
            {"column": "id", "check_type": "nulls", "params": {"threshold": 0.0}},
            {"column": "id", "check_type": "duplicates", "params": {"key_columns": ["id"]}},
            {"column": "amount", "check_type": "nulls", "params": {"threshold": 0.05}},
            {
                "column": "amount",
                "check_type": "range",
                "params": {"min_val": 0.01, "max_val": MAX_AMOUNT},
            },
            {
                "column": "status",
                "check_type": "referential",
                "params": {"valid_values": STATUSES},
            },
            {
                "column": "amount",
                "check_type": "drift",
                "params": {
                    "baseline_mean": baseline["mean"],
                    "baseline_std": baseline["std"],
                    "z_threshold": 3.0,
                },
                "baseline_source": (
                    f"Jan-Nov 2024 rows before drift injection (n={baseline['rows']})"
                ),
            },
        ],
        "partitioned_by": ["country"],
        "primary_key": ["id"],
        "created_at": "2024-01-01T00:00:00Z",
    }


def build(output_dir: Path) -> pd.DataFrame:
    """Generate both files into output_dir and return the transactions dataframe."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    df, baseline = generate_transactions(N_ROWS, rng)
    df.to_csv(output_dir / "transactions.csv", index=False)
    schema = generate_schema(baseline)
    (output_dir / "schema.json").write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    return df


def main() -> None:
    output_dir = Path(__file__).parent
    print(f"Generating {N_ROWS:,} transaction rows...")
    df = build(output_dir)
    csv_path = output_dir / "transactions.csv"
    print(f"Saved: {csv_path} ({len(df):,} rows, {csv_path.stat().st_size / 1024:.1f} KB)")

    drifted = pd.to_datetime(df["timestamp"]) >= DRIFT_START
    print("\nDataset stats:")
    print(f"  Total rows:      {len(df):,}")
    print(f"  Duplicate ids:   {df['id'].duplicated().sum():,}")
    print(f"  Null amounts:    {df['amount'].isna().sum():,} ({df['amount'].isna().mean():.1%})")
    print(f"  Null country:    {df['country'].isna().sum():,}")
    print(f"  Drifted rows:    {drifted.sum():,} (timestamp >= {DRIFT_START.date()})")
    print(f"  Amount mean:     {df['amount'].mean():.2f}")
    print(f"  Amount > {MAX_AMOUNT:,.0f}: {(df['amount'] > MAX_AMOUNT).sum():,}")
    print(f"\nSaved: {output_dir / 'schema.json'}")


if __name__ == "__main__":
    main()
