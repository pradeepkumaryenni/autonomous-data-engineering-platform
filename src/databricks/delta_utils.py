from __future__ import annotations

import json
import math
from dataclasses import dataclass, field


@dataclass
class TableStats:
    table_name: str
    location: str
    num_files: int
    size_bytes: int
    partitioned_by: list[str] = field(default_factory=list)
    last_modified: str = ""
    properties: dict = field(default_factory=dict)

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)

    @property
    def avg_file_size_mb(self) -> float:
        if self.num_files == 0:
            return 0.0
        return self.size_mb / self.num_files


class DeltaUtils:
    """Utilities for Delta Lake table analysis and optimization."""

    @staticmethod
    def parse_describe_detail(rows: list[dict]) -> TableStats:
        """Parse the single row returned by `DESCRIBE DETAIL <table>`.

        DESCRIBE DETAIL is the Delta command that reports file count and size
        (numFiles, sizeInBytes, partitionColumns). Values may arrive as strings
        from the SQL Statement API, so everything is parsed defensively.
        """
        row = rows[0] if rows else {}

        def parse_int(val: object) -> int:
            try:
                return int(str(val).replace(",", "").strip())
            except (TypeError, ValueError):
                return 0

        partitions = row.get("partitionColumns") or []
        if isinstance(partitions, str):
            try:
                partitions = json.loads(partitions)
            except json.JSONDecodeError:
                partitions = [p.strip() for p in partitions.strip("[]").split(",") if p.strip()]

        return TableStats(
            table_name=str(row.get("name", "unknown")),
            location=str(row.get("location", "")),
            num_files=parse_int(row.get("numFiles", 0)),
            size_bytes=parse_int(row.get("sizeInBytes", 0)),
            partitioned_by=[str(p).strip("\"' ") for p in partitions],
            last_modified=str(row.get("lastModified", "")),
            properties={"format": row.get("format", "")},
        )

    @staticmethod
    def compute_partition_skew(partition_counts: dict[str, int]) -> float:
        """Compute partition skew coefficient (0=uniform, 1=maximally skewed).

        Uses coefficient of variation normalized to [0, 1].
        """
        if not partition_counts:
            return 0.0
        counts = list(partition_counts.values())
        if len(counts) == 1:
            return 0.0
        mean = sum(counts) / len(counts)
        if mean == 0:
            return 0.0
        variance = sum((c - mean) ** 2 for c in counts) / len(counts)
        std = math.sqrt(variance)
        cv = std / mean  # coefficient of variation
        # Normalize: CV of 0 = no skew, we cap at 1.0
        return min(cv, 1.0)

    @staticmethod
    def estimate_compaction_needed(
        file_count: int,
        avg_file_size_mb: float,
        target_file_size_mb: float = 128.0,
    ) -> bool:
        """Return True if table should be compacted (OPTIMIZE).

        Compaction is needed when:
        - Many small files exist (avg < 50% of target), OR
        - File count is excessive (>1000 files and avg < target)
        """
        if file_count == 0:
            return False
        is_small_files = avg_file_size_mb < (target_file_size_mb * 0.5)
        is_excessive_files = file_count > 1000 and avg_file_size_mb < target_file_size_mb
        return is_small_files or is_excessive_files

    @staticmethod
    def calculate_z_order_benefit(column_cardinality: int, query_frequency: int) -> float:
        """Estimate Z-ORDER benefit score (0-1).

        Higher cardinality + higher query frequency = higher benefit.
        Score = tanh(log(cardinality * query_frequency) / 10)
        """
        if column_cardinality <= 0 or query_frequency <= 0:
            return 0.0
        product = column_cardinality * query_frequency
        score = math.tanh(math.log(product + 1) / 10.0)
        return round(min(score, 1.0), 4)

    @staticmethod
    def generate_optimize_command(table_name: str, z_order_cols: list[str] | None = None) -> str:
        """Generate OPTIMIZE SQL command, optionally with Z-ORDER."""
        base = f"OPTIMIZE {table_name}"
        if z_order_cols:
            cols = ", ".join(z_order_cols)
            return f"{base} ZORDER BY ({cols});"
        return f"{base};"

    @staticmethod
    def generate_vacuum_command(table_name: str, retain_hours: int = 168) -> str:
        """Generate VACUUM SQL command with specified retention period."""
        return f"VACUUM {table_name} RETAIN {retain_hours} HOURS;"
