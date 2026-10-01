"""
Benchmark Bronze load strategies for Bronze -> Silver.

This script only reads the latest Bronze JSON files and writes optimized Bronze
copies under S3.BRONZE_OPTIMIZED_PATH for compacted JSON and Parquet tests. It
does not run cleaning, Iceberg writes, or CSV exports.

Usage:
    python scripts/benchmark_bronze_load_formats.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import DuckDB  # noqa: E402
from src.bronze_to_silver.pipeline import (  # noqa: E402
    bronze_optimized_path,
    load_bronze_data_from_files,
    materialize_bronze_compacted_json,
    materialize_bronze_parquet,
)


def _timed(label: str, fn) -> tuple[Any, dict[str, Any]]:
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    value = fn()
    return value, {
        "stage": label,
        "wall_seconds": round(time.perf_counter() - wall_start, 6),
        "cpu_seconds": round(time.process_time() - cpu_start, 6),
    }


def _row_count(df) -> int:
    return int(len(df))


def main() -> None:
    output_dir = Path(os.environ.get("BRONZE_LOAD_BENCHMARK_DIR", "profile_results"))
    output_dir.mkdir(parents=True, exist_ok=True)

    con = DuckDB.get_connection()
    latest_files = DuckDB.get_latest_bronze_files(con)

    results: list[dict[str, Any]] = []
    metadata = {
        "source_file_count": len(latest_files),
        "compacted_json_path": bronze_optimized_path(latest_files, "json"),
        "parquet_path": bronze_optimized_path(latest_files, "parquet"),
    }

    direct_df, direct_timing = _timed(
        "json_direct_read",
        lambda: load_bronze_data_from_files(
            con,
            latest_files,
            load_format="json",
            materialize_optimized=False,
        )[0],
    )
    direct_timing["rows_out"] = _row_count(direct_df)
    results.append(direct_timing)

    _, compact_write_timing = _timed(
        "compacted_json_materialize",
        lambda: materialize_bronze_compacted_json(con, latest_files),
    )
    results.append(compact_write_timing)

    compact_df, compact_read_timing = _timed(
        "compacted_json_cached_read",
        lambda: load_bronze_data_from_files(
            con,
            latest_files,
            load_format="compacted_json",
            materialize_optimized=False,
        )[0],
    )
    compact_read_timing["rows_out"] = _row_count(compact_df)
    results.append(compact_read_timing)

    _, parquet_write_timing = _timed(
        "parquet_materialize",
        lambda: materialize_bronze_parquet(con, latest_files),
    )
    results.append(parquet_write_timing)

    parquet_df, parquet_read_timing = _timed(
        "parquet_cached_read",
        lambda: load_bronze_data_from_files(
            con,
            latest_files,
            load_format="parquet",
            materialize_optimized=False,
        )[0],
    )
    parquet_read_timing["rows_out"] = _row_count(parquet_df)
    results.append(parquet_read_timing)

    row_counts = {
        "json_direct": _row_count(direct_df),
        "compacted_json": _row_count(compact_df),
        "parquet": _row_count(parquet_df),
    }
    if len(set(row_counts.values())) != 1:
        raise RuntimeError(f"Row count mismatch: {row_counts}")

    report = {
        "metadata": metadata,
        "row_counts": row_counts,
        "results": results,
    }
    output_path = output_dir / f"bronze_load_format_benchmark_{time.strftime('%Y%m%d_%H%M%S')}.json"
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nbenchmark_result={output_path}")


if __name__ == "__main__":
    main()
