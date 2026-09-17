"""
Benchmark Bronze read formats for profiling.

Compares:
- current small JSON files on S3
- compacted NDJSON on S3
- compacted Parquet on S3
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from io import BytesIO
import json
import os
from pathlib import Path
import time

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import DuckDB, S3
from oliveyoung_common.batch import build_run_id


@dataclass
class Measurement:
    stage: str
    wall_seconds: float
    cpu_seconds: float
    cpu_wall_ratio: float
    rows: int | None = None
    bytes: int | None = None
    path: str | None = None
    note: str | None = None


def measure(stage: str, func):
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    result = func()
    wall_seconds = time.perf_counter() - wall_start
    cpu_seconds = time.process_time() - cpu_start
    ratio = cpu_seconds / wall_seconds if wall_seconds else 0
    return result, wall_seconds, cpu_seconds, ratio


def s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def upload_bytes(key: str, data: bytes, content_type: str) -> None:
    boto3.client("s3", region_name=S3.REGION).put_object(
        Bucket=S3.BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def read_original(files: list[str]) -> pd.DataFrame:
    con = DuckDB.get_connection()
    file_list_sql = ", ".join(f"'{path}'" for path in files)
    return con.execute(
        f"SELECT * FROM read_json_auto([{file_list_sql}], ignore_errors=true)"
    ).df()


def read_compacted_json(path: str) -> pd.DataFrame:
    con = DuckDB.get_connection()
    return con.execute(f"SELECT * FROM read_json_auto('{path}', ignore_errors=true)").df()


def read_parquet(path: str) -> pd.DataFrame:
    con = DuckDB.get_connection()
    return con.execute(f"SELECT * FROM read_parquet('{path}')").df()


def dataframe_to_json_bytes(df: pd.DataFrame) -> bytes:
    return df.to_json(orient="records", lines=True, force_ascii=False).encode("utf-8")


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def write_outputs(result: dict) -> None:
    out_dir = Path("profile_results")
    out_dir.mkdir(exist_ok=True)

    json_path = out_dir / "bronze_format_benchmark_20260917.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    rows = result["summary_rows"]
    md_path = out_dir / "bronze_format_benchmark_summary_20260917.md"
    lines = [
        "# Bronze Format Benchmark Summary",
        "",
        "## 실행 요약",
        "",
        f"- 입력 파일 수: `{result['input']['file_count']}`",
        f"- 입력 행 수: `{result['input']['row_count']}`",
        f"- compacted JSON: `{result['artifacts']['compacted_json_path']}`",
        f"- compacted Parquet: `{result['artifacts']['parquet_path']}`",
        "",
        "## 읽기 성능 비교",
        "",
        "| format | wall_s | cpu_s | cpu/wall | rows | file_size_mb | 비고 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {format} | `{wall_seconds:.2f}` | `{cpu_seconds:.2f}` | `{cpu_wall_ratio:.2f}` | "
            "`{rows}` | `{file_size_mb}` | {note} |".format(**row)
        )

    lines.extend(
        [
            "",
            "## 해석",
            "",
            result["conclusion"],
            "",
            "## 생성 비용",
            "",
            "| stage | wall_s | cpu_s | size_mb |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for item in result["compaction_costs"]:
        lines.append(
            f"| `{item['stage']}` | `{item['wall_seconds']:.2f}` | "
            f"`{item['cpu_seconds']:.2f}` | `{item['size_mb']:.2f}` |"
        )

    md_path.write_text("\n".join(lines) + "\n")


def main() -> None:
    run_id = build_run_id("bronze_format_benchmark")
    base_key = f"profile_results/bronze_format_benchmark/{run_id}"

    con = DuckDB.get_connection()
    files = DuckDB.get_latest_bronze_files(con)

    measurements: list[Measurement] = []

    raw_df, wall, cpu, ratio = measure("read_original_s3_json_parts", lambda: read_original(files))
    measurements.append(
        Measurement(
            stage="read_original_s3_json_parts",
            wall_seconds=wall,
            cpu_seconds=cpu,
            cpu_wall_ratio=ratio,
            rows=len(raw_df),
            note=f"{len(files)} S3 JSON files",
        )
    )

    json_bytes, wall, cpu, ratio = measure("build_compacted_json", lambda: dataframe_to_json_bytes(raw_df))
    json_key = f"{base_key}/bronze_compacted.ndjson"
    upload_bytes(json_key, json_bytes, "application/x-ndjson")
    json_path = s3_url(S3.BUCKET, json_key)
    measurements.append(
        Measurement(
            stage="build_compacted_json",
            wall_seconds=wall,
            cpu_seconds=cpu,
            cpu_wall_ratio=ratio,
            rows=len(raw_df),
            bytes=len(json_bytes),
            path=json_path,
        )
    )

    parquet_bytes, wall, cpu, ratio = measure("build_parquet", lambda: dataframe_to_parquet_bytes(raw_df))
    parquet_key = f"{base_key}/bronze_compacted.parquet"
    upload_bytes(parquet_key, parquet_bytes, "application/octet-stream")
    parquet_path = s3_url(S3.BUCKET, parquet_key)
    measurements.append(
        Measurement(
            stage="build_parquet",
            wall_seconds=wall,
            cpu_seconds=cpu,
            cpu_wall_ratio=ratio,
            rows=len(raw_df),
            bytes=len(parquet_bytes),
            path=parquet_path,
        )
    )

    compacted_json_df, wall, cpu, ratio = measure("read_compacted_s3_json", lambda: read_compacted_json(json_path))
    measurements.append(
        Measurement(
            stage="read_compacted_s3_json",
            wall_seconds=wall,
            cpu_seconds=cpu,
            cpu_wall_ratio=ratio,
            rows=len(compacted_json_df),
            bytes=len(json_bytes),
            path=json_path,
            note="single NDJSON file on S3",
        )
    )

    parquet_df, wall, cpu, ratio = measure("read_s3_parquet", lambda: read_parquet(parquet_path))
    measurements.append(
        Measurement(
            stage="read_s3_parquet",
            wall_seconds=wall,
            cpu_seconds=cpu,
            cpu_wall_ratio=ratio,
            rows=len(parquet_df),
            bytes=len(parquet_bytes),
            path=parquet_path,
            note="single Snappy Parquet file on S3",
        )
    )

    read_rows = []
    for stage, label, note in [
        ("read_original_s3_json_parts", "original_json_parts", f"{len(files)} S3 JSON files"),
        ("read_compacted_s3_json", "compacted_json", "single NDJSON file"),
        ("read_s3_parquet", "parquet", "single Snappy Parquet file"),
    ]:
        m = next(item for item in measurements if item.stage == stage)
        size_mb = None if m.bytes is None else round(m.bytes / 1024 / 1024, 2)
        read_rows.append(
            {
                "format": label,
                "wall_seconds": round(m.wall_seconds, 6),
                "cpu_seconds": round(m.cpu_seconds, 6),
                "cpu_wall_ratio": round(m.cpu_wall_ratio, 6),
                "rows": m.rows,
                "file_size_mb": "" if size_mb is None else size_mb,
                "note": note,
            }
        )

    original_wall = read_rows[0]["wall_seconds"]
    parquet_wall = read_rows[2]["wall_seconds"]
    json_wall = read_rows[1]["wall_seconds"]
    conclusion = (
        f"현재 small JSON 방식은 {len(files)}개 S3 JSON 파일에서 {len(raw_df)}건을 읽는 데 "
        f"{original_wall:.2f}s가 걸렸다. compacted JSON은 {json_wall:.2f}s, "
        f"Parquet은 {parquet_wall:.2f}s로 측정됐다. 파일 수를 줄이는 것만으로도 S3 object 요청과 "
        "JSON 파싱 오버헤드가 줄고, Parquet은 컬럼형 저장/압축 덕분에 Bronze->Silver 입력 포맷으로 가장 유리하다."
    )

    result = {
        "run_id": run_id,
        "input": {
            "bronze_glob": S3.BRONZE_GLOB,
            "file_count": len(files),
            "row_count": len(raw_df),
        },
        "artifacts": {
            "compacted_json_path": json_path,
            "parquet_path": parquet_path,
        },
        "summary_rows": read_rows,
        "compaction_costs": [
            {
                "stage": item.stage,
                "wall_seconds": item.wall_seconds,
                "cpu_seconds": item.cpu_seconds,
                "size_mb": 0 if item.bytes is None else item.bytes / 1024 / 1024,
            }
            for item in measurements
            if item.stage in {"build_compacted_json", "build_parquet"}
        ],
        "measurements": [asdict(item) for item in measurements],
        "conclusion": conclusion,
    }
    write_outputs(result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
