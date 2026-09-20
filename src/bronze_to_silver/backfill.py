"""Manual, history-only replay of one exact Olive Young crawl run.

Dry-run is read-only. Apply requires a separate, explicit confirmation; neither
path calls the normal pipeline's current/gold/CDC/Neo4j writers.
"""

import argparse
import json
import logging
import os
import re
import urllib.request
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from pyiceberg.expressions import And, EqualTo

from config.settings import DuckDB, OliveyoungIceberg, S3
from models.batch_metadata import BatchMetadata
from oliveyoung_common import s3_paths
from silver_pipeline.write_silver import _build_arrow_table_for_silver

logger = logging.getLogger(__name__)
STAGE = "bronze_to_silver_backfill"


def _key(source_run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", source_run_id):
        raise ValueError("source_run_id는 영문·숫자·_·-만 허용합니다")
    return f"backfill_{source_run_id}"


def _batch_date(catalog, source_run_id: str) -> str:
    table = catalog.load_table(OliveyoungIceberg.DQ_METRICS_TABLE)
    rows = table.scan(
        row_filter=And(EqualTo("stage", "crawl"), EqualTo("run_id", source_run_id)),
        selected_fields=("batch_date",),
    ).to_arrow()
    dates = set(rows.column("batch_date").to_pylist())
    if len(dates) != 1:
        raise ValueError(f"crawl DQ batch_date가 없거나 충돌합니다: run={source_run_id}, dates={dates}")
    batch_date = dates.pop()
    datetime.strptime(batch_date, "%Y-%m-%d")
    return batch_date


def _manifest(source_run_id: str) -> dict:
    try:
        result = boto3.client("s3", region_name=S3.REGION).get_object(
            Bucket=S3.BUCKET, Key=s3_paths.manifest_key(source_run_id)
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return {"status": "missing"}
        raise
    return json.loads(result["Body"].read())


def _existing(catalog, batch_date: str, batch_job: str) -> dict:
    day = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    history = catalog.load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
    history_rows = history.scan(
        row_filter=EqualTo("batch_date", day), selected_fields=("batch_job",)
    ).to_arrow().column("batch_job").to_pylist()
    other_jobs = sorted({str(job) for job in history_rows if job != batch_job})
    dq = catalog.load_table(OliveyoungIceberg.DQ_METRICS_TABLE)
    dq_rows = dq.scan(
        row_filter=EqualTo("batch_date", batch_date),
        selected_fields=("stage", "run_id"),
    ).to_arrow().to_pylist()
    backfill_runs = [row["run_id"] for row in dq_rows if row["stage"] == STAGE]
    normal_runs = sorted({str(row["run_id"]) for row in dq_rows
                          if row["stage"] == "bronze_to_silver"})
    other_dq = sorted({str(run) for run in backfill_runs if run != batch_job})
    return {
        "history_same_key": sum(job == batch_job for job in history_rows),
        "history_other_jobs": other_jobs,
        "dq_same_key": sum(run == batch_job for run in backfill_runs),
        "dq_other_runs": other_dq,
        "dq_normal_runs": normal_runs,
    }


def preflight(catalog, con, source_run_id: str) -> tuple[dict, pd.DataFrame]:
    batch_job = _key(source_run_id)
    batch_date = _batch_date(catalog, source_run_id)
    files = DuckDB.get_bronze_files_for_run(con, source_run_id)
    manifest = _manifest(source_run_id)
    # A strict parse is deliberate: an unreadable part must fail, not silently
    # produce a plausible partial backfill.
    raw_df = con.execute(
        "SELECT * FROM read_json_auto(?, ignore_errors=false, union_by_name=true)",
        [files],
    ).df()
    categories = sorted({"/".join(f.split("/run_id=")[0].split("/")[-2:]) for f in files})
    manifest_parts = {part["key"] for part in manifest.get("parts", []) if "key" in part}
    discovered_parts = {f.removeprefix(f"s3://{S3.BUCKET}/") for f in files}
    manifest_consistent = (
        manifest_parts == discovered_parts
        and manifest.get("total_products") == len(raw_df)
    )
    existing = _existing(catalog, batch_date, batch_job)
    preview = {
        "source_run_id": source_run_id,
        "batch_date": batch_date,
        "batch_job": batch_job,
        "manifest_status": manifest.get("status", "unknown"),
        "manifest_total_products": manifest.get("total_products"),
        "manifest_consistent": manifest_consistent,
        "manifest_missing_parts": sorted(manifest_parts - discovered_parts),
        "manifest_unlisted_parts": sorted(discovered_parts - manifest_parts),
        "subcategories": categories,
        "part_count": len(files),
        "bronze_rows": len(raw_df),
        "existing": existing,
    }
    return preview, raw_df


def _assert_safe(preview: dict, allow_incomplete: bool) -> None:
    existing = preview["existing"]
    if existing["history_other_jobs"] or existing["dq_other_runs"] or existing["dq_normal_runs"]:
        raise ValueError("동일 batch_date에 다른 history/DQ run이 있습니다. 자동 적재를 거부합니다")
    if (preview["manifest_status"] != "completed" or not preview["manifest_consistent"]) and not allow_incomplete:
        raise ValueError("manifest가 미완료이거나 입력 파일/건수와 불일치합니다. 명시적 allow_incomplete=true가 필요합니다")
    if preview["bronze_rows"] == 0:
        raise ValueError("Bronze JSON 0행은 백필하지 않습니다")


def _replace_history(catalog, silver_df: pd.DataFrame, batch_date: str, batch_job: str) -> None:
    table = catalog.load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
    arrow = (
        _build_arrow_table_for_silver(silver_df, table)
        if not silver_df.empty else pa.Table.from_batches([], schema=table.schema().as_arrow())
    )
    day = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    table.overwrite(
        arrow,
        overwrite_filter=And(EqualTo("batch_date", day), EqualTo("batch_job", batch_job)),
    )


def _replace_dq(catalog, preview: dict, error_df: pd.DataFrame, silver_count: int) -> dict:
    error_count = len(error_df)
    processed = silver_count + error_count
    metrics = {
        "bronze_loaded": preview["bronze_rows"],
        "silver_ok": silver_count,
        "silver_error": error_count,
        "error_rate": round(error_count / processed, 4) if processed else 0.0,
    }
    if error_count:
        if "error_type" not in error_df:
            raise ValueError("error_df에 error_type이 없습니다")
        counts = error_df["error_type"].fillna("UNCLASSIFIED").value_counts()
        metrics.update({f"err_{kind}": int(count) for kind, count in counts.items()})
        if sum(v for k, v in metrics.items() if k.startswith("err_")) != error_count:
            raise ValueError("error_type 합계가 silver_error와 다릅니다")

    table = catalog.load_table(OliveyoungIceberg.DQ_METRICS_TABLE)
    now = datetime.now(timezone.utc)
    rows = [{
        "batch_date": preview["batch_date"],
        "run_id": preview["batch_job"],
        "stage": STAGE,
        "metric_name": name,
        "metric_value": float(value),
        "target_table": OliveyoungIceberg.SILVER_HISTORY_TABLE,
        "created_at": now,
    } for name, value in metrics.items()]
    arrow = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    table.overwrite(
        arrow,
        overwrite_filter=And(
            And(EqualTo("stage", STAGE), EqualTo("batch_date", preview["batch_date"])),
            EqualTo("run_id", preview["batch_job"]),
        ),
    )
    return metrics


def _verify(catalog, preview: dict, silver_count: int, metrics: dict) -> None:
    day = datetime.strptime(preview["batch_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    history = catalog.load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
    actual_history = history.scan(row_filter=And(
        EqualTo("batch_date", day), EqualTo("batch_job", preview["batch_job"])
    ), selected_fields=("batch_job",)).to_arrow().num_rows
    dq = catalog.load_table(OliveyoungIceberg.DQ_METRICS_TABLE)
    actual_dq = dq.scan(row_filter=And(
        And(EqualTo("stage", STAGE), EqualTo("batch_date", preview["batch_date"])),
        EqualTo("run_id", preview["batch_job"]),
    ), selected_fields=("metric_name", "metric_value")).to_arrow()
    found = dict(zip(actual_dq.column("metric_name").to_pylist(),
                     actual_dq.column("metric_value").to_pylist()))
    if actual_history != silver_count or actual_dq.num_rows != len(metrics) or len(found) != len(metrics) or any(
        found.get(name) != float(value) for name, value in metrics.items()
    ):
        raise RuntimeError("백필 적재 후 history/DQ 검증 실패")


def _report(preview: dict, metrics: dict, allow_incomplete: bool) -> None:
    webhook = os.environ.get("DISCORD_DQ_WEBHOOK_URL")
    if not webhook:
        logger.warning("백필 완료 리포트 미발송: DISCORD_DQ_WEBHOOK_URL 미설정")
        return
    body = (
        "**과거 백필 완료 (history·DQ 전용)**\n"
        f"source_run_id: `{preview['source_run_id']}` | batch_date: `{preview['batch_date']}`\n"
        f"manifest: `{preview['manifest_status']}` | 부분 실행 승인: `{allow_incomplete}`\n"
        f"Bronze: {metrics['bronze_loaded']} / Silver: {metrics['silver_ok']} / Error: {metrics['silver_error']}\n"
        "현재 silver_current·gold·CDC·Neo4j에는 반영하지 않았습니다. "
        "과거 입력을 현재 정제 규칙으로 처리한 결과입니다."
    )
    try:
        request = urllib.request.Request(
            webhook, data=json.dumps({"content": body}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as exc:
        logger.warning("백필 데이터는 성공했지만 Discord 리포트 전송 실패: %s", exc)


def run(source_run_id: str, mode: str, confirm_source_run_id: str | None,
        allow_incomplete: bool = False) -> dict:
    _key(source_run_id)
    if mode not in {"dry-run", "apply"}:
        raise ValueError("mode는 dry-run 또는 apply여야 합니다")
    if mode == "apply" and confirm_source_run_id != source_run_id:
        raise ValueError("apply하려면 confirm_source_run_id를 정확히 일치시켜야 합니다")
    catalog = OliveyoungIceberg.get_catalog()
    con = DuckDB.get_connection()
    try:
        preview, raw_df = preflight(catalog, con, source_run_id)
        print(json.dumps({"mode": mode, **preview}, ensure_ascii=False, indent=2), flush=True)
        if mode == "dry-run":
            return preview
        _assert_safe(preview, allow_incomplete)
        # Import only for apply: a dry-run never loads dictionaries or writes tables.
        from src.bronze_to_silver.pipeline import load_dictionaries
        from src.bronze_to_silver.cleaner import process_pipeline

        dictionaries = load_dictionaries()
        batch = BatchMetadata(
            batch_job=preview["batch_job"],
            batch_date=datetime.strptime(preview["batch_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc),
        )
        silver_df, error_df = process_pipeline(
            df=raw_df, ac_automaton=dictionaries.ac_automaton,
            typo_list=dictionaries.typo_list, typo_regex_list=dictionaries.typo_regex_list,
            garbage_config=dictionaries.garbage_config,
            product_name_norm_list=dictionaries.product_name_norm_list,
            batch=batch, batch_date=preview["batch_date"],
        )
        _replace_history(catalog, silver_df, preview["batch_date"], preview["batch_job"])
        metrics = _replace_dq(catalog, preview, error_df, len(silver_df))
        _verify(catalog, preview, len(silver_df), metrics)
        print(json.dumps({"result": "verified", "metrics": metrics}, ensure_ascii=False), flush=True)
        _report(preview, metrics, allow_incomplete)
        return preview
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="History-only Olive Young backfill")
    parser.add_argument("--source-run-id", default=os.environ.get("BACKFILL_SOURCE_RUN_ID"), required=False)
    parser.add_argument("--mode", choices=("dry-run", "apply"),
                        default=os.environ.get("BACKFILL_MODE", "dry-run"))
    parser.add_argument("--confirm-source-run-id", default=os.environ.get("BACKFILL_CONFIRM_SOURCE_RUN_ID"))
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    if not args.source_run_id:
        parser.error("source_run_id가 필요합니다")
    allow_incomplete = args.allow_incomplete or os.environ.get("BACKFILL_ALLOW_INCOMPLETE", "false").lower() == "true"
    run(args.source_run_id, args.mode, args.confirm_source_run_id, allow_incomplete)


if __name__ == "__main__":
    main()
