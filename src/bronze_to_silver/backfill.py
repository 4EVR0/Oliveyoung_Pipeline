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
    # manifest["parts"]: flat list of {key, part_num, category, subcategory,
    # product_count, uploaded_at}. This crawl is chronically "interrupted" (no
    # run in recent history reaches "completed"), so gating on status would
    # block every backfill. We instead gate on *integrity*: no file exists in
    # S3 that the manifest doesn't know about (rogue), and the parts that do
    # exist account for exactly the rows we loaded. A category later deleted
    # from S3 (e.g. 맨즈케어) only shrinks "missing_parts" (informational) and
    # does not fail integrity, since the remaining parts' counts still match.
    manifest_part_counts = {
        part["key"]: part.get("product_count")
        for part in manifest.get("parts", []) if "key" in part
    }
    manifest_parts = set(manifest_part_counts)
    discovered_parts = {f.removeprefix(f"s3://{S3.BUCKET}/") for f in files}
    rogue_parts = discovered_parts - manifest_parts
    missing_parts = manifest_parts - discovered_parts
    present_product_count = sum(
        count for key, count in manifest_part_counts.items()
        if key in discovered_parts and count is not None
    )
    manifest_integrity_ok = not rogue_parts and present_product_count == len(raw_df)
    existing = _existing(catalog, batch_date, batch_job)
    preview = {
        "source_run_id": source_run_id,
        "batch_date": batch_date,
        "batch_job": batch_job,
        "manifest_status": manifest.get("status", "unknown"),
        "manifest_total_products": manifest.get("total_products"),
        "manifest_integrity_ok": manifest_integrity_ok,
        # In manifest but no longer in S3 — informational only (e.g. a
        # category retired after this run); does not block the backfill.
        "manifest_missing_parts": sorted(missing_parts),
        # In S3 but not in manifest — an actual integrity gap.
        "manifest_rogue_parts": sorted(rogue_parts),
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
    # in_progress = the crawl is still writing; data is a moving target. No
    # override can make this safe, unlike "interrupted" (a finished-but-partial
    # crawl, which is this project's normal state and not itself a reason to
    # block — see manifest_integrity_ok below).
    if preview["manifest_status"] == "in_progress":
        raise ValueError("크롤이 아직 진행 중입니다(in_progress). 완료 후 다시 시도하세요 — override 불가")
    if not preview["manifest_integrity_ok"] and not allow_incomplete:
        raise ValueError(
            "manifest 무결성 이상입니다(S3에 manifest가 모르는 파일이 있거나, 존재하는 part의 "
            "product_count 합이 로드한 행수와 다릅니다). 명시적 allow_incomplete=true가 필요합니다"
        )
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
    body = _format_backfill_report(preview, metrics, allow_incomplete)
    try:
        request = urllib.request.Request(
            webhook, data=json.dumps({"content": body}, ensure_ascii=False).encode("utf-8"),
            # The default urllib User-Agent may be rejected by Discord/Cloudflare.
            headers={"Content-Type": "application/json", "User-Agent": "oliveyoung-backfill/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as exc:
        # An HTTPError can contain the webhook URL (including its secret token).
        logger.warning("백필 데이터 검증 성공, Discord 리포트만 전송 실패: %s", type(exc).__name__)


def _format_backfill_report(preview: dict, metrics: dict, allow_incomplete: bool) -> str:
    """Format an operator-facing success message; never imply graph/current parity.

    Mirrors the crawl/pipeline report's visual style (icon + bracketed scope
    title, ━ divider, one emoji-labeled row per fact) so backfill reports read
    as the same family of notification, not a separate ad-hoc format.
    """
    status = preview["manifest_status"]
    integrity_ok = preview["manifest_integrity_ok"]
    # This crawl is chronically interrupted, so "interrupted" alone is not an
    # anomaly — it's informational (partial category coverage, still worth
    # flagging). A failed integrity check is the real anomaly: it means an
    # operator explicitly overrode a file/count mismatch to get here.
    icon = "⚠️" if not integrity_ok else ("ℹ️" if status != "completed" else "✅")
    processed = metrics["silver_ok"] + metrics["silver_error"]
    rate = metrics["silver_error"] / processed if processed else 0.0

    # 필드 라벨(bronze 로드/정상/에러/오류율)은 정상 파이프라인 완료 리포트
    # (oliveyoung_common/dq_metrics.py:_send_report)와 동일한 용어를 그대로 써서,
    # 제목의 '백필' 구분자 없이도 같은 계열의 알림임을 알 수 있게 한다.
    lines = [
        f"{icon} **[올리브영 전처리 백필] 정제 완료**",
        "━" * 20,
        f"📅 배치   {preview['batch_date']}",
        f"📥 bronze 로드   {metrics['bronze_loaded']:,}건",
        f"✅ 정상   {metrics['silver_ok']:,}건",
        f"⚠️ 에러   {metrics['silver_error']:,}건",
        f"📊 오류율   {rate:.1%}",
        f"📂 서브카테고리   {len(preview['subcategories']):,}개 ({preview['part_count']:,} part)",
        f"🧬 소스 run   `{preview['source_run_id']}` → 백필 키 `{preview['batch_job']}`",
    ]
    if status != "completed":
        lines.append(f"ℹ️ manifest 상태   {status} (이 크롤의 정상 상태 — 부분 크롤)")
    error_types = sorted(
        ((name.removeprefix("err_"), int(count)) for name, count in metrics.items()
         if name.startswith("err_") and count),
        key=lambda item: (-item[1], item[0]),
    )
    if error_types:
        top = ", ".join(f"{re.sub(r'[`\r\n]', '_', name)[:48]} {count:,}건"
                         for name, count in error_types[:5])
        lines.append(f"🔎 오류 유형 Top{min(len(error_types), 5)}   {top}")
    if preview.get("manifest_missing_parts"):
        lines.append(
            f"ℹ️ manifest엔 있으나 S3엔 없는 part {len(preview['manifest_missing_parts']):,}개"
            "(의도적 삭제 가능 — 예: 폐지된 카테고리)"
        )
    if integrity_ok:
        lines.append("🔍 manifest 무결성 정상")
    else:
        lines.append(f"🔍 manifest 무결성 이상 — override 사용: `{allow_incomplete}`")
    dashboard = os.environ.get("BACKFILL_DQ_DASHBOARD_URL", "").strip()
    if dashboard.startswith(("https://", "http://")):
        lines.append(f"🔗 [DQ 대시보드(정상 배치)]({dashboard})")
    lines.append("📊 백필 DQ   `/dq/latest?stage=bronze_to_silver_backfill&metric=silver_ok`")
    lines.append("🚫 `silver_current`·gold·CDC·Neo4j 미반영. 과거 Bronze를 현재 정제 규칙으로 처리했습니다.")
    return "\n".join(lines)


def _commit_backfill(catalog, preview: dict, silver_df: pd.DataFrame,
                     error_df: pd.DataFrame, allow_incomplete: bool) -> None:
    """Only a fully verified write may emit the success report."""
    _replace_history(catalog, silver_df, preview["batch_date"], preview["batch_job"])
    metrics = _replace_dq(catalog, preview, error_df, len(silver_df))
    _verify(catalog, preview, len(silver_df), metrics)
    print(json.dumps({"result": "verified", "metrics": metrics}, ensure_ascii=False), flush=True)
    _report(preview, metrics, allow_incomplete)


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
        _commit_backfill(catalog, preview, silver_df, error_df, allow_incomplete)
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
