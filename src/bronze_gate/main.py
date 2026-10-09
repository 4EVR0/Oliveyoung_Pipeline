"""전처리 입력 선택·품질 게이트 실행 — bronze_to_silver가 로드하기 직전에 입력을 고르고 판정한다.

종료 코드: PASS·WARN·우회 진행 → 고른 입력을 돌려줌, BLOCK → 99(DAG skip_on_exit_code),
BLOCK인데 DQ 기록 실패 → 1(알림 근거가 없으니 DAG 실패 알림으로 드러나게).
"""

import json
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

from config.settings import OliveyoungIceberg, S3
from oliveyoung_common import s3_paths
from oliveyoung_common.batch import build_run_id
from oliveyoung_common.dq_metrics import write_dq_metrics
from oliveyoung_common.logging import log_dq
from src.bronze_gate.decide import BLOCK, MAX_REASONS, GateResult, decide_gate, index_files, run_sort_key

logger = logging.getLogger(__name__)

STAGE = "bronze_gate"
BLOCK_EXIT_CODE = 99
_MANIFEST_PREFIX = "oliveyoung/_manifests/"


def _s3():
    return boto3.client("s3", region_name=S3.REGION)


def load_manifest(client, run_id: str) -> dict | None:
    """manifest 조회. 없으면 None, 그 밖의 S3 오류는 그대로 올린다(fail-closed)."""
    try:
        obj = client.get_object(Bucket=S3.BUCKET, Key=s3_paths.manifest_key(run_id))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    return json.loads(obj["Body"].read())


def list_bronze_files(client) -> list[str]:
    """bronze 전체 파일 목록(s3://.../oliveyoung/main/sub/run_id=X/*.json). _manifests·_optimized 등은 제외."""
    files = []
    prefix = f"{S3.BRONZE_PREFIX}/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=S3.BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key[len(prefix):].split("/")
            if len(parts) == 4 and parts[2].startswith("run_id=") and key.endswith(".json"):
                files.append(f"s3://{S3.BUCKET}/{key}")
    return files


def list_manifest_runs(client) -> list[str]:
    """manifest가 있는 정상 run_id 목록(part가 0개인 run 포함)."""
    run_ids = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=S3.BUCKET, Prefix=_MANIFEST_PREFIX, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            rid = cp["Prefix"][len(_MANIFEST_PREFIX):].strip("/").removeprefix("run_id=")
            if run_sort_key(rid) is not None:
                run_ids.append(rid)
    return run_ids


def load_inputs(client) -> tuple[dict, dict]:
    """(카테고리별 run 파일 색인, 정상 run_id별 manifest).

    manifest를 **먼저** 스냅샷으로 읽고 그다음 파일 목록을 읽는다. 순서가 반대면 파일 목록을 읽은 뒤
    크롤이 끝났을 때 "완료" manifest + 반쪽 파일 목록을 통과시킬 수 있다. 스냅샷 뒤에 파일이 생긴
    새 run은 manifest 없음(None)으로 남아 후보에서 탈락한다. part가 0개인 run도 manifest로 보인다.
    """
    manifests = {rid: load_manifest(client, rid) for rid in sorted(list_manifest_runs(client))}
    index = index_files(list_bronze_files(client))
    for rid in {r for runs in index.values() for r in runs if run_sort_key(r) is not None}:
        manifests.setdefault(rid, None)
    return index, manifests


def attach_source_run_id(raw_df):
    """DuckDB filename 컬럼(파일 경로)에서 run_id를 뽑아 행 출처 source_run_id로 남긴다(정상·백필 공용)."""
    if "filename" in raw_df.columns:
        raw_df["source_run_id"] = raw_df.pop("filename").astype(str).str.extract(r"run_id=([^/]+)/", expand=False)
    return raw_df


def _override_requested() -> bool:
    mode = os.environ.get("GATE_MODE", "").strip()
    if mode and mode != "skip":
        logger.warning("알 수 없는 GATE_MODE=%r — 무시(우회는 정확히 'skip'만)", mode)
    return mode == "skip"


def _record(result: GateResult, batch_date: str) -> bool:
    """로그 + DQ(리포트 포함). DQ 기록 성공 여부를 돌려준다."""
    run_id = build_run_id(STAGE)
    log_dq(logger, stage=STAGE, batch_job=run_id, **result.metrics)
    for reason in result.reasons:
        logger.info("[게이트] %s", reason)
    try:
        write_dq_metrics(
            OliveyoungIceberg.get_catalog(),
            stage=STAGE,
            batch_date=batch_date,
            run_id=run_id,
            report_webhook=os.environ.get("DISCORD_DQ_WEBHOOK_URL"),
            report_lines=result.reasons[:MAX_REASONS],
            **result.metrics,
        )
        return True
    except Exception as e:
        logger.warning("게이트 dq_metrics 적재 실패: %s", e)
        return False


def run_bronze_gate(resolve_batch_date) -> GateResult:
    """입력 선택·판정·기록 후 BLOCK이면 종료한다. 진행 가능하면 결과(고른 입력 포함)를 돌려준다.

    resolve_batch_date(max_run_id): bronze_to_silver와 같은 batch_date 규칙(DQ 기록용).
    """
    print("2-1. 입력 선택·품질 게이트...")
    override = _override_requested()
    index, manifests = load_inputs(_s3())
    if not index:
        raise RuntimeError("bronze 파일을 찾지 못했습니다")

    result = decide_gate(index, manifests, override=override)
    selected = [rid for p in result.plans.values() if (rid := p.selected_run) and run_sort_key(rid)]
    batch_date = resolve_batch_date(max(selected, key=run_sort_key) if selected else "")
    print(f"   판정: gate_status={result.status} {result.metrics}")
    for p in sorted(result.plans.values(), key=lambda p: p.prefix):
        logger.info("[입력] %s → %s (%s, %s일 전)%s", p.key or p.prefix, p.selected_run, p.status,
                    p.age_days, f" 건너뜀: {p.skipped}" if p.skipped else "")
    recorded = _record(result, batch_date)

    if result.status == BLOCK:
        if not recorded:
            logger.error("게이트 보류인데 DQ 기록 실패 — 보류 알림 근거가 없어 태스크를 실패시킴")
            sys.exit(1)
        logger.warning("게이트 보류 — bronze 로드 전에 중단(exit %d)", BLOCK_EXIT_CODE)
        sys.exit(BLOCK_EXIT_CODE)
    return result
