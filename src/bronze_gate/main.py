"""전처리 입력 품질 게이트 실행 — bronze_to_silver가 로드하기 직전에 같은 파일 목록으로 판정한다.

종료 코드: PASS·WARN·우회 진행 → 그대로 진행(반환), BLOCK → 99(DAG skip_on_exit_code),
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
from src.bronze_gate.decide import (
    BLOCK, MAX_REASONS, RUN_ID_PATTERN, GateResult, decide_gate, group_inputs,
)

logger = logging.getLogger(__name__)

STAGE = "bronze_gate"
BLOCK_EXIT_CODE = 99
RECENT_RUNS = 6                          # 연속 누락 계산에 쓰는 최근 완료 크롤 수
_MANIFEST_PREFIX = "oliveyoung/_manifests/"


def _s3():
    return boto3.client("s3", region_name=S3.REGION)


def _load_manifest(client, run_id: str) -> dict | None:
    """manifest 조회. 없으면 None(판별 불가), 그 밖의 S3 오류는 그대로 올린다(fail-closed)."""
    try:
        obj = client.get_object(Bucket=S3.BUCKET, Key=s3_paths.manifest_key(run_id))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    return json.loads(obj["Body"].read())


def _recent_manifests(client, cache: dict[str, dict | None]) -> list[dict]:
    """in_progress가 아닌 최근 크롤 manifest를 최신순으로 RECENT_RUNS개."""
    run_ids = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=S3.BUCKET, Prefix=_MANIFEST_PREFIX, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            rid = cp["Prefix"][len(_MANIFEST_PREFIX):].strip("/").removeprefix("run_id=")
            if RUN_ID_PATTERN.match(rid):
                run_ids.append(rid)

    recent = []
    for rid in sorted(run_ids, reverse=True):
        if rid not in cache:
            cache[rid] = _load_manifest(client, rid)
        manifest = cache[rid]
        if manifest and manifest.get("status") != "in_progress":
            recent.append(manifest)
        if len(recent) >= RECENT_RUNS:
            break
    return recent


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


def run_bronze_gate(latest_files: list[str], batch_date: str) -> GateResult:
    """판정·기록 후 BLOCK이면 종료한다. 진행 가능하면 결과를 돌려준다."""
    print("2-1. 입력 품질 게이트...")
    override = _override_requested()
    client = _s3()

    run_ids = sorted({rid for _, rid in group_inputs(latest_files) if RUN_ID_PATTERN.match(rid)})
    manifests = {rid: _load_manifest(client, rid) for rid in run_ids}

    try:
        recent = _recent_manifests(client, dict(manifests))
    except Exception as e:
        # 누락·연속 누락 판정만 생략하고 부분 수집 판정은 그대로 수행
        logger.warning("최근 manifest 조회 실패 — 누락 판정 생략: %s", e)
        recent = []

    result = decide_gate(latest_files, manifests, recent, override=override)
    print(f"   판정: gate_status={result.status} {result.metrics}")
    recorded = _record(result, batch_date)

    if result.status == BLOCK:
        if not recorded:
            logger.error("게이트 보류인데 DQ 기록 실패 — 보류 알림 근거가 없어 태스크를 실패시킴")
            sys.exit(1)
        logger.warning("게이트 보류 — bronze 로드 전에 중단(exit %d)", BLOCK_EXIT_CODE)
        sys.exit(BLOCK_EXIT_CODE)
    return result
