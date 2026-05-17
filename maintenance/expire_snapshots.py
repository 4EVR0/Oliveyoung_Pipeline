"""
Iceberg 스냅샷 만료 및 Orphan 파일 정리 모듈

실행 시점: 2026-06-16 이후 (파이프라인 가동 약 2개월 후)

배경:
    - silver_current: 매 배치 overwrite → 이전 스냅샷의 parquet 파일이 S3에 누적됨
    - silver_history: append 전용 → 오래된 스냅샷 메타데이터가 누적됨
    - gold 테이블:   append 전용 → 동일

주의:
    CDC(_get_new_products, _get_removed_products)는 최신 2개 스냅샷을 사용하므로
    retain_last=2 를 반드시 지켜야 합니다.

사용법:
    python -m maintenance.expire_snapshots
    python -m maintenance.expire_snapshots --older-than-days 30 --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from config.settings import Iceberg

logger = logging.getLogger(__name__)


# 정리 대상 테이블 목록
_TARGET_TABLES = [
    Iceberg.SILVER_CURRENT_TABLE,
    Iceberg.SILVER_HISTORY_TABLE,
    Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE,
    Iceberg.GOLD_PRODUCT_CHANGE_LOG_TABLE,
]

# CDC가 prev/curr 스냅샷을 필요로 하므로 최소 2개는 유지
_RETAIN_LAST = 2


def expire_table_snapshots(
    table,
    older_than: datetime,
    dry_run: bool = False,
) -> dict:
    """
    단일 테이블의 오래된 스냅샷을 만료하고 orphan 파일을 삭제합니다.

    Args:
        table     : pyiceberg Table 인스턴스
        older_than: 이 시각보다 오래된 스냅샷을 만료
        dry_run   : True면 실제 삭제 없이 대상만 로깅

    Returns:
        {"table": str, "expired_snapshots": int, "orphan_files": int}
    """
    name = table.name()
    history = sorted(table.history(), key=lambda h: h.timestamp_ms)

    expired_candidates = [
        h for h in history
        if datetime.fromtimestamp(h.timestamp_ms / 1000, tz=timezone.utc) < older_than
    ]
    # retain_last 보장: 전체 스냅샷 수 - retain_last 이상은 지우지 않음
    safe_expire_count = max(0, len(history) - _RETAIN_LAST)
    expired_candidates = expired_candidates[:safe_expire_count]

    logger.info(
        f"[{name}] 전체 스냅샷={len(history)}개 / "
        f"만료 대상={len(expired_candidates)}개 / "
        f"유지={len(history) - len(expired_candidates)}개"
    )

    if dry_run:
        logger.info(f"[{name}] dry-run — 실제 삭제 건너뜀")
        return {"table": name, "expired_snapshots": len(expired_candidates), "orphan_files": 0}

    # ── 스냅샷 만료 ──────────────────────────────────────────
    expire_action = (
        table.expire_snapshots()
        .expire_older_than(older_than)
        .retain_last(_RETAIN_LAST)
    )
    expire_result = expire_action.commit()
    expired_count = len(getattr(expire_result, "deleted_manifest_files", []))
    logger.info(f"[{name}] 스냅샷 만료 완료")

    # ── Orphan 파일 삭제 ─────────────────────────────────────
    orphan_count = 0
    try:
        orphan_result = table.delete_orphan_files().older_than(older_than).execute()
        orphan_count = len(getattr(orphan_result, "orphan_file_locations", []))
        logger.info(f"[{name}] orphan 파일 {orphan_count}개 삭제 완료")
    except AttributeError:
        # pyiceberg 버전에 따라 delete_orphan_files 미지원 시
        logger.warning(f"[{name}] delete_orphan_files 미지원 — 스냅샷 만료만 적용됨")

    return {"table": name, "expired_snapshots": expired_count, "orphan_files": orphan_count}


def run_maintenance(older_than_days: int = 30, dry_run: bool = False) -> None:
    """
    전체 Iceberg 테이블에 대해 스냅샷 만료 + orphan 파일 정리를 실행합니다.

    Args:
        older_than_days: 기준일로부터 이 일수보다 오래된 스냅샷을 만료 (default: 30일)
        dry_run        : True면 삭제 없이 대상만 출력
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=older_than_days)
    logger.info(
        f"Iceberg 유지보수 시작 — "
        f"cutoff={cutoff.strftime('%Y-%m-%d %H:%M UTC')}  dry_run={dry_run}"
    )

    catalog = Iceberg.get_catalog()
    results = []

    for table_name in _TARGET_TABLES:
        try:
            table = catalog.load_table(table_name)
            result = expire_table_snapshots(table, older_than=cutoff, dry_run=dry_run)
            results.append(result)
        except Exception as e:
            logger.error(f"[{table_name}] 처리 중 오류: {e}")
            results.append({"table": table_name, "error": str(e)})

    # ── 요약 출력 ────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("유지보수 완료 요약")
    logger.info("=" * 60)
    for r in results:
        if "error" in r:
            logger.error(f"  {r['table']}: 오류 — {r['error']}")
        else:
            logger.info(
                f"  {r['table']}: "
                f"만료={r['expired_snapshots']}  orphan={r['orphan_files']}"
            )


# ==========================================
# CLI 진입점
# ==========================================

if __name__ == "__main__":
    from oliveyoung_common.logging import setup_logging

    setup_logging("iceberg-expire-snapshots")

    parser = argparse.ArgumentParser(
        description="Iceberg 스냅샷 만료 및 orphan 파일 정리"
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=30,
        help="이 일수보다 오래된 스냅샷을 만료합니다 (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 삭제 없이 대상만 출력합니다",
    )
    args = parser.parse_args()

    # 2026-06-16 이전에는 실행을 차단합니다
    earliest_run = datetime(2026, 6, 16, tzinfo=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    if now < earliest_run and not args.dry_run:
        print(
            f"[차단] 이 스크립트는 {earliest_run.strftime('%Y-%m-%d')} 이후에 실행하세요.\n"
            f"현재: {now.strftime('%Y-%m-%d')}\n"
            f"(동작 확인은 --dry-run 으로 가능합니다)"
        )
        sys.exit(1)

    run_maintenance(older_than_days=args.older_than_days, dry_run=args.dry_run)
