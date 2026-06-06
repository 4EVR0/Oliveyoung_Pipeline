"""
CDC (Change Data Capture) 모듈 — Iceberg 스냅샷 비교 방식

전체 테이블 스캔 없이 Iceberg 스냅샷 메타데이터를 활용해 변경분을 추출합니다.

흐름:
    NEW     : silver_history의 마지막 2 스냅샷 사이에 추가된 delta 파일만 읽기
              → append된 레코드 = 이번 배치에 신규 등장한 product
    REMOVED : silver_current의 이전 스냅샷 vs 현재 스냅샷 product_id 집합 비교
              → silver_current는 overwrite라 항상 현재 배치 크기만큼만 읽힘
    CHANGED : 양쪽 스냅샷에 모두 존재하는 product 중
              product_ingredients / rating / review_count / review_stats 가 바뀐 것

반환:
    pd.DataFrame | None — 변경 레코드가 없으면 None
"""

import logging

import pandas as pd

from config.settings import OliveyoungIceberg as Iceberg
from models.batch_metadata import BatchMetadata, add_batch_metadata

logger = logging.getLogger(__name__)

_SELECTED_FIELDS = (
    "product_id",
    "category_id",
    "product_name",
    "product_brand",
    "product_ingredients",
    "rating",
    "review_count",
    "review_stats",
    "batch_date",
)

_CHANGE_TRACK_FIELDS = ("product_ingredients", "rating", "review_count", "review_stats")


# ==========================================
# 스냅샷 유틸
# ==========================================

def _latest_two_snapshot_ids(table) -> tuple[int, int] | None:
    """
    테이블 히스토리에서 최신 2개 snapshot_id를 (prev, curr) 순으로 반환합니다.
    스냅샷이 2개 미만이면 None 반환.
    """
    history = sorted(table.history(), key=lambda h: h.timestamp_ms)

    if len(history) < 2:
        logger.warning(
            f"{table.name()} 스냅샷이 {len(history)}개뿐입니다. "
            "CDC를 실행하려면 최소 2개의 스냅샷이 필요합니다."
        )
        return None

    prev_snapshot_id = history[-2].snapshot_id
    curr_snapshot_id = history[-1].snapshot_id
    logger.info(
        f"{table.name()} 스냅샷 비교 — "
        f"prev={prev_snapshot_id}  curr={curr_snapshot_id}"
    )
    return prev_snapshot_id, curr_snapshot_id


# ==========================================
# NEW: silver_history 증분 스캔
# ==========================================

def _get_new_products(catalog) -> pd.DataFrame:
    """
    silver_history의 마지막 두 스냅샷 사이에 추가된 delta 파일만 읽어
    이번 배치에 신규 등장한 product를 반환합니다.

    pyiceberg incremental scan은 from_snapshot_id ~ to_snapshot_id 구간에
    추가된 데이터 파일만 읽으므로 전체 history 스캔이 발생하지 않습니다.
    """
    table = catalog.load_table(Iceberg.SILVER_HISTORY_TABLE)

    result = _latest_two_snapshot_ids(table)
    if result is None:
        return pd.DataFrame()

    prev_snapshot_id, curr_snapshot_id = result

    delta_arrow = table.scan(
        from_snapshot_id=prev_snapshot_id,
        to_snapshot_id=curr_snapshot_id,
        selected_fields=_SELECTED_FIELDS,
    ).to_arrow()

    new_df = delta_arrow.to_pandas()
    new_df["change_type"] = "NEW"
    logger.info(f"NEW 후보: {len(new_df)}건 (delta 파일에서 읽음)")
    return new_df


# ==========================================
# REMOVED / CHANGED: silver_current 스냅샷 비교
# ==========================================

def _get_removed_and_changed_products(catalog) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    silver_current의 이전/현재 스냅샷을 한 번만 로드해
    REMOVED(사라진)와 CHANGED(변경된) product를 함께 반환합니다.

    silver_current는 배치마다 overwrite되므로 항상 현재 배치 크기만큼만
    읽히고, 전체 history 누적 부담이 없습니다.

    CHANGED 판정 기준: product_ingredients / rating / review_count / review_stats
    """
    table = catalog.load_table(Iceberg.SILVER_CURRENT_TABLE)

    result = _latest_two_snapshot_ids(table)
    if result is None:
        return pd.DataFrame(), pd.DataFrame()

    prev_snapshot_id, curr_snapshot_id = result

    prev_df = (
        table.scan(snapshot_id=prev_snapshot_id, selected_fields=_SELECTED_FIELDS)
        .to_arrow()
        .to_pandas()
    )
    curr_df = (
        table.scan(snapshot_id=curr_snapshot_id, selected_fields=_SELECTED_FIELDS)
        .to_arrow()
        .to_pandas()
    )

    prev_ids = set(prev_df["product_id"].dropna())
    curr_ids = set(curr_df["product_id"].dropna())

    # REMOVED
    removed_ids = prev_ids - curr_ids
    if removed_ids:
        removed_df = prev_df[prev_df["product_id"].isin(removed_ids)].copy()
        removed_df["change_type"] = "REMOVED"
        logger.info(f"REMOVED: {len(removed_df)}건")
    else:
        removed_df = pd.DataFrame()
        logger.info("REMOVED: 0건")

    # CHANGED: 양쪽에 모두 존재하는 product 중 추적 필드가 달라진 것
    common_ids = prev_ids & curr_ids
    changed_df = pd.DataFrame()
    if common_ids:
        prev_common = (
            prev_df[prev_df["product_id"].isin(common_ids)].set_index("product_id")
        )
        curr_common = (
            curr_df[curr_df["product_id"].isin(common_ids)].set_index("product_id")
        )
        merged = prev_common.join(curr_common, lsuffix="_prev", rsuffix="_curr")

        change_mask = pd.Series(False, index=merged.index)
        for field in _CHANGE_TRACK_FIELDS:
            prev_col, curr_col = f"{field}_prev", f"{field}_curr"
            if prev_col in merged.columns and curr_col in merged.columns:
                change_mask |= (
                    merged[prev_col].apply(str) != merged[curr_col].apply(str)
                )

        changed_ids = set(merged[change_mask].index)
        if changed_ids:
            changed_df = curr_df[curr_df["product_id"].isin(changed_ids)].copy()
            changed_df["change_type"] = "CHANGED"
            logger.info(f"CHANGED: {len(changed_df)}건")
        else:
            logger.info("CHANGED: 0건")

    return removed_df, changed_df


# ==========================================
# 진입점
# ==========================================

def compute_change_log(catalog, batch: BatchMetadata) -> pd.DataFrame | None:
    """
    silver_history / silver_current Iceberg 스냅샷을 비교해
    변경 레코드 DataFrame을 반환합니다.

    Args:
        catalog  : pyiceberg Catalog 인스턴스
        batch: 현재 배치 메타데이터

    Returns:
        변경 레코드 DataFrame 또는 변경 없으면 None
    """
    new_df                   = _get_new_products(catalog)
    removed_df, changed_df   = _get_removed_and_changed_products(catalog)

    if new_df.empty and removed_df.empty and changed_df.empty:
        logger.info("변경 레코드 없음 — CDC 건너뜀")
        return None

    change_df = pd.concat([new_df, removed_df, changed_df], ignore_index=True)
    add_batch_metadata(change_df, batch)

    output_cols = [
        "batch_date",
        "product_id",
        "category_id",
        "change_type",
        "product_name",
        "product_brand",
        "product_ingredients",
        "rating",
        "review_count",
        "review_stats",
        "batch_job",
    ]
    change_df = change_df[output_cols].reset_index(drop=True)

    new_cnt     = (change_df["change_type"] == "NEW").sum()
    removed_cnt = (change_df["change_type"] == "REMOVED").sum()
    changed_cnt = (change_df["change_type"] == "CHANGED").sum()
    logger.info(f"CDC 완료 — NEW={new_cnt}건  REMOVED={removed_cnt}건  CHANGED={changed_cnt}건")
    return change_df
