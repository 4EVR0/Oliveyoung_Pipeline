"""
Gold 레이어 Iceberg write 모듈

- write_gold_ingredient_frequency : silver_current → gold_ingredient_frequency append
- write_gold_change_log           : CDC 결과 → gold_product_change_log append
"""

import logging

import duckdb
import pandas as pd
import pyarrow as pa

from config.settings import OliveyoungIceberg
from models.batch_metadata import BatchMetadata, add_batch_metadata

logger = logging.getLogger(__name__)


# ==========================================
# 공통 유틸
# ==========================================

def _build_arrow(df: pd.DataFrame, table) -> pa.Table:
    """
    DataFrame을 Iceberg 테이블 스키마 기준 Arrow Table로 변환합니다.
    silver 쪽 write_silver.py 와 동일한 패턴.
    """
    iceberg_arrow_schema = table.schema().as_arrow()
    target_cols = iceberg_arrow_schema.names

    work_df = df.copy()

    # 없는 컬럼은 None으로 채움
    for col in target_cols:
        if col not in work_df.columns:
            work_df[col] = None

    # timestamp 정규화
    for col in target_cols:
        if col in work_df.columns and "timestamp" in str(iceberg_arrow_schema.field(col).type).lower():
            work_df[col] = pd.to_datetime(work_df[col], utc=True, errors="coerce")

    # list<string> 정규화
    if "product_ingredients" in work_df.columns:
        work_df["product_ingredients"] = work_df["product_ingredients"].apply(
            lambda v: [str(x) for x in v] if isinstance(v, list) else None
        )

    # map<string, map<string, string>> 정규화
    if "review_stats" in work_df.columns:
        def _normalize_review_stats(v):
            if not isinstance(v, dict):
                return None
            return {
                str(k): {str(ik): str(iv) for ik, iv in inner.items()}
                if isinstance(inner, dict) else {}
                for k, inner in v.items()
            }
        work_df["review_stats"] = work_df["review_stats"].apply(_normalize_review_stats)

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        col = field.name
        values = work_df[col].tolist() if col in work_df.columns else [None] * len(work_df)
        arrow_dict[col] = pa.array(values, type=field.type, from_pandas=True)

    return pa.table(arrow_dict, schema=iceberg_arrow_schema)


# ==========================================
# gold_ingredient_frequency
# ==========================================

_INGREDIENT_FREQUENCY_QUERY = r"""
WITH unnested AS (
    SELECT category_id, unnest(product_ingredients) AS ingredient_name
    FROM silver_arrow
),
filtered AS (
    SELECT * FROM unnested
    WHERE length(ingredient_name) > 1
      AND ingredient_name NOT SIMILAR TO '[0-9]+'
),
by_category AS (
    SELECT category_id, ingredient_name, COUNT(*) AS usage_count
    FROM filtered
    GROUP BY category_id, ingredient_name
),
total AS (
    SELECT 'TOTAL' AS category_id, ingredient_name, COUNT(*) AS usage_count
    FROM filtered
    GROUP BY ingredient_name
),
combined AS (
    SELECT * FROM by_category
    UNION ALL
    SELECT * FROM total
),
ranked AS (
    SELECT
        category_id,
        ingredient_name,
        usage_count,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY category_id
            ORDER BY usage_count DESC, ingredient_name ASC
        ) AS INTEGER) AS rank
    FROM combined
)
SELECT category_id, ingredient_name, usage_count, rank
FROM ranked
WHERE rank <= 50
ORDER BY category_id, rank
"""


def write_gold_ingredient_frequency(catalog, batch: BatchMetadata) -> None:
    """
    silver_current 데이터를 집계하여 gold_ingredient_frequency 에 append 합니다.

    Args:
        catalog   : pyiceberg Catalog 인스턴스
        batch: 현재 배치 메타데이터
    """
    logger.info("silver_current 로드 중...")
    silver_table = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE)
    silver_arrow = silver_table.scan(selected_fields=("category_id", "product_ingredients")).to_arrow()

    con = duckdb.connect()
    con.register("silver_arrow", silver_arrow)
    gold_df: pd.DataFrame = con.execute(_INGREDIENT_FREQUENCY_QUERY).df()
    con.close()

    logger.info(f"성분 빈도 집계 완료: {len(gold_df)}건")

    add_batch_metadata(gold_df, batch)

    gold_table  = catalog.load_table(OliveyoungIceberg.GOLD_INGREDIENT_FREQUENCY_TABLE)
    arrow_table = _build_arrow(gold_df, gold_table)
    gold_table.append(arrow_table)

    logger.info(f"gold_ingredient_frequency append 완료: {len(gold_df)}건")


# ==========================================
# gold_product_change_log
# ==========================================

def write_gold_change_log(catalog, change_df: pd.DataFrame) -> None:
    """
    CDC 결과 DataFrame을 gold_product_change_log 에 append 합니다.

    Args:
        catalog   : pyiceberg Catalog 인스턴스
        change_df : cdc.compute_change_log() 반환값
    """
    if change_df is None or change_df.empty:
        logger.info("변경 레코드 없음 — gold_product_change_log write 건너뜀")
        return

    change_table = catalog.load_table(OliveyoungIceberg.GOLD_PRODUCT_CHANGE_LOG_TABLE)
    arrow_table  = _build_arrow(change_df, change_table)
    change_table.append(arrow_table)

    new_cnt     = (change_df["change_type"] == "NEW").sum()
    removed_cnt = (change_df["change_type"] == "REMOVED").sum()
    changed_cnt = (change_df["change_type"] == "CHANGED").sum()
    logger.info(
        f"gold_product_change_log append 완료: "
        f"NEW={new_cnt}건  REMOVED={removed_cnt}건  CHANGED={changed_cnt}건"
    )
