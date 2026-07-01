"""
Gold 레이어 gold_product_ingredients write 모듈

silver_current의 unique 성분 × inci_db.gold_kcia_cosing_ingredients_current → gold_product_ingredients overwrite
조인 키: silver.product_ingredients[i] ↔ inci.kor_name
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging
from datetime import datetime

import duckdb
import pandas as pd
from pyiceberg.expressions import AlwaysTrue

from config.settings import OliveyoungIceberg, INCIIceberg
from gold_pipeline.write_gold import _build_arrow
from oliveyoung_common.logging import log_dq
from oliveyoung_common.dq_metrics import write_dq_metrics

logger = logging.getLogger(__name__)

_PRODUCT_INGREDIENTS_QUERY = """
WITH all_ingredients AS (
    SELECT product_id, UNNEST(product_ingredients) AS ingredient_name
    FROM silver_arrow
    WHERE product_ingredients IS NOT NULL
),
unique_ingredients AS (
    SELECT ingredient_name, COUNT(DISTINCT product_id) AS usage_count
    FROM all_ingredients
    GROUP BY ingredient_name
)
SELECT
    u.ingredient_name,
    i.inci_name,
    i.kor_name,
    i.eng_name,
    i.cosing_functions,
    i.status,
    i.cosmetic_restriction,
    i.other_restrictions,
    u.usage_count
FROM unique_ingredients u
LEFT JOIN inci_arrow i ON u.ingredient_name = i.kor_name
ORDER BY u.usage_count DESC, u.ingredient_name
"""


def write_gold_product_ingredients(
    catalog,
    batch_job: str,
    batch_date: datetime,
) -> None:
    """
    silver_current의 unique 성분을 inci_db.gold_kcia_cosing_ingredients_current와 조인하여
    gold_product_ingredients 를 overwrite합니다.

    Args:
        catalog   : pyiceberg Catalog 인스턴스
        batch_job : 배치 식별자 (예: "20260501_120000")
        batch_date: 배치 기준 시각 (UTC datetime)
    """
    logger.info("silver_current 로드 중...")
    silver_table = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE)
    silver_arrow = silver_table.scan(
        selected_fields=("product_id", "product_ingredients")
    ).to_arrow()

    logger.info("inci_db.gold_kcia_cosing_ingredients_current 로드 중...")
    inci_catalog = INCIIceberg.get_catalog()
    inci_table   = inci_catalog.load_table(INCIIceberg.GOLD_INGREDIENTS_CURRENT_TABLE)
    inci_arrow = inci_table.scan().to_arrow()

    con = duckdb.connect()
    con.register("silver_arrow", silver_arrow)
    con.register("inci_arrow",   inci_arrow)

    result_df: pd.DataFrame = con.execute(_PRODUCT_INGREDIENTS_QUERY).df()
    con.close()

    total      = len(result_df)
    matched    = result_df["inci_name"].notna().sum()
    match_rate = matched / total if total else 0
    logger.info(f"unique 성분: {total}건 | INCI 매핑 성공: {matched}건 ({match_rate:.1%})")

    # 정합성 메트릭 — 로그(Loki) + 테이블(dq_metrics) 이중 기록, 같은 수치
    metrics = dict(
        ingredients_unique=int(total),
        ingredients_matched=int(matched),
        ingredients_unmatched=int(total - matched),
        match_rate=round(float(match_rate), 4),
    )
    log_dq(logger, stage="silver_to_gold", batch_job=batch_job, **metrics)
    # 테이블 적재 실패가 파이프라인을 깨지 않도록 비치명적 처리
    try:
        write_dq_metrics(
            catalog,
            stage="silver_to_gold",
            batch_job=batch_job,
            target_table=OliveyoungIceberg.GOLD_PRODUCT_INGREDIENTS_TABLE,
            **metrics,
        )
    except Exception as e:
        logger.warning(f"dq_metrics 적재 실패(무시): {e}")

    result_df["batch_job"]  = batch_job
    result_df["batch_date"] = pd.to_datetime(batch_date, utc=True)

    gold_table  = catalog.load_table(OliveyoungIceberg.GOLD_PRODUCT_INGREDIENTS_TABLE)
    arrow_table = _build_arrow(result_df, gold_table)
    gold_table.overwrite(arrow_table, overwrite_filter=AlwaysTrue())

    logger.info(f"gold_product_ingredients overwrite 완료: {total}건")
