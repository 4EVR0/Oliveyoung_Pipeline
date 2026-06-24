"""
Silver → Gold 파이프라인 오케스트레이션 로직
"""

import logging

from config.settings import OliveyoungIceberg
from gold_pipeline.cdc import compute_change_log
from gold_pipeline.write_gold import write_gold_change_log, write_gold_ingredient_frequency
from gold_pipeline.write_gold_product_ingredients import write_gold_product_ingredients
from models.batch_metadata import BatchMetadata, create_batch_metadata

logger = logging.getLogger(__name__)


def run_pipeline():
    """Silver → Gold 파이프라인 전체를 실행합니다."""
    batch_base = create_batch_metadata("iceberg_silver_to_gold")
    batch = BatchMetadata(batch_job=batch_base.run_id, batch_date=batch_base.batch_date)

    logger.info(f"=== Silver → Gold 파이프라인 시작: batch_job={batch.batch_job} ===")

    catalog = OliveyoungIceberg.get_catalog()

    logger.info("[Step 1] CDC — silver 변경 분석 → gold_product_change_log")
    change_df = compute_change_log(catalog, batch=batch)
    write_gold_change_log(catalog, change_df)

    logger.info("[Step 2] 성분 빈도 집계 — silver_current → gold_ingredient_frequency")
    write_gold_ingredient_frequency(catalog, batch=batch)

    logger.info("[Step 3] 성분 매핑 mart — silver_current × INCI → gold_product_ingredients")
    write_gold_product_ingredients(catalog, batch.batch_job, batch.batch_date)

    logger.info(f"=== Silver → Gold 파이프라인 완료: batch_job={batch.batch_job} ===")
