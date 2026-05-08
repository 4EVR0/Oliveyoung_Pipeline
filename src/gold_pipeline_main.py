"""
Gold 파이프라인 오케스트레이션 진입점

실행:
    python src/gold_pipeline_main.py

실행 순서:
    1. CDC  : silver_history 최신 2 스냅샷 비교 → gold_product_change_log append
    2. Mart : silver_current 집계             → gold_ingredient_frequency append
"""

import logging

from oliveyoung_common.logging import setup_logging

from config.settings import OliveyoungIceberg
from gold_pipeline.cdc import compute_change_log
from gold_pipeline.write_gold import write_gold_change_log, write_gold_ingredient_frequency
from models.batch_metadata import create_batch_metadata

setup_logging("iceberg-gold")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_gold_pipeline() -> None:
    batch = create_batch_metadata("gold_pipeline")

    logger.info(f"=== Gold Pipeline 시작: batch_job={batch.batch_job} ===")

    catalog = OliveyoungIceberg.get_catalog()

    # ----------------------------------------
    # Step 1. CDC
    # ----------------------------------------
    logger.info("[Step 1] CDC — silver_history 변경 분석")
    try:
        change_df = compute_change_log(catalog, batch=batch)
        write_gold_change_log(catalog, change_df)
    except Exception as e:
        logger.error(f"CDC 실패: {e}", exc_info=True)
        raise

    # ----------------------------------------
    # Step 2. 성분 빈도 집계
    # ----------------------------------------
    logger.info("[Step 2] 성분 빈도 집계 — silver_current → gold_ingredient_frequency")
    try:
        write_gold_ingredient_frequency(catalog, batch=batch)
    except Exception as e:
        logger.error(f"성분 빈도 집계 실패: {e}", exc_info=True)
        raise

    logger.info(f"=== Gold Pipeline 완료: batch_job={batch.batch_job} ===")


if __name__ == "__main__":
    run_gold_pipeline()
