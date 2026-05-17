"""
Iceberg에 gold_product_ingredients 테이블 생성
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging

from oliveyoung_common.logging import setup_logging
from pyiceberg.catalog.glue import GlueCatalog

from gold_pipeline.schemas import GOLD_PRODUCT_INGREDIENTS_SCHEMA

setup_logging("iceberg-create-gold-product-ingredients")
logger = logging.getLogger(__name__)


def create_gold_product_ingredients_table():
    catalog = GlueCatalog("oliveyoung_catalog", **{
        "s3.region": "ap-northeast-2",
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "warehouse": "s3://oliveyoung-crawl-data/olive_young_gold/",
    })

    table_identifier = "oliveyoung_db.gold_product_ingredients"

    try:
        catalog.drop_table(table_identifier)
        logger.info(f"기존 테이블 삭제: {table_identifier}")
    except Exception:
        pass  # 테이블이 없으면 무시

    try:
        catalog.create_table(
            identifier=table_identifier,
            schema=GOLD_PRODUCT_INGREDIENTS_SCHEMA,
            location="s3://oliveyoung-crawl-data/olive_young_gold/gold_product_ingredients",
        )
        logger.info(f"테이블 생성 완료: {table_identifier}")
    except Exception as e:
        logger.error(f"테이블 생성 중 오류: {e}")


if __name__ == "__main__":
    create_gold_product_ingredients_table()
