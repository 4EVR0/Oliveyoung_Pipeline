"""
Gold 레이어 Iceberg 테이블 초기화 스크립트

실행:
    python gold_pipeline/create_gold_tables.py

동작:
    - gold_ingredient_frequency  : batch_date 파티션, append 방식
    - gold_product_change_log    : batch_date 파티션, CDC 변경 로그
"""

import logging

from oliveyoung_common.logging import setup_logging
from config.settings import OliveyoungIceberg, S3
from gold_pipeline.schemas import (
    GOLD_INGREDIENT_FREQUENCY_PARTITION,
    GOLD_INGREDIENT_FREQUENCY_SCHEMA,
    GOLD_INGREDIENT_FREQUENCY_SORT,
    GOLD_PRODUCT_CHANGE_LOG_PARTITION,
    GOLD_PRODUCT_CHANGE_LOG_SCHEMA,
    GOLD_PRODUCT_CHANGE_LOG_SORT,
    NEO4J_SYNC_CHECKPOINT_SCHEMA,
    NEO4J_SYNC_CHECKPOINT_SORT,
)

setup_logging("iceberg-create-gold-tables")
logger = logging.getLogger(__name__)


def _create_table(catalog, identifier: str, schema, partition, sort_order, location: str) -> None:
    try:
        catalog.create_table(
            identifier=identifier,
            schema=schema,
            partition_spec=partition,
            sort_order=sort_order,
            location=location,
        )
        logger.info(f"테이블 생성 완료: {identifier}")
    except Exception as e:
        if "AlreadyExistsException" in type(e).__name__ or "already exists" in str(e).lower():
            logger.info(f"이미 존재함 (건너뜀): {identifier}")
        else:
            logger.error(f"테이블 생성 실패: {identifier} — {e}")
            raise


def create_gold_ingredient_frequency(catalog) -> None:
    _create_table(
        catalog=catalog,
        identifier=OliveyoungIceberg.GOLD_INGREDIENT_FREQUENCY_TABLE,
        schema=GOLD_INGREDIENT_FREQUENCY_SCHEMA,
        partition=GOLD_INGREDIENT_FREQUENCY_PARTITION,
        sort_order=GOLD_INGREDIENT_FREQUENCY_SORT,
        location=f"{S3.GOLD_PATH}gold_ingredient_frequency",
    )


def create_gold_product_change_log(catalog) -> None:
    _create_table(
        catalog=catalog,
        identifier=OliveyoungIceberg.GOLD_PRODUCT_CHANGE_LOG_TABLE,
        schema=GOLD_PRODUCT_CHANGE_LOG_SCHEMA,
        partition=GOLD_PRODUCT_CHANGE_LOG_PARTITION,
        sort_order=GOLD_PRODUCT_CHANGE_LOG_SORT,
        location=f"{S3.GOLD_PATH}gold_product_change_log",
    )


def create_neo4j_sync_checkpoint(catalog) -> None:
    _create_table(
        catalog=catalog,
        identifier=OliveyoungIceberg.NEO4J_SYNC_CHECKPOINT_TABLE,
        schema=NEO4J_SYNC_CHECKPOINT_SCHEMA,
        partition=None,
        sort_order=NEO4J_SYNC_CHECKPOINT_SORT,
        location=f"{S3.GOLD_PATH}neo4j_sync_checkpoint",
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gold 테이블 초기화")
    parser.add_argument(
        "table",
        choices=["ingredient_frequency", "product_change_log", "neo4j_sync_checkpoint", "all"],
        help="생성할 테이블 선택 (all: 전체 생성)",
    )
    args = parser.parse_args()

    catalog = OliveyoungIceberg.get_catalog()

    if args.table in ("ingredient_frequency", "all"):
        create_gold_ingredient_frequency(catalog)

    if args.table in ("product_change_log", "all"):
        create_gold_product_change_log(catalog)

    if args.table in ("neo4j_sync_checkpoint", "all"):
        create_neo4j_sync_checkpoint(catalog)
