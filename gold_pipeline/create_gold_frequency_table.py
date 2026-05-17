'''
IceBerg에 gold_ingredient_frequency 테이블 생성
(콘솔에서 생성 및 수정 시 동기화가 잘 안되는 문제가 있어서 스크립트로 생성)
'''

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, LongType, NestedField
import logging

from oliveyoung_common.logging import setup_logging
from oliveyoung_common import s3_paths

setup_logging("iceberg-create-gold-frequency-table")
logger = logging.getLogger(__name__)

def create_gold_table():
    # 1. 카탈로그 연결
    catalog = GlueCatalog("oliveyoung_catalog", **{
        "s3.region": "ap-northeast-2",
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "warehouse": s3_paths.GOLD_PATH,
    })

    # 2. 스키마 정의 (TOP 50 집계 목적에 최적화)
    schema = Schema(
        NestedField(field_id=1, name="category_id", field_type=StringType(), required=False),
        NestedField(field_id=2, name="ingredient_name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="usage_count", field_type=LongType(), required=False),
        NestedField(field_id=4, name="rank", field_type=IntegerType(), required=False)
    )

    table_identifier = "oliveyoung_db.gold_ingredient_frequency"

    try:
        # 기존에 잘못 생성되었을 수 있는 테이블 삭제 (필요시 사용)
        # catalog.drop_table(table_identifier)
        
        # 3. 테이블 생성
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            location=f"{s3_paths.GOLD_PATH}gold_ingredient_frequency",
        )
        logger.info(f"성공적으로 테이블을 생성했습니다: {table_identifier}")
        
    except Exception as e:
        logger.error(f"테이블 생성 중 오류 발생: {e}")

if __name__ == "__main__":
    create_gold_table()
