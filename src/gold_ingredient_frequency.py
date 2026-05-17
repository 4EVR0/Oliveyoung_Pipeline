import duckdb
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
import boto3
from io import StringIO
import logging

from oliveyoung_common.batch import build_run_id
from oliveyoung_common.logging import job_unit
from oliveyoung_common.logging import setup_logging
from oliveyoung_common import s3_paths

setup_logging("iceberg-gold-ingredient-frequency")
logger = logging.getLogger(__name__)


def _run_gold_ingredient_frequency(run_id: str):
    # 1. 카탈로그 연결 (실버 데이터 로드 및 골드 적재 공용)
    # warehouse 경로는 테이블 생성 시 설정한 경로와 일치해야 합니다.
    catalog = GlueCatalog("oliveyoung_catalog", **{
        "s3.region": "ap-northeast-2",
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "warehouse": s3_paths.GOLD_PATH,
    })

    DATABASE_NAME = "oliveyoung_db"
    TABLE_NAME = "gold_ingredient_frequency"
    GOLD_CSV_PATH = "olive_young_gold/gold_ingredient_frequency/dsta_csv/"

    try:
        # [핵심] 2. Silver 테이블에서 최신 스냅샷 데이터만 Arrow로 가져오기
        # S3 경로(*.parquet)를 직접 읽으면 삭제된 파일까지 중복 계산되므로, 
        # Iceberg 메타데이터가 보증하는 '현재 유효한 데이터'만 스캔합니다.
        logger.info("Silver 테이블에서 유효한 스냅샷 데이터를 로드 중...")
        silver_table = catalog.load_table(f"{DATABASE_NAME}.oliveyoung_silver")
        silver_arrow = silver_table.scan().to_arrow() 

        # 3. DuckDB 설정 및 데이터 가공
        con = duckdb.connect()
        
        # [수정] 1. filtered_data 단계에서 숫자 및 한 글자 노이즈 제거
        # [수정] 2. ROW_NUMBER() 결과를 INTEGER로 캐스팅하여 스키마 일치
        gold_query = rf"""
        WITH unnested_data AS (
            SELECT 
                category_id, 
                unnest(product_ingredients) AS ingredient_name
            FROM silver_arrow -- 메모리 상의 Arrow Table 참조
        ),
        filtered_data AS (
            -- 성분명이 1글자 이하('1', '2' 등)거나 숫자만 있는 경우 필터링
            SELECT * FROM unnested_data
            WHERE length(ingredient_name) > 1 
              AND ingredient_name NOT SIMILAR TO '[0-9]+'
        ),
        category_counts AS (
            SELECT 
                category_id, 
                ingredient_name, 
                COUNT(*) AS usage_count
            FROM filtered_data
            GROUP BY category_id, ingredient_name
        ),
        total_counts AS (
            SELECT 
                'TOTAL' AS category_id, 
                ingredient_name, 
                COUNT(*) AS usage_count
            FROM filtered_data
            GROUP BY ingredient_name
        ),
        combined_counts AS (
            SELECT * FROM category_counts
            UNION ALL
            SELECT * FROM total_counts
        ),
        ranked_ingredients AS (
            SELECT 
                category_id,
                ingredient_name,
                usage_count,
                -- Iceberg 스키마(int) 호환을 위해 CAST
                CAST(ROW_NUMBER() OVER (
                    PARTITION BY category_id 
                    ORDER BY usage_count DESC, ingredient_name ASC
                ) AS INTEGER) AS rank
            FROM combined_counts
        )
        SELECT 
            category_id,
            ingredient_name,
            usage_count,
            rank
        FROM ranked_ingredients
        WHERE rank <= 50
        ORDER BY category_id, rank
        """

        gold_df = con.execute(gold_query).df()
        logger.info(f"집계 완료: {len(gold_df)}개 레코드 생성됨 (노이즈 제거 완료).")

        # 4. Gold 테이블에 Overwrite 적재
        # 이미 생성된 골드 테이블을 로드하여 덮어쓰기 수행
        gold_table = catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")
        arrow_table = pa.Table.from_pandas(gold_df, preserve_index=False)
        
        gold_table.overwrite(arrow_table)
        logger.info(f"Gold 테이블 적재 성공: {DATABASE_NAME}.{TABLE_NAME}")

        # 5. CSV S3 저장 추가
        s3 = boto3.client('s3')
        csv_file_name = f"{TABLE_NAME}_{run_id}.csv"
        
        csv_buffer = StringIO()
        gold_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        s3.put_object(
            Bucket=s3_paths.BUCKET,
            Key=f"{GOLD_CSV_PATH}{csv_file_name}",
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Gold 집계 결과 CSV 백업 완료: s3://{s3_paths.BUCKET}/{GOLD_CSV_PATH}{csv_file_name}")

    except Exception as e:
        logger.error(f"오류 발생: {e}")
        raise


def run_gold_ingredient_frequency():
    run_id = build_run_id("gold_ingredient_frequency")
    with job_unit(logger, job="gold_ingredient_frequency", run_id=run_id):
        _run_gold_ingredient_frequency(run_id=run_id)


if __name__ == "__main__":
    run_gold_ingredient_frequency()
