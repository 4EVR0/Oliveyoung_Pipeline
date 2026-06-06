"""
Iceberg Pipeline 전역 설정 파일 (EC2 전용)

사용법:
    from config.settings import S3, OliveyoungIceberg, INCIIceberg, DataPath, DuckDB
"""

import os
import duckdb
from pyiceberg.catalog import load_catalog

from oliveyoung_common import s3_paths

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Iceberg_pipeline/


# ==========================================
# AWS / S3 경로 설정
# ==========================================
class S3:
    REGION = "ap-northeast-2"
    BUCKET = s3_paths.BUCKET

    # Bronze: s3://.../oliveyoung/main_category/sub_category/run_id=YYYYMMDD_HHMMSS/part_*.json
    BRONZE_PREFIX = s3_paths.BRONZE_PREFIX
    BRONZE_GLOB   = s3_paths.BRONZE_GLOB

    # KCIA: INCI_data_silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv
    KCIA_PREFIX = s3_paths.KCIA_PREFIX
    KCIA_GLOB   = s3_paths.KCIA_GLOB

    # Silver
    SILVER_CURRENT_PATH  = s3_paths.SILVER_CURRENT_PATH
    SILVER_HISTORY_PATH  = s3_paths.SILVER_HISTORY_PATH
    SILVER_ERROR_PATH    = s3_paths.SILVER_ERROR_PATH
    CATEGORY_MASTER_PATH = s3_paths.CATEGORY_MASTER_PATH

    # Gold
    GOLD_PATH = s3_paths.GOLD_PATH

    # Iceberg 메타데이터
    ICEBERG_METADATA_PATH = s3_paths.ICEBERG_METADATA_PATH

    # 처리 결과 CSV 저장 (조회용)
    DATA_CSV_PATH = s3_paths.DATA_CSV_PATH

    # Reference Data (typo_map, garbage_keywords, custom_ingredient_dict)
    REFERENCE_TYPO_MAP_PATH               = f"s3://{s3_paths.BUCKET}/reference/typo_map/"
    REFERENCE_GARBAGE_KEYWORDS_PATH       = f"s3://{s3_paths.BUCKET}/reference/garbage_keywords/"
    REFERENCE_CUSTOM_INGREDIENT_DICT_PATH = f"s3://{s3_paths.BUCKET}/reference/custom_ingredient_dict/"


# ==========================================
# Glue Catalog / Iceberg 설정 — oliveyoung_db
# ==========================================
class OliveyoungIceberg:
    CATALOG_NAME = "glue"
    DATABASE     = "oliveyoung_db"

    SILVER_CURRENT_TABLE  = f"{DATABASE}.oliveyoung_silver_current"
    SILVER_HISTORY_TABLE  = f"{DATABASE}.oliveyoung_silver_history"
    SILVER_ERROR_TABLE    = f"{DATABASE}.oliveyoung_silver_error"
    CATEGORY_MASTER_TABLE           = f"{DATABASE}.oliveyoung_category_master"
    GOLD_INGREDIENT_FREQUENCY_TABLE = f"{DATABASE}.gold_ingredient_frequency"
    GOLD_PRODUCT_CHANGE_LOG_TABLE   = f"{DATABASE}.gold_product_change_log"
    GOLD_PRODUCT_INGREDIENTS_TABLE  = f"{DATABASE}.gold_product_ingredients"
    NEO4J_SYNC_CHECKPOINT_TABLE     = f"{DATABASE}.neo4j_sync_checkpoint"
    TYPO_MAP_TABLE                  = f"{DATABASE}.typo_map"
    GARBAGE_KEYWORDS_TABLE          = f"{DATABASE}.garbage_keywords"
    CUSTOM_INGREDIENT_DICT_TABLE    = f"{DATABASE}.custom_ingredient_dict"

    @staticmethod
    def get_catalog():
        return load_catalog(
            OliveyoungIceberg.CATALOG_NAME,
            **{
                "type":      "glue",
                "warehouse": S3.ICEBERG_METADATA_PATH,
                "s3.region": S3.REGION,
            }
        )


# ==========================================
# Glue Catalog / Iceberg 설정 — inci_db
# ==========================================
class INCIIceberg:
    CATALOG_NAME = "glue"
    DATABASE     = "inci_db"

    KCIA_BRONZE_CURRENT_TABLE   = f"{DATABASE}.kcia_bronze_current"
    KCIA_BRONZE_HISTORY_TABLE   = f"{DATABASE}.kcia_bronze_history"

    COSING_BRONZE_CURRENT_TABLE = f"{DATABASE}.cosing_bronze_current"
    COSING_BRONZE_HISTORY_TABLE = f"{DATABASE}.cosing_bronze_history"

    SILVER_MATCHED_CURRENT_TABLE  = f"{DATABASE}.silver_kcia_cosing_matched_current"
    SILVER_MATCHED_HISTORY_TABLE  = f"{DATABASE}.silver_kcia_cosing_matched_history"

    SILVER_GRAPHRAG_CURRENT_TABLE = f"{DATABASE}.silver_kcia_cosing_graphrag_current"
    SILVER_GRAPHRAG_HISTORY_TABLE = f"{DATABASE}.silver_kcia_cosing_graphrag_history"

    SILVER_FUZZY_REVIEW_TABLE     = f"{DATABASE}.silver_kcia_cosing_fuzzy_review_current"

    GOLD_INGREDIENTS_CURRENT_TABLE = f"{DATABASE}.gold_kcia_cosing_ingredients_current"
    GOLD_INGREDIENTS_HISTORY_TABLE = f"{DATABASE}.gold_kcia_cosing_ingredients_history"

    _WAREHOUSE = f"s3://{S3.BUCKET}/inci_iceberg_metadata/"

    @staticmethod
    def get_catalog():
        return load_catalog(
            INCIIceberg.CATALOG_NAME,
            **{
                "type":      "glue",
                "warehouse": INCIIceberg._WAREHOUSE,
                "s3.region": S3.REGION,
            }
        )


# ==========================================
# 데이터 파일 경로 (EC2 로컬 디스크)
# ==========================================
class DataPath:
    DATA_DIR                     = os.path.join(_BASE_DIR, "data")
    KCIA_MAPPING_JSON            = os.path.join(DATA_DIR, "kcia_mapping_dict.json")
    TYPO_MAP_JSON                = os.path.join(DATA_DIR, "typo_map.json")
    TYPO_MAP_REGEX_JSON          = os.path.join(DATA_DIR, "typo_map_regex.json")
    GARBAGE_KEYWORDS_JSON        = os.path.join(DATA_DIR, "garbage_keywords.json")
    PRODUCT_NAME_NORM_MAP_JSON   = os.path.join(DATA_DIR, "product_name_norm_map.json")
    CUSTOM_INGREDIENT_DICT_JSON  = os.path.join(DATA_DIR, "custom_ingredient_dict.json")


# ==========================================
# DuckDB 설정
# ==========================================
class DuckDB:
    @staticmethod
    def get_connection() -> duckdb.DuckDBPyConnection:
        """S3 읽기용 DuckDB 커넥션을 반환합니다. IAM Role이 EC2에 연결되어 있어야 합니다."""
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL aws;   LOAD aws;")
        con.execute("CALL load_aws_credentials();")
        con.execute(f"SET s3_region='{S3.REGION}';")
        return con

    @staticmethod
    def get_latest_bronze_files(con: duckdb.DuckDBPyConnection) -> list[str]:
        """
        sub_category별로 가장 최신 run_id에 해당하는 JSON 파일 경로 목록을 반환합니다.

        S3 구조:
            oliveyoung/{main_category}/{sub_category}/run_id={YYYYMMDD_HHMMSS}/{part_*.json}


        동작:
            1. BRONZE_GLOB으로 전체 파일 목록 조회
            2. sub_category별 max(run_id) 선택  ← 문자열 정렬로 최신값 결정
            3. 최신 run_id에 해당하는 파일 경로만 반환


        Returns:
            list[str]: 최신 run_id 파일 경로 목록

        Raises:
            RuntimeError: S3에서 파일을 찾지 못한 경우
        """
        df = con.execute(f"""
            WITH all_files AS (
                SELECT
                    file,
                    regexp_extract(file, '/([^/]+)/([^/]+)/run_id=([^/]+)/', 1) AS main_category,
                    regexp_extract(file, '/([^/]+)/([^/]+)/run_id=([^/]+)/', 2) AS sub_category,
                    regexp_extract(file, 'run_id=([^/]+)/',                  1) AS run_id
                FROM glob('{S3.BRONZE_GLOB}')
            ),
            latest_runs AS (
                SELECT sub_category, max(run_id) AS latest_run_id
                FROM all_files
                GROUP BY sub_category
            )
            SELECT f.file
            FROM all_files   f
            JOIN latest_runs l
              ON f.sub_category = l.sub_category
             AND f.run_id       = l.latest_run_id
            ORDER BY f.main_category, f.sub_category, f.file
        """).df()

        if df.empty:
            raise RuntimeError(
                f"S3에서 bronze 파일을 찾지 못했습니다.\n"
                f"패턴: {S3.BRONZE_GLOB}"
            )

        files = df["file"].tolist()
        print(f"   최신 run_id 파일 {len(files)}개 선택됨")
        return files


    @staticmethod
    def get_latest_kcia_s3_path(con: duckdb.DuckDBPyConnection) -> str:
        """
        S3에서 batch=YYYY-MM 파티션 중 가장 최신 batch의 KCIA CSV 경로를 반환합니다.

        S3 구조:
            INCI_data_silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv

        Returns:
            str: 최신 batch CSV의 S3 경로

        Raises:
            RuntimeError: S3에서 파일을 찾지 못한 경우
        """
        df = con.execute(f"""
            SELECT
                file,
                regexp_extract(file, 'batch=([^/]+)/', 1) AS batch_id
            FROM glob('{S3.KCIA_GLOB}')
            ORDER BY batch_id DESC
            LIMIT 1
        """).df()

        if df.empty:
            raise RuntimeError(
                f"S3에서 KCIA CSV 파일을 찾지 못했습니다.\n"
                f"패턴: {S3.KCIA_GLOB}"
            )

        path = df['file'].iloc[0]
        batch_id = df['batch_id'].iloc[0]
        print(f"   KCIA 최신 batch: {batch_id} ({path})")
        return path
