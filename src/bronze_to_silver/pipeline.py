"""
Bronze → Silver 전처리 파이프라인 오케스트레이션 로직
"""

import logging
import os
import re
import sys

from config.settings import OliveyoungIceberg, INCIIceberg, DuckDB
from models.pipeline_models import Dictionaries
from src.bronze_to_silver.ac_builder import (
    generate_kcia_mapping_dict,
    load_custom_ingredient_dict_from_iceberg,
    apply_custom_ingredient_dict,
    load_typo_maps_from_iceberg,
    load_product_name_norms_from_iceberg,
    load_garbage_config_from_iceberg,
    build_ahocorasick,
)
from src.bronze_to_silver.cleaner import process_pipeline
from silver_pipeline.write_silver import write_to_iceberg, write_csv_to_s3
from oliveyoung_common.logging import log_dq
from oliveyoung_common.dq_metrics import write_dq_metrics
from oliveyoung_common.batch import build_run_id, batch_date_from_run_id

logger = logging.getLogger(__name__)


def load_bronze_data(con):
    """
    DuckDB 커넥션으로 최신 run_id bronze 파일을 로드합니다.

    Returns:
        (raw_df, source_run_id): bronze raw 데이터 + 소스 run_id(max, 단독 실행 batch_date 파생용)
    """
    print("2. 최신 run_id bronze 파일 탐색...")
    try:
        latest_files = DuckDB.get_latest_bronze_files(con)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    print(f"3. Bronze 데이터 로드 ({len(latest_files)}개 파일)...")
    try:
        file_list_sql = ", ".join(f"'{f}'" for f in latest_files)
        raw_df = con.execute(
            f"SELECT * FROM read_json_auto([{file_list_sql}], ignore_errors=true)"
        ).df()
    except Exception as e:
        print(f"[ERROR] JSON 로드 실패: {e}")
        sys.exit(1)
    print(f"   로드 완료: {len(raw_df)}건\n")

    # 로드한 파일 경로에서 소스 run_id(max) 추출 — 단독 실행 시 batch_date 파생용
    run_ids = [m.group(1) for f in latest_files if (m := re.search(r"run_id=([^/]+)/", f))]
    source_run_id = max(run_ids) if run_ids else ""

    return raw_df, source_run_id


def load_dictionaries() -> Dictionaries:
    """
    KCIA 사전, 유의어/오타 사전, garbage 설정, Aho-Corasick 오토마타를 준비합니다.
    """
    catalog      = OliveyoungIceberg.get_catalog()
    inci_catalog = INCIIceberg.get_catalog()

    print("4. KCIA 성분 사전 준비...")
    kcia_dict = generate_kcia_mapping_dict(inci_catalog)
    print(f"   KCIA: {len(kcia_dict)}개 키워드 로드됨")
    custom_entries = load_custom_ingredient_dict_from_iceberg(catalog)
    kcia_dict = apply_custom_ingredient_dict(kcia_dict, custom_entries)
    print(f"   커스텀 적용 후 총 {len(kcia_dict)}개 키워드\n")

    print("5. 유의어/오타 사전 로드...")
    typo_list, typo_regex_list = load_typo_maps_from_iceberg(catalog)

    print("\n6. 제품명 정규화 규칙 로드...")
    product_name_norm_list = load_product_name_norms_from_iceberg(catalog)

    print("\n7. garbage 키워드 설정 로드...")
    garbage_config = load_garbage_config_from_iceberg(catalog)

    print("\n8. Aho-Corasick 빌드...")
    ac_automaton = build_ahocorasick(kcia_dict)
    print("   빌드 완료\n")

    return Dictionaries(
        ac_automaton           = ac_automaton,
        typo_list              = typo_list,
        typo_regex_list        = typo_regex_list,
        garbage_config         = garbage_config,
        product_name_norm_list = product_name_norm_list,
    )


def run_pipeline():
    """Bronze → Silver 전처리 파이프라인 전체를 실행합니다."""
    print("=== Bronze → Silver 전처리 시작 ===\n")

    print("1. DuckDB 커넥션 설정...")
    con = DuckDB.get_connection()

    raw_df, source_run_id = load_bronze_data(con)
    dicts  = load_dictionaries()

    # 단계 관통 논리 배치 날짜 — Airflow가 BATCH_DATE로 주입, 단독 실행 시 소스 bronze run_id에서 파생
    batch_date = os.environ.get("BATCH_DATE") or (
        batch_date_from_run_id(source_run_id) if source_run_id
        else batch_date_from_run_id(build_run_id("bronze_to_silver"))
    )

    print("9. 전처리 파이프라인 실행...")
    silver_df, error_df = process_pipeline(
        df                     = raw_df,
        ac_automaton           = dicts.ac_automaton,
        typo_list              = dicts.typo_list,
        typo_regex_list        = dicts.typo_regex_list,
        garbage_config         = dicts.garbage_config,
        product_name_norm_list = dicts.product_name_norm_list,
        batch_date             = batch_date,
    )
    print(f"   정상: {len(silver_df)}건 / 에러: {len(error_df)}건\n")

    # 정합성 메트릭 — 적재 보존(bronze 로드) + 전처리(정상/에러)
    # 에러율은 출력 안에서 닫힌 비율(dedup 영향 없음) — 점수판/임계값용
    processed = len(silver_df) + len(error_df)
    error_rate = round(len(error_df) / processed, 4) if processed else 0.0
    batch_job = build_run_id("bronze_to_silver")   # 이 run의 유니크 식별(run_id 컬럼용)
    metrics = dict(
        bronze_loaded=len(raw_df),
        silver_ok=len(silver_df),
        silver_error=len(error_df),
        error_rate=error_rate,
    )
    # 로그(Loki) + 테이블(dq_metrics) 이중 기록, 같은 수치
    log_dq(logger, stage="bronze_to_silver", batch_job=batch_job, **metrics)
    # 테이블 적재 실패가 파이프라인을 깨지 않도록 비치명적 처리
    # silver 행·dq_metrics 모두 같은 batch_date로 찍혀 배치-정밀 드릴다운 가능
    try:
        write_dq_metrics(
            OliveyoungIceberg.get_catalog(),
            stage="bronze_to_silver",
            batch_date=batch_date,
            run_id=batch_job,
            target_table=OliveyoungIceberg.SILVER_CURRENT_TABLE,
            **metrics,
        )
    except Exception as e:
        logger.warning(f"dq_metrics 적재 실패(무시): {e}")

    print("10. Iceberg write...")
    write_to_iceberg(silver_df, error_df)

    print("\n11. CSV 저장 (s3 data_csv/)...")
    write_csv_to_s3(silver_df, error_df)

    print("\n=== 완료 ===")