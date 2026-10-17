"""
Bronze → Silver 전처리 파이프라인 오케스트레이션 로직
"""

import logging
import os
import re
import sys
from datetime import datetime, timezone

from pyiceberg.expressions import And, EqualTo

from config.settings import OliveyoungIceberg, INCIIceberg, DuckDB
from models.batch_metadata import BatchMetadata, check_batch_date_required, create_batch_metadata
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
from src.bronze_gate.main import attach_source_run_id, run_bronze_gate

logger = logging.getLogger(__name__)


def load_bronze_data(con):
    """
    입력 선택·품질 게이트가 카테고리마다 고른 bronze 파일을 DuckDB로 로드합니다.

    Returns:
        (raw_df, max_run_id): bronze raw 데이터(행마다 source_run_id) + 선택된 run_id 최댓값(단독 실행 batch_date 파생용)
    """
    print("2. 입력 선택 + bronze 파일 탐색...")
    # 카테고리마다 쓸 run을 고르고 판정(보류면 여기서 종료, 쓰기 없음). 고른 목록 그대로 로드한다.
    gate = run_bronze_gate(_resolve_batch_date)
    latest_files = gate.files
    run_ids = [m.group(1) for f in latest_files if (m := re.search(r"run_id=([^/]+)/", f))]
    max_run_id = max(run_ids) if run_ids else ""

    print(f"3. Bronze 데이터 로드 ({len(latest_files)}개 파일)...")
    try:
        file_list_sql = ", ".join(f"'{f}'" for f in latest_files)
        raw_df = con.execute(
            f"SELECT * FROM read_json_auto([{file_list_sql}], ignore_errors=true, filename=true)"
        ).df()
    except Exception as e:
        print(f"[ERROR] JSON 로드 실패: {e}")
        sys.exit(1)
    raw_df = attach_source_run_id(raw_df)
    print(f"   로드 완료: {len(raw_df)}건\n")

    return raw_df, max_run_id


def _resolve_batch_date(source_run_id: str) -> str:
    """단계 관통 논리 배치 날짜 — Airflow가 BATCH_DATE로 주입, 단독 실행 시 소스 bronze run_id에서 파생."""
    return os.environ.get("BATCH_DATE") or (
        batch_date_from_run_id(source_run_id) if source_run_id
        else batch_date_from_run_id(build_run_id("bronze_to_silver"))
    )


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


def _verify_silver(batch_date: str, batch_job: str, silver_count: int) -> None:
    """쓰기 후 재조회 — history의 이번 실행 행 수와 current 행 수가 정제 결과와 같아야 한다."""
    catalog = OliveyoungIceberg.get_catalog()
    day = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    history = catalog.load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
    history_count = history.scan(
        row_filter=And(EqualTo("batch_date", day), EqualTo("batch_job", batch_job)),
        selected_fields=("batch_job",),
    ).to_arrow().num_rows
    current_count = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE).scan(
        selected_fields=("product_id",)
    ).to_arrow().num_rows
    if history_count != silver_count or current_count != silver_count:
        raise RuntimeError(
            f"Silver 적재 검증 실패: 정제 {silver_count}건, history {history_count}건, current {current_count}건"
        )


def run_pipeline():
    """Bronze → Silver 전처리 파이프라인 전체를 실행합니다.

    재실행 멱등: 같은 batch_date의 이전 정상 실행 결과(history·DQ)를 교체한다.
    쓰기 순서는 Silver(history → error → current) → 재조회 검증 → DQ·리포트 → CSV.
    current 이후에 실패할 수 있는 지점은 검증뿐이다(DQ·CSV는 비치명). current를 쓴 뒤
    실패해 재실행하면 같은 내용 current 스냅샷이 하나 더 생겨 CDC의 최근 두 스냅샷 비교가
    이번 배치 변화를 놓칠 수 있다 — 남는 위험(근본 해결은 CDC의 배치 경계 스냅샷 선택).
    """
    print("=== Bronze → Silver 전처리 시작 ===\n")
    check_batch_date_required()

    print("1. DuckDB 커넥션 설정...")
    con = DuckDB.get_connection()

    raw_df, max_run_id = load_bronze_data(con)
    dicts  = load_dictionaries()

    batch_date = _resolve_batch_date(max_run_id)
    # 이 실행의 식별자 하나를 history 행(batch_job)과 DQ(run_id)에 같이 쓴다.
    # cleaner는 행에 batch.batch_job을 찍으므로 batch_job 자리에 실행 ID를 넣는다(silver_to_gold와 같은 방식).
    # create_batch_metadata("bronze_to_silver")를 그대로 넘기면 행에는 작업 이름만 찍혀 실행을 구분할 수 없다.
    base = create_batch_metadata("bronze_to_silver")
    batch_job = base.run_id
    batch = BatchMetadata(batch_job=batch_job, batch_date=base.batch_date)

    print("9. 전처리 파이프라인 실행...")
    silver_df, error_df = process_pipeline(
        df                     = raw_df,
        ac_automaton           = dicts.ac_automaton,
        typo_list              = dicts.typo_list,
        typo_regex_list        = dicts.typo_regex_list,
        garbage_config         = dicts.garbage_config,
        product_name_norm_list = dicts.product_name_norm_list,
        batch                  = batch,
        batch_date             = batch_date,
    )
    print(f"   정상: {len(silver_df)}건 / 에러: {len(error_df)}건\n")

    # 정제 결과 0건은 정상 결과가 아니라 장애 신호(사전 손상 등). 아무것도 쓰기 전에 멈춰
    # 이전 history/current를 그대로 두고 DAG 실패로 알린다 — 0건을 쓰면 재실행이 한 벌로
    # 수렴하지 않고, current가 이전 날짜 그대로인 채 silver_to_gold가 진행된다.
    if silver_df.empty:
        raise RuntimeError(f"정제 결과 0건(에러 {len(error_df)}건, bronze {len(raw_df)}건) — 쓰기 전에 중단")

    print("10. Iceberg write...")
    replaced = write_to_iceberg(silver_df, error_df, batch_date, batch_job)
    if replaced:
        logger.info(f"같은 batch_date({batch_date})의 이전 정상 실행 history {replaced}건을 교체(재실행)")
    _verify_silver(batch_date, batch_job, len(silver_df))

    # 정합성 메트릭 — 적재 보존(bronze 로드) + 전처리(정상/에러)
    # 에러율은 출력 안에서 닫힌 비율(dedup 영향 없음) — 점수판/임계값용
    processed = len(silver_df) + len(error_df)
    error_rate = round(len(error_df) / processed, 4) if processed else 0.0
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
    summary_ok = False
    try:
        write_dq_metrics(
            OliveyoungIceberg.get_catalog(),
            stage="bronze_to_silver",
            batch_date=batch_date,
            run_id=batch_job,
            target_table=OliveyoungIceberg.SILVER_CURRENT_TABLE,
            report_webhook=os.environ.get("DISCORD_DQ_WEBHOOK_URL"),  # 완료 리포트(옵트인)
            replace="other_runs",   # 같은 batch_date 이전 실행 기록 교체(재실행이면 리포트에 표시)
            **metrics,
        )
        summary_ok = True
    except Exception as e:
        logger.warning(f"dq_metrics 적재 실패(무시): {e}")

    # 정합성 메트릭(2) — error_type별 레코드 수. 저장만(Discord 리포트 미발송, 비치명).
    # UNMAPPED_RESIDUAL은 매칭 유무와 무관히 잔여 성분이 있으면 남음(Silver 적재 여부 섞임) →
    # '상품 수'가 아닌 '레코드 수'로 집계. run 식별은 위 silver_error가 마커.
    # 요약이 실패했으면 쓰지 않는다 — 유형별만 쓰면 교체가 이전 실행의 완전한 기록을 지우고
    # err_*만 남긴다. 요약만 남는 경우는 dq_api가 '유형별 자료 없음'으로 표시한다.
    if summary_ok and not error_df.empty and "error_type" in error_df.columns:
        # 집계·기록 전체를 try 안에 두어 'DQ 계측은 비치명' 계약을 끝까지 지킴.
        # error_type null/누락은 value_counts가 기본 제외 → fillna로 UNCLASSIFIED에 담아
        # 불변식 sum(err_counts) == len(error_df) 보장(dq_api '집계 불완전' 판정의 근거).
        try:
            etypes = error_df["error_type"].fillna("UNCLASSIFIED")
            err_counts = {f"err_{t}": int(c) for t, c in etypes.value_counts().items()}
            if err_counts.get("err_UNCLASSIFIED"):
                logger.warning(f"error_type 누락 {err_counts['err_UNCLASSIFIED']}건 → UNCLASSIFIED로 집계")
            write_dq_metrics(
                OliveyoungIceberg.get_catalog(),
                stage="bronze_to_silver",
                batch_date=batch_date,
                run_id=batch_job,
                target_table=OliveyoungIceberg.SILVER_ERROR_TABLE,
                # report_webhook 미전달 → 요약만 리포트, 유형별은 저장만
                replace="other_runs",   # 같은 실행의 요약 행은 run_id가 같아 지우지 않음
                **err_counts,
            )
        except Exception as e:
            logger.warning(f"error_type dq_metrics 적재 실패(무시): {e}")

    # CSV는 실행 시각 파일명이라 재실행마다 새 파일 — 원본 데이터가 아니므로 멱등 대상 아님.
    # 비치명: CSV 실패로 태스크가 실패하면 재실행이 current를 다시 써 CDC 입력을 흐트러뜨린다.
    print("\n11. CSV 저장 (s3 data_csv/)...")
    try:
        write_csv_to_s3(silver_df, error_df)
    except Exception as e:
        logger.warning(f"CSV 저장 실패(무시): {e}")

    print("\n=== 완료 ===")