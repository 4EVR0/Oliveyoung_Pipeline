"""
Bronze → Silver 전처리 파이프라인 오케스트레이션 로직
"""

import hashlib
import os
from pathlib import Path
import sys

from config.settings import OliveyoungIceberg, INCIIceberg, DuckDB, S3
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
from src.bronze_to_silver.profiler import PipelineProfiler
from silver_pipeline.write_silver import write_to_iceberg, write_csv_to_s3


SUPPORTED_BRONZE_LOAD_FORMATS = {"json", "compacted_json", "parquet"}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_list(values: list[str]) -> str:
    return ", ".join(_sql_literal(value) for value in values)


def _bronze_source_sql(latest_files: list[str]) -> str:
    file_list_sql = _sql_list(latest_files)
    return f"SELECT * FROM read_json_auto([{file_list_sql}], ignore_errors=true)"


def _normalize_bronze_load_format(load_format: str | None = None) -> str:
    selected = (load_format or os.environ.get("BRONZE_LOAD_FORMAT") or "json").strip().lower()
    aliases = {
        "direct_json": "json",
        "json_direct": "json",
        "compact_json": "compacted_json",
        "json_compacted": "compacted_json",
    }
    selected = aliases.get(selected, selected)
    if selected not in SUPPORTED_BRONZE_LOAD_FORMATS:
        raise ValueError(
            "BRONZE_LOAD_FORMAT은 json, compacted_json, parquet 중 하나여야 합니다. "
            f"입력값: {selected}"
        )
    return selected


def bronze_file_set_id(latest_files: list[str]) -> str:
    payload = "\n".join(latest_files).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def bronze_optimized_path(latest_files: list[str], extension: str) -> str:
    file_set_id = bronze_file_set_id(latest_files)
    return f"{S3.BRONZE_OPTIMIZED_PATH}/file_set={file_set_id}/bronze_latest.{extension}"


def _ensure_local_parent(path: str) -> None:
    if "://" not in path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)


def materialize_bronze_compacted_json(con, latest_files: list[str]) -> str:
    target_path = bronze_optimized_path(latest_files, "json")
    _ensure_local_parent(target_path)
    source_sql = _bronze_source_sql(latest_files)
    con.execute(f"COPY ({source_sql}) TO {_sql_literal(target_path)} (FORMAT JSON, ARRAY false)")
    return target_path


def materialize_bronze_parquet(con, latest_files: list[str]) -> str:
    target_path = bronze_optimized_path(latest_files, "parquet")
    _ensure_local_parent(target_path)
    source_sql = _bronze_source_sql(latest_files)
    con.execute(f"COPY ({source_sql}) TO {_sql_literal(target_path)} (FORMAT PARQUET)")
    return target_path


def load_bronze_data_from_files(
    con,
    latest_files: list[str],
    load_format: str | None = None,
    materialize_optimized: bool = True,
):
    load_format = _normalize_bronze_load_format(load_format)
    metadata = {
        "load_format": load_format,
        "source_file_count": len(latest_files),
        "file_set_id": bronze_file_set_id(latest_files),
    }

    if load_format == "json":
        raw_df = con.execute(_bronze_source_sql(latest_files)).df()
        return raw_df, metadata

    if load_format == "compacted_json":
        target_path = bronze_optimized_path(latest_files, "json")
        if materialize_optimized:
            target_path = materialize_bronze_compacted_json(con, latest_files)
            metadata["materialized"] = True
        else:
            metadata["materialized"] = False
        metadata["optimized_path"] = target_path
        raw_df = con.execute(
            f"SELECT * FROM read_json_auto({_sql_literal(target_path)}, ignore_errors=true)"
        ).df()
        return raw_df, metadata

    target_path = bronze_optimized_path(latest_files, "parquet")
    if materialize_optimized:
        target_path = materialize_bronze_parquet(con, latest_files)
        metadata["materialized"] = True
    else:
        metadata["materialized"] = False
    metadata["optimized_path"] = target_path
    raw_df = con.execute(f"SELECT * FROM read_parquet({_sql_literal(target_path)})").df()
    return raw_df, metadata


def load_bronze_data(con):
    """
    DuckDB 커넥션으로 최신 run_id bronze 파일을 로드합니다.

    Returns:
        tuple[pd.DataFrame, dict]: bronze raw 데이터와 로드 메타데이터
    """
    print("2. 최신 run_id bronze 파일 탐색...")
    try:
        latest_files = DuckDB.get_latest_bronze_files(con)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    load_format = _normalize_bronze_load_format()
    print(f"3. Bronze 데이터 로드 ({len(latest_files)}개 파일, format={load_format})...")
    try:
        raw_df, metadata = load_bronze_data_from_files(con, latest_files, load_format=load_format)
    except Exception as e:
        print(f"[ERROR] Bronze 로드 실패(format={load_format}): {e}")
        sys.exit(1)
    print(f"   로드 완료: {len(raw_df)}건")
    if metadata.get("optimized_path"):
        print(f"   optimized_path: {metadata['optimized_path']}")
    print()

    return raw_df, metadata


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
    profiler = PipelineProfiler()

    succeeded = False
    try:
        with profiler.step("total") as total_step:
            print("1. DuckDB 커넥션 설정...")
            with profiler.step("duckdb_connection"):
                con = DuckDB.get_connection()

            with profiler.step("bronze_load") as step:
                raw_df, bronze_metadata = load_bronze_data(con)
                step.set_rows_out(len(raw_df))
                step.set_metadata(**bronze_metadata)

            with profiler.step("dictionary_load"):
                dicts = load_dictionaries()

            print("9. 전처리 파이프라인 실행...")
            with profiler.step("process_pipeline", rows_in=len(raw_df)) as step:
                silver_df, error_df = process_pipeline(
                    df                     = raw_df,
                    ac_automaton           = dicts.ac_automaton,
                    typo_list              = dicts.typo_list,
                    typo_regex_list        = dicts.typo_regex_list,
                    garbage_config         = dicts.garbage_config,
                    product_name_norm_list = dicts.product_name_norm_list,
                    profiler               = profiler,
                )
                step.set_rows_out(len(silver_df) + len(error_df))
                step.set_metadata(silver_rows=len(silver_df), error_rows=len(error_df))
            print(f"   정상: {len(silver_df)}건 / 에러: {len(error_df)}건\n")

            print("10. Iceberg write...")
            with profiler.step("iceberg_write", rows_in=len(silver_df) + len(error_df)) as step:
                write_to_iceberg(silver_df, error_df, profiler=profiler)
                step.set_rows_out(len(silver_df) + len(error_df))

            print("\n11. CSV 저장 (s3 data_csv/)...")
            with profiler.step("csv_s3_write", rows_in=len(silver_df) + len(error_df)) as step:
                write_csv_to_s3(silver_df, error_df, profiler=profiler)
                step.set_rows_out(len(silver_df) + len(error_df))

            total_step.set_rows_out(len(silver_df) + len(error_df))
            total_step.set_metadata(
                bronze_rows=len(raw_df),
                silver_rows=len(silver_df),
                error_rows=len(error_df),
            )
            succeeded = True
    finally:
        profiler.print_summary()

    if succeeded:
        print("\n=== 완료 ===")
