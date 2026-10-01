"""
Silver / Silver Error 테이블 Iceberg write + CSV 저장 모듈
- 스키마의 기준은 PyArrow가 아니라 Iceberg 테이블이다.
- Arrow 스키마는 항상 table.schema().as_arrow() 에서 가져온다.
- schema evolution 이후에는 반드시 테이블을 reload 한다.
"""

from concurrent.futures import ThreadPoolExecutor
import io
import os
from typing import Any, Callable, TYPE_CHECKING

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.types import StringType, TimestamptzType

from config.settings import S3, OliveyoungIceberg
from oliveyoung_common.batch import build_run_id

if TYPE_CHECKING:
    from src.bronze_to_silver.profiler import PipelineProfiler


# ==========================================
# 공통 유틸
# ==========================================

def _now_ts() -> str:
    """UTC 기준 run_id 문자열 반환. 예) oliveyoung_silver_20260318_153042"""
    return build_run_id("oliveyoung_silver")


def _upload_csv(df: pd.DataFrame, s3_key: str) -> None:
    """DataFrame을 CSV로 직렬화하여 S3에 업로드합니다."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    boto3.client("s3", region_name=S3.REGION).put_object(
        Bucket=S3.BUCKET,
        Key=s3_key,
        Body=buf.getvalue().encode("utf-8-sig"),
        ContentType="text/csv",
    )


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    """
    pandas Series를 UTC timezone-aware datetime으로 변환.
    """
    return pd.to_datetime(series, utc=True, errors="coerce")


def _normalize_list_of_strings(value: Any) -> list[str] | None:
    """
    Iceberg list<string> 컬럼용 값 정규화.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]


def _normalize_review_stats(value: Any) -> dict[str, dict[str, str]] | None:
    """
    review_stats: map<string, map<string, string>>
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if not value:
        return {}

    result: dict[str, dict[str, str]] = {}
    for outer_key, inner_val in dict(value).items():
        outer_key = str(outer_key)

        if inner_val is None:
            result[outer_key] = {}
            continue

        inner_dict: dict[str, str] = {}
        for inner_key, inner_item in dict(inner_val).items():
            inner_dict[str(inner_key)] = "" if inner_item is None else str(inner_item)

        result[outer_key] = inner_dict

    return result


def _ensure_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼이 없습니다: {missing}")


def _add_missing_columns_as_none(df: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
    """
    Iceberg 테이블에는 있는데 DataFrame에 없는 컬럼은 None으로 채운다.
    """
    out = df.copy()
    for col in target_columns:
        if col not in out.columns:
            out[col] = None
    return out


# ==========================================
# Schema Evolution
# ==========================================

def _evolve_schema(table) -> bool:
    """
    테이블에 batch_job, batch_date 컬럼이 없을 때만 추가합니다.

    Returns:
        bool: schema evolution commit이 발생했으면 True
    """
    existing = {f.name for f in table.schema().fields}
    missing_batch_job = "batch_job" not in existing
    missing_batch_date = "batch_date" not in existing

    if not missing_batch_job and not missing_batch_date:
        return False

    with table.update_schema() as update:
        if missing_batch_job:
            update.add_column("batch_job", StringType())
        if missing_batch_date:
            update.add_column("batch_date", TimestamptzType())

    # update_schema() commit 이후에는 호출 측에서 table을 reload 해서
    # 최신 schema/table metadata를 다시 잡는 것이 안전하다.
    return True


def _load_and_evolve_table(
    catalog,
    identifier: str,
    profiler: "PipelineProfiler" = None,
    stage_name: str | None = None,
):
    """
    테이블 로드 → 필요한 경우에만 schema evolution → 최신 테이블 반환
    """

    def prepare_table():
        table = catalog.load_table(identifier)
        before = {f.name for f in table.schema().fields}

        schema_evolved = _evolve_schema(table)
        if schema_evolved:
            table = catalog.load_table(identifier)

        after = {f.name for f in table.schema().fields}

        if before != after:
            print(f"   schema evolve 완료: {identifier}")
            print(f"   before: {sorted(before)}")
            print(f"   after : {sorted(after)}")

        return table, schema_evolved, len(after)

    if profiler and stage_name:
        with profiler.step(stage_name) as step:
            table, schema_evolved, field_count = prepare_table()
            step.set_metadata(
                table=identifier,
                schema_evolved=schema_evolved,
                field_count=field_count,
            )
            return table

    table, _, _ = prepare_table()
    return table


# ==========================================
# Iceberg schema 기반 Arrow 변환
# ==========================================

def _build_arrow_table_for_silver(df: pd.DataFrame, table) -> pa.Table:
    """
    silver DataFrame을 'Iceberg 테이블 스키마 기준' PyArrow Table로 변환.
    고정 PyArrow 스키마를 직접 정의하지 않는다.
    """
    required = ["product_id"]
    _ensure_required_columns(df, required)

    # Iceberg 스키마 기준 컬럼 목록
    iceberg_arrow_schema = table.schema().as_arrow()
    target_columns = iceberg_arrow_schema.names

    # DataFrame에 없는 컬럼은 None 추가
    work_df = _add_missing_columns_as_none(df, target_columns)

    # 필요한 정규화
    if "product_ingredients" in work_df.columns:
        work_df["product_ingredients"] = work_df["product_ingredients"].apply(_normalize_list_of_strings)

    if "review_stats" in work_df.columns:
        work_df["review_stats"] = work_df["review_stats"].apply(_normalize_review_stats)

    for ts_col in ["crawled_at", "batch_date"]:
        if ts_col in work_df.columns:
            work_df[ts_col] = _normalize_timestamp_series(work_df[ts_col])

    # Iceberg 테이블 컬럼 순서대로만 구성
    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        col = field.name

        if col not in work_df.columns:
            values = [None] * len(work_df)
        else:
            values = work_df[col].tolist()

        arrow_dict[col] = pa.array(values, type=field.type)

    pa_table = pa.table(arrow_dict, schema=iceberg_arrow_schema)

    # required(not null) 검증
    required_not_null = {f.name for f in table.schema().fields if getattr(f, "required", False)}
    for col in required_not_null:
        if col in pa_table.column_names and pa_table.column(col).null_count > 0:
            raise ValueError(f"필수 컬럼 '{col}' 에 null 값이 있습니다.")

    return pa_table


def _build_arrow_table_for_error(df: pd.DataFrame, table) -> pa.Table:
    """
    error DataFrame을 'Iceberg 테이블 스키마 기준' PyArrow Table로 변환.
    """
    required = ["product_id"]
    _ensure_required_columns(df, required)

    iceberg_arrow_schema = table.schema().as_arrow()
    target_columns = iceberg_arrow_schema.names
    work_df = _add_missing_columns_as_none(df, target_columns)

    for ts_col in ["crawled_at", "batch_date"]:
        if ts_col in work_df.columns:
            work_df[ts_col] = _normalize_timestamp_series(work_df[ts_col])

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        col = field.name
        if col not in work_df.columns:
            arrow_dict[col] = pa.array([None] * len(work_df), type=field.type)
        else:
            values = [
                None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
                for v in work_df[col]
            ]
            if pa.types.is_string(field.type):
                values = [str(v) if v is not None else None for v in values]
            arrow_dict[col] = pa.array(values, type=field.type)

    pa_table = pa.table(arrow_dict, schema=iceberg_arrow_schema)

    required_not_null = {f.name for f in table.schema().fields if getattr(f, "required", False)}
    for col in required_not_null:
        if col in pa_table.column_names and pa_table.column(col).null_count > 0:
            raise ValueError(f"필수 컬럼 '{col}' 에 null 값이 있습니다.")

    return pa_table


# ==========================================
# Iceberg write
# ==========================================

def _parallel_iceberg_writes_enabled() -> bool:
    return os.environ.get("ICEBERG_PARALLEL_WRITES", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _write_current_table(
    silver_df: pd.DataFrame,
    profiler: "PipelineProfiler" = None,
) -> str:
    catalog = OliveyoungIceberg.get_catalog()
    current_table = _load_and_evolve_table(
        catalog,
        OliveyoungIceberg.SILVER_CURRENT_TABLE,
        profiler=profiler,
        stage_name="iceberg_current_load_table",
    )
    if profiler:
        with profiler.step("pandas_to_arrow_current", rows_in=len(silver_df)) as step:
            current_arrow = _build_arrow_table_for_silver(silver_df, current_table)
            step.set_rows_out(current_arrow.num_rows)
        with profiler.step("iceberg_current_write", rows_in=current_arrow.num_rows) as step:
            current_table.overwrite(current_arrow)
            step.set_rows_out(current_arrow.num_rows)
            step.set_metadata(table=OliveyoungIceberg.SILVER_CURRENT_TABLE, mode="overwrite")
    else:
        current_arrow = _build_arrow_table_for_silver(silver_df, current_table)
        current_table.overwrite(current_arrow)

    return f"   Iceberg overwrite 완료: {OliveyoungIceberg.SILVER_CURRENT_TABLE} ({len(silver_df)}건)"


def _write_history_table(
    silver_df: pd.DataFrame,
    profiler: "PipelineProfiler" = None,
) -> str:
    catalog = OliveyoungIceberg.get_catalog()
    history_table = _load_and_evolve_table(
        catalog,
        OliveyoungIceberg.SILVER_HISTORY_TABLE,
        profiler=profiler,
        stage_name="iceberg_history_load_table",
    )
    if profiler:
        with profiler.step("pandas_to_arrow_history", rows_in=len(silver_df)) as step:
            history_arrow = _build_arrow_table_for_silver(silver_df, history_table)
            step.set_rows_out(history_arrow.num_rows)
        with profiler.step("iceberg_history_write", rows_in=history_arrow.num_rows) as step:
            history_table.append(history_arrow)
            step.set_rows_out(history_arrow.num_rows)
            step.set_metadata(table=OliveyoungIceberg.SILVER_HISTORY_TABLE, mode="append")
    else:
        history_arrow = _build_arrow_table_for_silver(silver_df, history_table)
        history_table.append(history_arrow)

    return f"   Iceberg append 완료:    {OliveyoungIceberg.SILVER_HISTORY_TABLE} ({len(silver_df)}건)"


def _write_error_table(
    error_df: pd.DataFrame,
    profiler: "PipelineProfiler" = None,
) -> str:
    catalog = OliveyoungIceberg.get_catalog()
    error_table = _load_and_evolve_table(
        catalog,
        OliveyoungIceberg.SILVER_ERROR_TABLE,
        profiler=profiler,
        stage_name="iceberg_error_load_table",
    )
    if profiler:
        with profiler.step("pandas_to_arrow_error", rows_in=len(error_df)) as step:
            error_arrow = _build_arrow_table_for_error(error_df, error_table)
            step.set_rows_out(error_arrow.num_rows)
        with profiler.step("iceberg_error_write", rows_in=error_arrow.num_rows) as step:
            error_table.overwrite(error_arrow)
            step.set_rows_out(error_arrow.num_rows)
            step.set_metadata(table=OliveyoungIceberg.SILVER_ERROR_TABLE, mode="overwrite")
    else:
        error_arrow = _build_arrow_table_for_error(error_df, error_table)
        error_table.overwrite(error_arrow)

    return f"   Iceberg overwrite 완료: {OliveyoungIceberg.SILVER_ERROR_TABLE} ({len(error_df)}건)"


def _run_iceberg_write_tasks(tasks: list[tuple[str, Callable[[], str]]], parallel: bool) -> None:
    if not tasks:
        return

    if not parallel or len(tasks) == 1:
        for _, task in tasks:
            print(task())
        return

    max_workers = min(3, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="iceberg-write") as executor:
        futures = [(name, executor.submit(task)) for name, task in tasks]
        for _, future in futures:
            print(future.result())


def write_to_iceberg(
    silver_df: pd.DataFrame,
    error_df: pd.DataFrame,
    profiler: "PipelineProfiler" = None,
) -> None:
    """
    silver / error DataFrame을 Iceberg 테이블에 기록합니다.

    silver:
        - current (overwrite): 항상 최신 배치 결과만 유지
        - history (append):    배치 누적 이력 보관

    error:
        - error/raw (overwrite): 최신 에러 결과 유지
    """
    tasks: list[tuple[str, Callable[[], str]]] = []

    if not silver_df.empty:
        tasks.append(("current", lambda: _write_current_table(silver_df, profiler=profiler)))
        tasks.append(("history", lambda: _write_history_table(silver_df, profiler=profiler)))
    else:
        print("   silver 데이터 없음 — Iceberg write 건너뜀")

    if not error_df.empty:
        tasks.append(("error", lambda: _write_error_table(error_df, profiler=profiler)))
    else:
        print("   error 데이터 없음 — Iceberg write 건너뜀")

    parallel = _parallel_iceberg_writes_enabled()
    if parallel and len(tasks) > 1:
        print(f"   Iceberg 병렬 write 시작: {len(tasks)}개 테이블")

    _run_iceberg_write_tasks(tasks, parallel=parallel)


# ==========================================
# CSV 저장 (S3 data_csv/)
# ==========================================

def write_csv_to_s3(
    silver_df: pd.DataFrame,
    error_df: pd.DataFrame,
    profiler: "PipelineProfiler" = None,
) -> None:
    """
    silver / error DataFrame을 S3 data_csv/ 폴더에 CSV로 저장합니다.

    저장 경로:
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_{YYYYMMDD_HHMMSS}.csv
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_error_{YYYYMMDD_HHMMSS}.csv
    """
    ts = _now_ts()
    prefix = S3.DATA_CSV_PATH.removeprefix(f"s3://{S3.BUCKET}/")

    if not silver_df.empty:
        if profiler:
            with profiler.step("csv_silver_prepare", rows_in=len(silver_df)) as step:
                csv_df = silver_df.copy()

                if "product_ingredients" in csv_df.columns:
                    csv_df["product_ingredients"] = csv_df["product_ingredients"].apply(
                        lambda v: "|".join(v) if isinstance(v, list) else v
                    )

                if "review_stats" in csv_df.columns:
                    csv_df["review_stats"] = csv_df["review_stats"].apply(
                        lambda v: str(v) if v is not None else None
                    )
                step.set_rows_out(len(csv_df))
        else:
            csv_df = silver_df.copy()

            if "product_ingredients" in csv_df.columns:
                csv_df["product_ingredients"] = csv_df["product_ingredients"].apply(
                    lambda v: "|".join(v) if isinstance(v, list) else v
                )

            if "review_stats" in csv_df.columns:
                csv_df["review_stats"] = csv_df["review_stats"].apply(
                    lambda v: str(v) if v is not None else None
                )

        silver_table_name = OliveyoungIceberg.SILVER_CURRENT_TABLE.split(".")[-1]
        key = f"{prefix}{silver_table_name}_{ts}.csv"
        if profiler:
            with profiler.step("csv_silver_upload", rows_in=len(csv_df)) as step:
                _upload_csv(csv_df, key)
                step.set_rows_out(len(csv_df))
        else:
            _upload_csv(csv_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")

    if not error_df.empty:
        error_table_name = OliveyoungIceberg.SILVER_ERROR_TABLE.split(".")[-1]
        key = f"{prefix}{error_table_name}_{ts}.csv"
        if profiler:
            with profiler.step("csv_error_upload", rows_in=len(error_df)) as step:
                _upload_csv(error_df, key)
                step.set_rows_out(len(error_df))
        else:
            _upload_csv(error_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")
