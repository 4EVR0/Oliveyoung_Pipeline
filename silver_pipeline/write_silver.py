"""
Silver / Silver Error 테이블 Iceberg write + CSV 저장 모듈
- 스키마의 기준은 PyArrow가 아니라 Iceberg 테이블이다.
- Arrow 스키마는 항상 table.schema().as_arrow() 에서 가져온다.
- schema evolution 이후에는 반드시 테이블을 reload 한다.
"""

import io
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.expressions import And, EqualTo, StartsWith
from pyiceberg.types import StringType, TimestamptzType

from config.settings import S3, OliveyoungIceberg
from oliveyoung_common.batch import build_run_id

# 정상 실행 history 행의 batch_job 접두어(create_batch_metadata("bronze_to_silver").run_id).
# 백필 행은 backfill_<source_run_id>라 이 접두어로 정상 교체 범위에서 빠진다.
NORMAL_BATCH_JOB_PREFIX = "bronze_to_silver_"


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

def _evolve_schema(table, with_source_run_id: bool = False) -> None:
    """
    테이블에 batch_job, batch_date, goods_no(+ silver면 source_run_id) 컬럼이 없으면 추가합니다.
    이미 존재하면 아무것도 하지 않습니다. 추가된 컬럼의 기존 행은 null.
    """
    existing = {f.name for f in table.schema().fields}

    with table.update_schema() as update:
        if "batch_job" not in existing:
            update.add_column("batch_job", StringType())
        if "batch_date" not in existing:
            update.add_column("batch_date", TimestamptzType())
        if "goods_no" not in existing:
            update.add_column("goods_no", StringType())
        # 행 출처 크롤 run(입력 선택 결과) — silver current·history만. 2026-10-09 추가, 이전 행 null
        if with_source_run_id and "source_run_id" not in existing:
            update.add_column("source_run_id", StringType())

    # 참고:
    # update_schema() commit 이후에는 호출 측에서 table을 reload 해서
    # 최신 schema/table metadata를 다시 잡는 것이 안전하다.


_SOURCE_RUN_ID_TABLES = {OliveyoungIceberg.SILVER_CURRENT_TABLE, OliveyoungIceberg.SILVER_HISTORY_TABLE}


def _load_and_evolve_table(catalog, identifier: str):
    """
    테이블 로드 → 필요한 schema evolution 수행 → 최신 테이블 reload 반환
    """
    table = catalog.load_table(identifier)
    before = {f.name for f in table.schema().fields}

    _evolve_schema(table, with_source_run_id=identifier in _SOURCE_RUN_ID_TABLES)

    # evolution 여부와 상관없이 reload 해서 최신 metadata 사용
    table = catalog.load_table(identifier)
    after = {f.name for f in table.schema().fields}

    if before != after:
        print(f"   schema evolve 완료: {identifier}")
        print(f"   before: {sorted(before)}")
        print(f"   after : {sorted(after)}")

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

def replace_history(catalog, silver_df: pd.DataFrame, batch_date: str,
                    batch_job: str, scope: str) -> int:
    """silver_history에서 같은 batch_date의 기존 행을 교체한다(재실행 멱등). 지운 행 수를 돌려준다.

    scope="normal"  : 같은 날짜의 정상 실행 행(batch_job 접두어) — 백필 행은 보존
    scope="backfill": 같은 날짜·같은 백필 키(batch_job) 행
    지울 행이 없으면 append(빈 delete 스냅샷·경고 방지). CDC NEW는 history 최근 두 스냅샷을
    비교하는데, 재실행 때 삭제 스냅샷이 '이 배치를 지운 상태'라 첫 실행과 같은 결과가 나온다.
    """
    if scope not in ("normal", "backfill"):
        raise ValueError(f"scope는 normal|backfill: {scope!r}")
    table = _load_and_evolve_table(catalog, OliveyoungIceberg.SILVER_HISTORY_TABLE)
    arrow = (
        _build_arrow_table_for_silver(silver_df, table)
        if not silver_df.empty else pa.Table.from_batches([], schema=table.schema().as_arrow())
    )
    day = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    job_cond = (StartsWith("batch_job", NORMAL_BATCH_JOB_PREFIX) if scope == "normal"
                else EqualTo("batch_job", batch_job))
    selector = And(EqualTo("batch_date", day), job_cond)
    replaced = table.scan(row_filter=selector, selected_fields=("batch_job",)).to_arrow().num_rows
    if replaced:
        table.overwrite(arrow, overwrite_filter=selector)
    elif arrow.num_rows:
        table.append(arrow)
    return replaced


def write_to_iceberg(
    silver_df: pd.DataFrame,
    error_df: pd.DataFrame,
    batch_date: str,
    batch_job: str,
) -> int:
    """
    silver / error DataFrame을 Iceberg 테이블에 기록합니다. history에서 교체한 행 수를 돌려줍니다.

    순서: history → error → current(마지막).
    CDC는 silver_current 최근 두 스냅샷으로 REMOVED/CHANGED를 본다. current를 먼저 쓰고 뒤에서
    실패해 재실행하면 같은 내용 스냅샷이 하나 더 생겨 다음 CDC가 이번 배치 변화를 놓치므로,
    current는 앞 단계가 다 성공한 뒤 한 번만 쓴다.

    - history (같은 batch_date 정상 행 교체): 재실행해도 한 벌
    - error/raw (overwrite): 최신 배치 에러만. 0건이어도 비워서 이전 배치 에러가 남지 않게
    - current (overwrite): 최신 배치 결과만
    """
    catalog = OliveyoungIceberg.get_catalog()

    replaced = 0
    if not silver_df.empty:
        replaced = replace_history(catalog, silver_df, batch_date, batch_job, scope="normal")
        print(f"   Iceberg history 기록 완료: {OliveyoungIceberg.SILVER_HISTORY_TABLE} "
              f"({len(silver_df)}건, 교체 {replaced}건)")
    else:
        print("   silver 데이터 없음 — history/current write 건너뜀")

    error_table = _load_and_evolve_table(catalog, OliveyoungIceberg.SILVER_ERROR_TABLE)
    error_arrow = (
        _build_arrow_table_for_error(error_df, error_table)
        if not error_df.empty else pa.Table.from_batches([], schema=error_table.schema().as_arrow())
    )
    error_table.overwrite(error_arrow)
    print(f"   Iceberg overwrite 완료: {OliveyoungIceberg.SILVER_ERROR_TABLE} ({len(error_df)}건)")

    if not silver_df.empty:
        current_table = _load_and_evolve_table(catalog, OliveyoungIceberg.SILVER_CURRENT_TABLE)
        current_arrow = _build_arrow_table_for_silver(silver_df, current_table)
        current_table.overwrite(current_arrow)
        print(f"   Iceberg overwrite 완료: {OliveyoungIceberg.SILVER_CURRENT_TABLE} ({len(silver_df)}건)")

    return replaced


# ==========================================
# CSV 저장 (S3 data_csv/)
# ==========================================

def write_csv_to_s3(silver_df: pd.DataFrame, error_df: pd.DataFrame) -> None:
    """
    silver / error DataFrame을 S3 data_csv/ 폴더에 CSV로 저장합니다.

    저장 경로:
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_{YYYYMMDD_HHMMSS}.csv
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_error_{YYYYMMDD_HHMMSS}.csv
    """
    ts = _now_ts()
    prefix = S3.DATA_CSV_PATH.removeprefix(f"s3://{S3.BUCKET}/")

    if not silver_df.empty:
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
        _upload_csv(csv_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")

    if not error_df.empty:
        error_table_name = OliveyoungIceberg.SILVER_ERROR_TABLE.split(".")[-1]
        key = f"{prefix}{error_table_name}_{ts}.csv"
        _upload_csv(error_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")
