"""
silver_current → Neo4j 노드/관계 CSV writer (oliveyoung 도메인).

이 모듈은 oliveyoung 도메인에서 어떤 silver 컬럼을 어떤 Neo4j 라벨/속성으로
보낼지를 선언하고, 실제 write 함수를 정의한다. CSV 직렬화/업로드는
oliveyoung_common.neo4j_csv 가 담당한다.

향후 노드/관계 추가 시:
    - {NODE}_COLUMNS 또는 RelationshipSpec 정의 추가
    - write_{node|rel}_csv() 함수 추가
    - src/silver_to_neo4j_csv/pipeline.py 에서 호출
"""

from __future__ import annotations

import logging

import pandas as pd

from oliveyoung_common.batch import create_batch_metadata
from oliveyoung_common.logging import job_unit, log_process_summary
from oliveyoung_common.neo4j_csv import (
    CsvColumn,
    build_node_csv,
    upload_csv_to_s3,
)
from oliveyoung_common.s3_paths import neo4j_csv_prefix

from config.settings import S3, OliveyoungIceberg


logger = logging.getLogger(__name__)

PIPELINE_NAME = "oliveyoung"


# ==========================================
# Product 노드
# ==========================================

PRODUCT_COLUMNS: list[CsvColumn] = [
    CsvColumn(name="product_id", is_id=True, id_space="Product"),
    CsvColumn(name="product_name"),
    CsvColumn(name="brand", source="product_brand"),
    CsvColumn(name="category"),
]


def write_product_node_csv() -> None:
    """silver_current → Product 노드 CSV → S3 (gold/neo4j/oliveyoung/nodes/Product/{run_id}/)."""
    batch = create_batch_metadata(f"{PIPELINE_NAME}_neo4j")
    run_id = batch.run_id

    with job_unit(logger, job="silver_to_neo4j_csv.product", run_id=run_id):
        catalog = OliveyoungIceberg.get_catalog()
        table = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE)
        df: pd.DataFrame = (
            table.scan(
                selected_fields=("product_id", "product_name", "product_brand", "category"),
            ).to_pandas()
        )

        df = df.dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"])

        if df.empty:
            logger.warning("silver_current에 Product 데이터 없음 — 업로드 skip")
            return

        header_csv, data_csv = build_node_csv(df, PRODUCT_COLUMNS)

        prefix = neo4j_csv_prefix(
            pipeline=PIPELINE_NAME,
            kind="nodes",
            name="Product",
            run_id=run_id,
        )
        upload_csv_to_s3(header_csv, S3.BUCKET, f"{prefix}/header.csv", S3.REGION)
        upload_csv_to_s3(data_csv,   S3.BUCKET, f"{prefix}/part-00000.csv", S3.REGION)

        log_process_summary(
            logger,
            job="silver_to_neo4j_csv.product",
            run_id=run_id,
            upserted_nodes=len(df),
        )
        logger.info(f"Product 노드 {len(df)}건 업로드: s3://{S3.BUCKET}/{prefix}/")
