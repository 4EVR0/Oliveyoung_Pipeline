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
import json
import os
from pathlib import Path

import duckdb
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
from gold_pipeline.fragrance_evidence import label_evidence


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
    CsvColumn(name="goods_no"),  # 올리브영 상품번호(raw 통과)
    CsvColumn(name="fragrance_evidence"),
]


def attach_fragrance_evidence(df: pd.DataFrame, claims: dict | None = None) -> pd.DataFrame:
    """Carry raw-label provenance and optional human-reviewed manufacturer claims."""
    df = df.copy()
    values = []
    for _, row in df.iterrows():
        evidence = label_evidence(row["product_id"], row.get("product_ingredients_raw"),
                                  row.get("product_url"), row.get("crawled_at"))
        claim = (claims or {}).get(str(row["product_id"]))
        if claim:
            evidence["manufacturer_claim"] = claim
        values.append(json.dumps(evidence, ensure_ascii=False))
    df["fragrance_evidence"] = values
    return df


def write_product_node_csv() -> None:
    """silver_current → Product 노드 CSV → S3 (gold/neo4j/oliveyoung/nodes/Product/{run_id}/)."""
    batch = create_batch_metadata(f"{PIPELINE_NAME}_neo4j")
    run_id = batch.run_id

    with job_unit(logger, job="silver_to_neo4j_csv.product", run_id=run_id):
        catalog = OliveyoungIceberg.get_catalog()
        table = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE)
        df: pd.DataFrame = (
            table.scan(
                selected_fields=("product_id", "product_name", "product_brand", "category", "goods_no",
                                 "product_ingredients_raw", "product_url", "crawled_at"),
            ).to_pandas()
        )

        df = df.dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"])

        if df.empty:
            logger.warning("silver_current에 Product 데이터 없음 — 업로드 skip")
            return

        claims_path = os.environ.get("FRAGRANCE_CLAIMS_PATH")
        claims = json.loads(Path(claims_path).read_text(encoding="utf-8")) if claims_path else {}
        if not isinstance(claims, dict):
            raise ValueError("FRAGRANCE_CLAIMS_PATH must contain a product_id → reviewed claim object")
        df = attach_fragrance_evidence(df, claims)
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


# ==========================================
# CONTAINS 관계 (Product → Ingredient)
# ==========================================

_CONTAINS_QUERY = """
SELECT DISTINCT s.product_id, g.inci_name
FROM (
    SELECT product_id, UNNEST(product_ingredients) AS ingredient_name
    FROM silver_arrow
    WHERE product_id IS NOT NULL AND product_ingredients IS NOT NULL
) s
INNER JOIN gold_arrow g ON s.ingredient_name = g.ingredient_name
WHERE g.inci_name IS NOT NULL
ORDER BY s.product_id, g.inci_name
"""


def write_contains_rel_csv() -> None:
    """silver_current × gold_product_ingredients → CONTAINS 관계 CSV → S3
    (gold/neo4j/oliveyoung/rels/CONTAINS/{run_id}/)
    """
    batch = create_batch_metadata(f"{PIPELINE_NAME}_neo4j")
    run_id = batch.run_id

    with job_unit(logger, job="silver_to_neo4j_csv.contains", run_id=run_id):
        catalog = OliveyoungIceberg.get_catalog()

        silver_arrow = catalog.load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE).scan(
            selected_fields=("product_id", "product_ingredients")
        ).to_arrow()

        gold_arrow = catalog.load_table(OliveyoungIceberg.GOLD_PRODUCT_INGREDIENTS_TABLE).scan(
            selected_fields=("ingredient_name", "inci_name")
        ).to_arrow()

        con = duckdb.connect()
        con.register("silver_arrow", silver_arrow)
        con.register("gold_arrow", gold_arrow)
        df = con.execute(_CONTAINS_QUERY).df()
        con.close()

        if df.empty:
            logger.warning("CONTAINS 관계 데이터 없음 — 업로드 skip")
            return

        header_csv = ":START_ID(Product),:END_ID(Ingredient)"
        data_csv   = df.to_csv(index=False, header=False)

        prefix = neo4j_csv_prefix(
            pipeline=PIPELINE_NAME,
            kind="rels",
            name="CONTAINS",
            run_id=run_id,
        )
        upload_csv_to_s3(header_csv, S3.BUCKET, f"{prefix}/header.csv",     S3.REGION)
        upload_csv_to_s3(data_csv,   S3.BUCKET, f"{prefix}/part-00000.csv", S3.REGION)

        log_process_summary(
            logger,
            job="silver_to_neo4j_csv.contains",
            run_id=run_id,
            upserted_nodes=len(df),
        )
        logger.info(f"CONTAINS 관계 {len(df)}건 업로드: s3://{S3.BUCKET}/{prefix}/")
