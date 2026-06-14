"""
Neo4j 증분 업데이트 스크립트

gold_product_change_log (Iceberg) 에서 미처리 배치를 읽어
Neo4j 에 NEW / REMOVED / CHANGED 를 반영합니다.

체크포인트: oliveyoung_db.neo4j_sync_checkpoint (Iceberg)

환경변수:
    ICEBERG_WAREHOUSE   S3.ICEBERG_METADATA_PATH 값
    NEO4J_PASSWORD      Neo4j 비밀번호
    NEO4J_BOLT_URI      (선택, 기본값: bolt://localhost:7687)
    NEO4J_USER          (선택, 기본값: neo4j)
    AWS_DEFAULT_REGION  (선택, 기본값: ap-northeast-2)
"""

import logging
import os
from datetime import datetime, timezone

import pyarrow as pa
from neo4j import GraphDatabase
from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AWS_REGION        = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE")
NEO4J_URI         = os.environ.get("NEO4J_BOLT_URI", "bolt://localhost:7687")
NEO4J_USER        = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD    = os.environ.get("NEO4J_PASSWORD")

CHANGE_LOG_TABLE  = "oliveyoung_db.gold_product_change_log"
INGREDIENTS_TABLE = "oliveyoung_db.gold_product_ingredients"
CHECKPOINT_TABLE  = "oliveyoung_db.neo4j_sync_checkpoint"


def _get_catalog():
    return load_catalog(
        "glue",
        **{
            "type":      "glue",
            "warehouse": ICEBERG_WAREHOUSE,
            "s3.region": AWS_REGION,
        },
    )


# ==========================================
# 체크포인트
# ==========================================

def get_last_checkpoint(catalog) -> str | None:
    table = catalog.load_table(CHECKPOINT_TABLE)
    df = table.scan(selected_fields=("batch_job", "synced_at")).to_pandas()
    if df.empty:
        return None
    return df.sort_values("synced_at").iloc[-1]["batch_job"]


def write_checkpoint(
    catalog,
    batch_job: str,
    new_count: int,
    removed_count: int,
    changed_count: int,
) -> None:
    table  = catalog.load_table(CHECKPOINT_TABLE)
    schema = table.schema().as_arrow()
    arrow_table = pa.table(
        {
            "batch_job":     pa.array([batch_job],     type=pa.string()),
            "synced_at":     pa.array([datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            "new_count":     pa.array([new_count],     type=pa.int32()),
            "removed_count": pa.array([removed_count], type=pa.int32()),
            "changed_count": pa.array([changed_count], type=pa.int32()),
        },
        schema=schema,
    )
    table.append(arrow_table)
    logger.info(f"체크포인트 저장: {batch_job}  NEW={new_count}  REMOVED={removed_count}  CHANGED={changed_count}")


# ==========================================
# 변경분 읽기
# ==========================================

def get_pending_changes(catalog, after_batch_job: str | None):
    df = catalog.load_table(CHANGE_LOG_TABLE).scan().to_pandas()
    if df.empty:
        return df
    if after_batch_job is not None:
        df = df[df["batch_job"] > after_batch_job]
    return df.reset_index(drop=True)


def get_ingredient_mapping(catalog) -> dict[str, str]:
    """ingredient_name → inci_name"""
    df = catalog.load_table(INGREDIENTS_TABLE).scan(
        selected_fields=("ingredient_name", "inci_name")
    ).to_pandas()
    return dict(zip(df["ingredient_name"], df["inci_name"]))


# ==========================================
# Neo4j 반영
# ==========================================

def _merge_contains(tx, product_id: str, ingredients: list, mapping: dict[str, str]) -> None:
    for raw_name in ingredients:
        inci = mapping.get(raw_name)
        if not inci:
            continue
        tx.run(
            """
            MERGE (i:Ingredient {ingredient_id: $inci})
            WITH i
            MATCH (p:Product {product_id: $product_id})
            MERGE (p)-[:CONTAINS]->(i)
            """,
            inci=inci,
            product_id=product_id,
        )


def apply_new(tx, product: dict, mapping: dict[str, str]) -> None:
    tx.run(
        """
        MERGE (p:Product {product_id: $product_id})
        SET p.product_name = $product_name,
            p.brand        = $brand,
            p.category     = $category
        """,
        product_id=product["product_id"],
        product_name=product.get("product_name"),
        brand=product.get("product_brand"),
        category=product.get("category_id"),
    )
    _merge_contains(tx, product["product_id"], product.get("product_ingredients") or [], mapping)


def apply_removed(tx, product_id: str) -> None:
    tx.run(
        "MATCH (p:Product {product_id: $product_id}) DETACH DELETE p",
        product_id=product_id,
    )


def apply_changed(tx, product: dict, mapping: dict[str, str]) -> None:
    tx.run(
        """
        MATCH (p:Product {product_id: $product_id})
        SET p.product_name = $product_name,
            p.brand        = $brand,
            p.category     = $category
        """,
        product_id=product["product_id"],
        product_name=product.get("product_name"),
        brand=product.get("product_brand"),
        category=product.get("category_id"),
    )
    tx.run(
        "MATCH (p:Product {product_id: $product_id})-[r:CONTAINS]->() DELETE r",
        product_id=product["product_id"],
    )
    _merge_contains(tx, product["product_id"], product.get("product_ingredients") or [], mapping)


# ==========================================
# 진입점
# ==========================================

def run() -> None:
    catalog = _get_catalog()
    driver  = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    last_batch_job = get_last_checkpoint(catalog)
    logger.info(f"마지막 체크포인트: {last_batch_job or '없음 (첫 실행)'}")

    changes = get_pending_changes(catalog, last_batch_job)
    if changes.empty:
        logger.info("처리할 변경 없음")
        driver.close()
        return

    latest_batch_job   = changes["batch_job"].max()
    ingredient_mapping = get_ingredient_mapping(catalog)

    new_rows     = changes[changes["change_type"] == "NEW"]
    removed_rows = changes[changes["change_type"] == "REMOVED"]
    changed_rows = changes[changes["change_type"] == "CHANGED"]

    logger.info(f"처리 대상 — NEW={len(new_rows)}  REMOVED={len(removed_rows)}  CHANGED={len(changed_rows)}")

    with driver.session() as session:
        for _, row in new_rows.iterrows():
            session.execute_write(apply_new, row.to_dict(), ingredient_mapping)

        for _, row in removed_rows.iterrows():
            session.execute_write(apply_removed, row["product_id"])

        for _, row in changed_rows.iterrows():
            session.execute_write(apply_changed, row.to_dict(), ingredient_mapping)

        session.run(
            """
            CREATE (s:SyncLog {
                batch_job:     $batch_job,
                synced_at:     datetime($synced_at),
                new_count:     $new_count,
                removed_count: $removed_count,
                changed_count: $changed_count
            })
            """,
            batch_job=latest_batch_job,
            synced_at=datetime.now(timezone.utc).isoformat(),
            new_count=len(new_rows),
            removed_count=len(removed_rows),
            changed_count=len(changed_rows),
        )


    write_checkpoint(catalog, latest_batch_job, len(new_rows), len(removed_rows), len(changed_rows))
    driver.close()
    logger.info("증분 업데이트 완료")


if __name__ == "__main__":
    run()
