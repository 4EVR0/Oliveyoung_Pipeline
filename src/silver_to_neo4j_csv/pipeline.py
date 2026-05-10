"""
Silver → Neo4j CSV 파이프라인 오케스트레이션 로직.

silver_current 의 데이터를 Neo4j neo4j-admin import 형식의 CSV 로 변환해
S3 (gold/neo4j/oliveyoung/...) 에 업로드한다. Iceberg 테이블은 만들지 않는다.

향후 노드 라벨이나 관계가 추가되면 이 함수에서 차례로 호출만 추가하면 된다.
"""

from gold_pipeline.write_neo4j_csv import write_product_node_csv


def run_pipeline():
    """Silver → Neo4j CSV 파이프라인 전체를 실행합니다."""
    print("=== Silver → Neo4j CSV 파이프라인 시작 ===\n")

    print("1. Product 노드 CSV 생성 중...")
    write_product_node_csv()

    # 향후 추가:
    # print("2. Ingredient 노드 CSV 생성 중...")
    # write_ingredient_node_csv()
    # print("3. PRODUCT_HAS_INGREDIENT 관계 CSV 생성 중...")
    # write_product_has_ingredient_rel_csv()

    print("\n=== 완료 ===")
