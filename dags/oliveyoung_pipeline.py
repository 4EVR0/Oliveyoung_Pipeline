import os
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

ECR_REGISTRY = os.environ.get("ECR_REGISTRY", "")
IMAGE = f"{ECR_REGISTRY}/evr0/oliveyoung-pipeline:latest"

COMMON = dict(
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    auto_remove="success",
    mount_tmp_dir=False,
    force_pull=True,
    environment={
        "AWS_DEFAULT_REGION": "ap-northeast-2",
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "LOG_FORMAT": "json",
        "LOG_LEVEL": "INFO",
    },
)

with DAG(
    dag_id="oliveyoung_pipeline",
    schedule=None,  # 크롤링 DAG의 TriggerDagRunOperator로 실행
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["oliveyoung", "etl"],
) as dag:

    sync_reference = DockerOperator(
        task_id="sync_reference_data",
        image=IMAGE,
        command="sync_reference",
        **COMMON,
    )

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image=IMAGE,
        command="bronze_to_silver",
        **COMMON,
    )

    silver_to_gold = DockerOperator(
        task_id="silver_to_gold",
        image=IMAGE,
        command="silver_to_gold",
        **COMMON,
    )

    gold_cdc = DockerOperator(
        task_id="gold_cdc",
        image=IMAGE,
        command="gold_pipeline",
        **COMMON,
    )

    neo4j_incremental = DockerOperator(
        task_id="neo4j_incremental",
        image=IMAGE,
        command="neo4j_incremental",
        **{**COMMON, "environment": {
            **COMMON["environment"],
            "ICEBERG_WAREHOUSE": os.environ.get("ICEBERG_WAREHOUSE", ""),
            "NEO4J_PASSWORD":    os.environ.get("NEO4J_PASSWORD", ""),
            "NEO4J_BOLT_URI":    os.environ.get("NEO4J_BOLT_URI", "bolt://localhost:7687"),
            "NEO4J_USER":        os.environ.get("NEO4J_USER", "neo4j"),
        }},
    )

    sync_reference >> bronze_to_silver >> silver_to_gold >> gold_cdc >> neo4j_incremental
