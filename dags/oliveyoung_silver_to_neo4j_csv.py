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
    dag_id="oliveyoung_silver_to_neo4j_csv",
    schedule=None,  # 초기 적재용 — 필요 시 수동 트리거
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["oliveyoung", "etl", "neo4j"],
) as dag:

    silver_to_neo4j_csv = DockerOperator(
        task_id="silver_to_neo4j_csv",
        image=IMAGE,
        command="silver_to_neo4j_csv",
        **COMMON,
    )
