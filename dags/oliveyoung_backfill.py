"""Manual history-only backfill. Trigger with conf; default is read-only dry-run."""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

ECR_REGISTRY = os.environ.get("ECR_REGISTRY", "")
IMAGE = f"{ECR_REGISTRY}/evr0/oliveyoung-pipeline:latest"

with DAG(
    dag_id="oliveyoung_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "manual", "backfill"],
) as dag:
    backfill = DockerOperator(
        task_id="backfill_history_and_dq",
        image=IMAGE,
        command="backfill",
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
            "DISCORD_DQ_WEBHOOK_URL": os.environ.get("DISCORD_DQ_WEBHOOK_URL", ""),
            "BACKFILL_SOURCE_RUN_ID": "{{ dag_run.conf.get('source_run_id', '') }}",
            "BACKFILL_MODE": "{{ dag_run.conf.get('mode', 'dry-run') }}",
            "BACKFILL_CONFIRM_SOURCE_RUN_ID": "{{ dag_run.conf.get('confirm_source_run_id', '') }}",
            "BACKFILL_ALLOW_INCOMPLETE": "{{ dag_run.conf.get('allow_incomplete', false) | lower }}",
        },
    )
