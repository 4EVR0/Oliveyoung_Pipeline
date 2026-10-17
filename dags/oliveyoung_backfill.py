"""Manual history-only backfill. Trigger with conf; default is read-only dry-run."""

import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

ECR_REGISTRY = os.environ.get("ECR_REGISTRY", "")
IMAGE = f"{ECR_REGISTRY}/evr0/oliveyoung-pipeline:latest"

def _guard_no_normal_run(**context):
    """apply 전에 정상 파이프라인이 실행·대기 중이면 거부한다.

    백필도 silver_history에 스냅샷을 만든다. 정상 bronze_to_silver와 silver_to_gold 사이에
    끼면 CDC NEW(history 최근 두 스냅샷 비교)가 정상 배치가 아니라 백필 스냅샷을 비교하게 된다.
    dry-run은 읽기만 하므로 막지 않는다. 반대 방향(백필 중 크롤이 정상 실행을 트리거)은
    백필이 수 분이라 창이 작아 운영 절차로 관리한다.
    """
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState

    if (context["dag_run"].conf or {}).get("mode", "dry-run") != "apply":
        return
    active = [r.run_id for state in (DagRunState.RUNNING, DagRunState.QUEUED)
              for r in DagRun.find(dag_id="oliveyoung_pipeline", state=state)]
    if active:
        raise AirflowFailException(
            f"oliveyoung_pipeline 실행 중({', '.join(active)}) — 끝난 뒤 다시 apply하세요"
        )


with DAG(
    dag_id="oliveyoung_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "manual", "backfill"],
    # Airflow 2.9.2 skips the trigger form for DAGs without params by default.
    # Keep source_run_id required so a plain Trigger cannot create a useless run.
    params={
        "source_run_id": Param("", type="string", minLength=1,
                               description="크롤 S3/checkpoint의 실제 run_id"),
        "mode": Param("dry-run", type="string", enum=["dry-run", "apply"],
                      description="먼저 dry-run을 실행하고 검토 후 apply"),
        "confirm_source_run_id": Param("", type="string",
                                       description="apply일 때 source_run_id와 같은 값"),
        "allow_incomplete": Param(False, type="boolean",
                                  description="미완료/불일치 manifest의 부분 백필을 명시적으로 승인"),
    },
) as dag:
    guard = PythonOperator(task_id="guard_no_normal_run", python_callable=_guard_no_normal_run)

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
            "BACKFILL_DQ_DASHBOARD_URL": os.environ.get(
                "DQ_DASHBOARD_URL", "http://15.165.179.181:3000/d/oliveyoung-dq-table"
            ),
            "BACKFILL_SOURCE_RUN_ID": "{{ dag_run.conf.get('source_run_id', '') }}",
            "BACKFILL_MODE": "{{ dag_run.conf.get('mode', 'dry-run') }}",
            "BACKFILL_CONFIRM_SOURCE_RUN_ID": "{{ dag_run.conf.get('confirm_source_run_id', '') }}",
            "BACKFILL_ALLOW_INCOMPLETE": "{{ dag_run.conf.get('allow_incomplete', false) | lower }}",
        },
    )

    guard >> backfill
