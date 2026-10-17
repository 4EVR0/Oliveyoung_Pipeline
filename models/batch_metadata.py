import os

from oliveyoung_common.batch import BatchMetadata, create_batch_metadata, add_batch_metadata

__all__ = ["BatchMetadata", "create_batch_metadata", "add_batch_metadata", "check_batch_date_required"]


def check_batch_date_required() -> None:
    """Airflow 정상 실행(REQUIRE_BATCH_DATE=1)인데 BATCH_DATE가 비면 중단한다.

    재실행 멱등 교체 키가 batch_date라, conf 없이 돌아 실행 시각·run_id로 날짜가 파생되면
    엉뚱한 날짜의 행을 교체할 수 있다. Airflow 밖 단독 실행은 기존 폴백을 그대로 쓴다.
    """
    if os.environ.get("REQUIRE_BATCH_DATE") == "1" and not os.environ.get("BATCH_DATE"):
        raise RuntimeError(
            "BATCH_DATE가 비어 있습니다 — DAG를 conf {\"batch_date\": \"YYYY-MM-DD\"}로 실행하세요"
        )
