# 올리브영 과거 배치 백필 운영 절차

이 DAG는 **수동 전용**입니다. 한 크롤 `source_run_id`의 Bronze JSON 전체를 현재 정제 규칙으로 재처리하여 `silver_history`와 별도 DQ stage(`bronze_to_silver_backfill`)만 갱신합니다. `silver_current`·`silver_error`·gold·CDC·Neo4j는 변경하지 않습니다. 따라서 Neo4j가 자동으로 과거 데이터와 정합해진다고 간주하면 안 됩니다.

## 배포 전 확인

1. 이 Pipeline 이미지가 ECR `latest`로 빌드·배포되고, Airflow DAG 디렉터리에 `oliveyoung_backfill.py` 링크가 반영되어 UI에 보이는지 확인합니다. 코드 PR 머지만으로 실행되지 않습니다.
2. 배포 이미지에서 `pyiceberg==0.12.0`인지 확인합니다. 조건부 overwrite는 로컬 임시 Iceberg 테이블에서 검증했지만, 운영 Glue/S3 환경의 소규모 테스트와 비용 확인 전에는 본 데이터에 apply하지 않습니다.
3. 크롤의 정상 `oliveyoung_pipeline`과 Neo4j 반영이 끝났는지 확인합니다. 백필 중 자동 트리거가 들어올 수 있으므로 정상 DAG의 신규 실행을 운영적으로 차단·보류합니다. `max_active_runs=1`은 **백필끼리만** 직렬화합니다.

## Airflow UI에서 실행

`oliveyoung_backfill` → **Trigger DAG w/ config**. 먼저 읽기 전용 프리뷰를 실행합니다.

```json
{"source_run_id":"20260725","mode":"dry-run"}
```

로그의 `batch_date`, manifest 상태·일치 여부, subcategory/part 수, JSON 행수, 기존 history/DQ 키 및 동일 날짜 충돌을 확인합니다. `source_run_id`는 실제 크롤 checkpoint/S3 run ID를 넣습니다. crawl DQ가 없거나 날짜가 충돌하면 백필할 수 없습니다. 충돌이 있다면 임의 삭제·우회하지 말고 원인을 조사합니다.

프리뷰를 검토하고 정상 파이프라인과 경합하지 않음을 다시 확인한 뒤 **별도 DAG run**으로 적용합니다.

```json
{"source_run_id":"20260725","mode":"apply","confirm_source_run_id":"20260725"}
```

manifest가 `completed`가 아니거나 파일/건수가 일치하지 않지만 부분 백필을 수용하기로 결정했다면 `"allow_incomplete":true`를 명시합니다. 이것은 동일 날짜 충돌을 우회하지 않습니다. 적용 성공 로그의 `result: verified`와 DQ stage를 확인합니다. Discord의 `과거 백필 완료` 메시지는 보조 신호이며 웹훅 실패만으로 데이터 적재를 재시도하지 않습니다.

재시도는 같은 두 키의 조건부 overwrite입니다. history 쓰기 후 DQ 쓰기 실패가 나면 태스크는 실패로 남고, 재실행 시 둘 다 교체합니다. 다른 날짜/정상 행을 바꾸지 않는 것은 운영 환경에서도 사전 확인해야 합니다. 백필 후 `silver_current`와 Neo4j 상품 ID 차이를 읽기 전용으로 비교하고, 불일치는 별도 이슈로 기록합니다.
