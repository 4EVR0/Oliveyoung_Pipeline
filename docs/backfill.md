# 올리브영 과거 배치 백필 배포·운영 가이드

EC2 Airflow의 수동 `oliveyoung_backfill` DAG는 한 크롤 `source_run_id`의 Bronze JSON 전체를 **현재 사전·정제 규칙**으로 재처리한다. 쓰기 대상은 `silver_history`와 DQ `stage=bronze_to_silver_backfill`뿐이다. `silver_current`·`silver_error`·gold·CDC·Neo4j는 쓰지 않는다. **그래프 자동 정합이나 과거 시점 규칙의 재현은 보장하지 않는다.**

## 1. 배포 순서

1. [Oliveyoung_Pipeline PR #32](https://github.com/4EVR0/Oliveyoung_Pipeline/pull/32)를 리뷰·머지한다. GitHub Actions의 **Build and Push to ECR**와 **Deploy to EC2** 결과를 확인한다. 전자는 `:latest` 이미지를 빌드하고, 후자는 SSM으로 `/home/airflow/pipelines/Oliveyoung_Pipeline`의 `git pull`을 요청한다.
2. [Airflow_Infra PR #5](https://github.com/4EVR0/Airflow_Infra/pull/5)를 리뷰·머지하고 **Deploy to EC2** 결과를 확인한다. 추가된 DAG 파일은 Pipeline 레포를 가리키는 심볼릭 링크다. Compose는 `./dags`와 `./pipelines`를 Airflow 컨테이너에 마운트한다.
3. **SSM 명령 전송 성공만으로 EC2 반영 완료를 단정하지 않는다.** Airflow EC2에서 다음을 읽기 전용으로 확인한다. 두 SHA는 머지 커밋과 대조한다.

   ```bash
   cd /home/airflow
   git -C pipelines/Oliveyoung_Pipeline rev-parse --short HEAD
   git rev-parse --short HEAD
   readlink -f dags/oliveyoung_backfill.py
   docker compose exec airflow-scheduler airflow dags list | grep oliveyoung_backfill
   docker compose exec airflow-scheduler airflow dags list-import-errors
   ```

   링크는 `/home/airflow/pipelines/Oliveyoung_Pipeline/dags/oliveyoung_backfill.py`로 해석되어야 한다. DAG가 안 보이면 체크아웃 버전·링크·import error부터 확인한다. 새 DAG는 처음에 paused일 수 있으므로 UI 상태도 본다.
4. 이미지 빌드 완료와 ECR `:latest` 갱신을 확인한다. DockerOperator는 실행 시 `force_pull=True`이다. **Airflow 컨테이너의** Python을 검사해도 작업 이미지의 PyIceberg 버전은 알 수 없다. 먼저 `docker compose exec airflow-scheduler printenv ECR_REGISTRY`로 레지스트리 주소를 확인하고, 이를 아래 `REGISTRY`에 복사해 작업 이미지를 검사한다. Compose의 `.env` 값이 호스트 셸에 자동으로 export되지는 않는다.

   ```bash
   REGISTRY='위에서 확인한 ECR_REGISTRY 값'
   docker pull "${REGISTRY}/evr0/oliveyoung-pipeline:latest"
   docker run --rm --entrypoint python "${REGISTRY}/evr0/oliveyoung-pipeline:latest" -c 'import pyiceberg; print(pyiceberg.__version__)'
   ```

   기대 버전은 **0.12.0**이다. 레지스트리 인증이 안 되면 기존 EC2 Docker/ECR 인증을 점검하고, 버전을 확인하지 못한 채 apply하지 않는다.

## 2. 본 데이터 apply 전 필수 게이트

- 로컬 임시 Iceberg 테이블에서 category 파티션 history·비파티션 DQ의 **조건부 교체, 다른 행 보존, 0건 교체**를 검증했다. **실제 배포 이미지 + Glue/S3의 폐기 가능한 작은 테스트 테이블**에서는 아직 검증하지 않았다. 그 환경의 overwrite 동작·재시도·비용을 확인하기 전에는 **본 데이터 apply 금지**다. 실패하면 append나 더 넓은 필터로 우회하지 않는다.
- 폐기 가능한 테스트 테이블에는 **같은 category의 백필 키 행과 다른 정상 키 행**을 함께 넣어야 한다. 대상 키 overwrite 후 정상 행 보존, 같은 입력 재실행 후 행수 불변, 빈 Arrow로 대상 키만 0건 교체, 비파티션 DQ의 같은 stage/date/run 교체와 다른 run 보존을 확인한다. 테스트 테이블·S3 경로는 본 테이블과 분리하고, 대상 테이블 식별자와 삭제 범위를 확인한 뒤에만 정리한다.
- 현재 크롤이 끝나고 정상 `oliveyoung_pipeline`의 bronze→gold→Neo4j가 모두 완료됐는지 확인한다. 백필 중 정상 DAG의 **running/queued가 0**이고 새 자동 트리거가 들어오지 않을 운영 창을 확보한다. `max_active_runs=1`은 백필끼리만 제한한다. **정상 DAG pause만으로 외부 트리거 차단을 보장하지 않는다.**
- 대상은 **실제 크롤의 S3/checkpoint run ID**다. 날짜로 추정하지 않는다. crawl DQ 기록이 없거나 그 `batch_date`가 둘 이상이면 코드가 거부한다. 중단된 크롤의 일부 데이터를 자동으로 완전하다고 보지 않는다.
- 가능하면 백필 전 `silver_current`와 Neo4j의 상품 ID 차이를 읽기 전용으로 기록해 기존 불일치와 새 불일치를 구별한다.

## 3. Airflow UI: dry-run → 검토 → 별도 apply

`oliveyoung_backfill` → **Trigger DAG w/ config**에서 먼저 실행한다. 새 DAG가 paused라면 UI에서 활성화한다. dry-run은 Iceberg·S3 데이터 쓰기와 Discord 전송을 하지 않는다.

```json
{"source_run_id":"실제_크롤_RUN_ID","mode":"dry-run"}
```

`backfill_history_and_dq` 태스크 로그의 JSON 프리뷰를 검토한다.

| 필드 | 확인할 것 |
|---|---|
| `batch_date` | crawl DQ에서 조회한 논리 날짜가 기대값인지 |
| `manifest_status`, `manifest_consistent` | 기본 적용 조건은 `completed`와 `true` |
| `manifest_missing_parts`, `manifest_unlisted_parts` | 비어 있는지. 값이 있으면 원인 조사 |
| `subcategories`, `part_count`, `bronze_rows` | 예상 범위인지. JSON 읽기 오류는 태스크 실패 |
| `existing.history_other_jobs`, `existing.dq_other_runs`, `existing.dq_normal_runs` | 모두 빈 배열이어야 함. 동일 날짜 정상/다른 백필과 충돌하면 apply 거부 |
| `existing.history_same_key`, `existing.dq_same_key` | 동일 키 재실행인지 판단하는 기존 행 수 |

프리뷰가 실패하거나 충돌하면 **apply하지 않는다.** 다른 배치의 행 삭제나 `batch_date` 변경으로 우회하지 않는다. §2 게이트를 **적용 직전 다시** 확인한 뒤 **새 DAG run**으로 실행한다.

```json
{"source_run_id":"실제_크롤_RUN_ID","mode":"apply","confirm_source_run_id":"실제_크롤_RUN_ID"}
```

`confirm_source_run_id`는 정확히 일치해야 한다. manifest가 미완료/누락이거나 파일·행수와 불일치하지만 **부분 입력을 수용하기로 명시적으로 결정**했다면 `"allow_incomplete":true`를 추가할 수 있다. 날짜 충돌은 우회하지 못한다. 가능하면 dry-run 직후 apply해 입력 변동 가능성을 줄인다.

## 4. 성공 판정과 후속 확인

1. Airflow 태스크가 `success`이고 로그에 `"result": "verified"`와 `metrics`가 있는지 확인한다. 쓰기 순서는 **history → DQ → 두 테이블 재조회 검증**이다. `bronze_loaded`, `silver_ok`, `silver_error`, `err_*`와 유형별 합계를 검토한다.
2. DQ API를 이용할 수 있으면 `GET /dq/latest?stage=bronze_to_silver_backfill&metric=silver_ok` 응답의 `run_id=backfill_<source_run_id>`와 `batch_date`를 확인한다. dq_api 캐시 TTL은 약 60초라 직후 반영이 늦을 수 있다. 정상 DQ 패널은 `stage=bronze_to_silver`와 구별된다.
3. Discord의 **`과거 백필 완료`** 메시지는 검증 뒤 한 번 시도된다. 웹훅 미설정/전송 실패는 경고만 남긴다. 메시지만으로 성공·실패를 판단하지 않는다.
4. `silver_current`·gold·CDC·Neo4j는 백필로 변경되지 않았음을 확인한다. 다음 정상 실행 뒤 `silver_current`와 Neo4j 상품 ID 차이를 다시 비교하고 신규 불일치는 **별도 이슈**로 기록한다.
5. 보류했던 정상 트리거/운영 상태를 원복한다. 여러 과거 run이면 각 run마다 dry-run→apply→검증을 반복하고 다음 run 전에 충돌과 경합을 다시 확인한다.

## 5. 실패·재시도

| 상황 | 대응 |
|---|---|
| crawl DQ 없음/날짜 충돌, Bronze 없음/JSON 오류 | 입력 run·크롤 기록·S3 part를 조사한다. 날짜를 run ID에서 파생해 강행하지 않는다. |
| manifest 미완료/불일치 | 누락 part와 원인을 조사한다. 부분 범위가 확정된 경우에만 `allow_incomplete`를 검토한다. |
| 같은 날짜 정상 history/DQ 또는 다른 백필 run 존재 | 자동 거부가 정상이다. 수동 삭제하지 말고 대상·날짜 계약을 재검토한다. |
| history 성공 후 DQ 실패 또는 검증 실패 | 태스크 실패다. 원인 해결 후 **같은 `source_run_id`**로 재실행한다. 같은 키 범위가 조건부 교체된다. |
| Discord 전송 실패만 발생 | 데이터 검증 성공이라면 재적재하지 않는다. 웹훅 설정/전송 로그만 조사한다. |
| 정상 DAG가 동시에 시작됨 | 새 apply를 중단하고 상태를 확인한다. 이미 시작된 작업은 무작정 강제 종료·수동 삭제하지 말고 쓰기 범위와 결과를 확인한 뒤 복구 방안을 정한다. |

이 가이드는 실행 승인 자체가 아니다. **실제 Glue/S3 overwrite 검증과 정상 DAG 경합 방지 없이는 apply하지 않는다.**
