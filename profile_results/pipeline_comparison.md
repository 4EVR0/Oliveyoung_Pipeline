# Bronze → Silver 전후 비교 측정 방법

`scripts/benchmark_pipeline_comparison.py`는 동일 입력으로 실제 S3 읽기, 정제,
Glue/Iceberg 쓰기, S3 CSV 업로드를 측정한다. Silver → Gold와 Neo4j 단계는
이번 비교 대상이 아니다. 전체 시간은 DuckDB 연결부터 CSV 업로드 완료까지이며,
컨테이너 시작과 Python 모듈 import는 포함하지 않는다.

## 비교 조건

| 조건 | 입력 | Iceberg 쓰기 | 메타데이터 로드 |
| --- | --- | --- | --- |
| `before` | 원본 JSON 직접 읽기 | `135398e`의 실제 순차 쓰기 코드 | 이전 코드의 무조건 reload |
| `after_materialized` | JSON → Parquet 생성 + 읽기 | 현재 코드의 3개 테이블 병렬 쓰기 | schema 변경 시에만 reload |
| `after_cached` | 준비된 Parquet 읽기 | 현재 코드의 3개 테이블 병렬 쓰기 | schema 변경 시에만 reload |

개선 후 두 조건에는 현재의 개선 요소를 모두 적용한다. `after_materialized`는
매 실행마다 Parquet 생성 비용을 포함한다. `after_cached`는 기존
`load_bronze_data_from_files(..., materialize_optimized=False)` 경로를 사용한다.
운영 `run_pipeline()`에는 자동 캐시 유효성 검사/재사용 정책이 없으므로 캐시 결과를
현재 기본 실행의 성능으로 해석하면 안 된다. `BRONZE_LOAD_FORMAT`의 기본값도
여전히 `json`이며, 운영에서 Parquet 적용 시 `BRONZE_LOAD_FORMAT=parquet`을 지정한다.

개선 전후 cleaner와 사전 처리 코드는 동일하다. 입력 SQL도 이전 JSON 로딩과
동일하다. 비교 스크립트는 이전 writer를 Git에서 추출하여 직접 실행하므로
병렬 옵션만 끄면서 메타데이터 개선을 남겨 놓는 비교가 아니다.

## 동일 조건과 검증

- 최신 파일 목록을 한 번 고정하고 S3 ETag, VersionId(제공되는 경우), 바이트 크기를 기록한다.
  각 실행의 전후에 전체 파일 메타데이터를 재확인하며, 시간 측정 안의 최신 파일
  탐색 결과도 고정 목록과 같아야 한다. 파일 변경 시 실행을 실패시킨다.
- KCIA와 reference 사전 4개 테이블의 Iceberg snapshot ID를 고정한다.
  각 실행은 고정 스냅샷을 다시 읽고 Aho-Corasick을 다시 구성한다.
- 배치 시각을 고정하므로 `batch_date`를 제외하지 않고 모든 결과 컬럼을 검증한다.
- 각 실행은 별도 Python 프로세스와 새 DuckDB 연결을 사용한다. 조건별 준비 실행
  1회를 제외하고 5회를 측정하며 실행 순서를 회전한다. OS 페이지 캐시, DNS,
  AWS 서비스 캐시는 통제하지 않는다. 따라서 물리적 cold-cache 측정이라고 부르지 않는다.
- 실행마다 독립된 `oliveyoung_db.bench_<measurement>_<mode>_<iteration>_*`
  테이블 3개를 생성한다. 운영 테이블과 같은 schema, partition spec, sort order,
  properties를 사용하고 모두 동일한 결과 배치 1개로 초기화한다. current/error는
  overwrite, history는 append한다. 초기화와 검증은 측정 시간 밖이다.
- 입력과 정제 결과의 컬럼명·전체 행 값을 비교한다. 행 순서는 무시하지만 중복
  개수와 리스트 내부 순서는 보존하며, 정렬한 행 다중집합의 SHA-256을 기록한다.
  Iceberg 결과는 실제로 다시 읽어 schema와 전체 행 다중집합을 seed와 비교한다.
  history는 seed + 측정 배치의 2배 행을 검증한다. CSV는 S3에서 다시 내려받아
  전체 바이트 SHA-256을 조건과 반복 실행 전체에 걸쳐 비교한다.
- 표준편차는 표본 표준편차(`n-1`), MAD는 중앙값에서의 절대 편차 중앙값이다.
  병목 비율은 각 실행의 `stage / total` 비율의 중앙값이다. 병렬 하위 작업은
  겹치므로 합산하지 않고 `iceberg_write` wrapper 시간을 사용한다.
  원시 JSON의 `cpu_seconds`는 process-wide `process_time()`이므로 병렬 작업별
  전용 CPU 시간으로 해석하거나 합산하지 않는다. RSS 역시 프로세스 최고치다.

준비·검증·원본 불변성 확인 비용은 파이프라인 시간이 아니라 측정 장치 비용이다.
전체 시간에는 연결, 파일 탐색, 입력 로드, 사전 준비, 정제, 실제 출력 쓰기를
모두 포함한다. 준비된 history는 1배치이므로 장기간 누적된 운영 history의 모든
메타데이터/파일 상태를 재현하는 결과는 아니다. 함께 실행 중인 서비스의 부하와
S3/Glue 네트워크 변동은 원시 결과의 load average와 편차를 함께 보고 판단한다.

## 재실행

동일 의존성이 설치된 pipeline Docker 이미지를 사용한다. AWS 자격증명은
읽기 전용으로 마운트하고, 측정 결과 디렉터리는 Git 저장소 밖에 둔다.
현재 데이터에 다시 실행하면 새로운 파일 목록/스냅샷을 고정한 별도 측정이 된다.

```bash
mkdir -p /tmp/oliveyoung-comparison-baseline /tmp/oliveyoung-comparison-results
git show 135398e:silver_pipeline/write_silver.py > /tmp/oliveyoung-comparison-baseline/write_silver.py
docker run --rm --network host --cpus 2 --memory 3g \
  -v "$HOME/.aws:/root/.aws:ro" \
  -v "$PWD:/app:ro" \
  -v /tmp/oliveyoung-comparison-baseline:/baseline:ro \
  -v /tmp/oliveyoung-comparison-results:/results \
  --entrypoint python oliveyoung-pipeline-local:latest \
  scripts/benchmark_pipeline_comparison.py prepare --output /results \
  --revision "$(git rev-parse HEAD)" \
  --image "$(docker inspect oliveyoung-pipeline-local:latest --format '{{.Id}}')"
docker run --rm --network host --cpus 2 --memory 3g \
  -v "$HOME/.aws:/root/.aws:ro" \
  -v "$PWD:/app:ro" \
  -v /tmp/oliveyoung-comparison-baseline:/baseline:ro \
  -v /tmp/oliveyoung-comparison-results:/results \
  --entrypoint python oliveyoung-pipeline-local:latest \
  scripts/benchmark_pipeline_comparison.py run --output /results --repeats 5
```

`prepare`는 동일 이름의 manifest 덮어쓰기를 거부하며, `run`도 기존 측정용 테이블을
덮어쓰지 않는다. 중단 후 다시 시작할 때는 새 결과 디렉터리로 새 측정을 준비한다.
원본/seed Parquet와 실행 로그는 커밋하지 않는다. 집계 JSON에는 입력 경로와 크기,
환경, source hash, snapshot ID, 각 실행의 단계별 원시 시간, 검증 해시가 들어간다.

측정이 끝나면 동일 이미지와 볼륨으로 다음 명령을 실행해 측정용 리소스를 정리한다.
`--apply`를 빼면 삭제 대상의 테이블 수와 S3 객체 수만 확인한다. manifest의 고정
측정 ID, 테이블 이름, 실제 저장 위치를 모두 검사하며 운영 입력/출력은 삭제 대상이 아니다.
S3 버전 관리가 활성화된 버킷의 과거 객체 버전은 버킷 수명 주기 정책을 따른다.

```bash
python scripts/cleanup_pipeline_benchmark.py --manifest /results/manifest.json
python scripts/cleanup_pipeline_benchmark.py --manifest /results/manifest.json --apply
```

## 검증 명령

```bash
docker run --rm -v "$PWD:/app:ro" --entrypoint python \
  oliveyoung-pipeline-local:latest scripts/test_benchmark_pipeline_comparison.py
python3 -m py_compile scripts/benchmark_pipeline_comparison.py \
  scripts/test_benchmark_pipeline_comparison.py src/bronze_to_silver/pipeline.py
git diff --check
```

Parquet 읽기의 `hive_partitioning=false`는 `file_set=...` 캐시 디렉터리가
입력 데이터 컬럼으로 유입되는 것을 방지한다. 로컬 회귀 테스트는 JSON,
생성 직후 Parquet, 재사용 Parquet의 전체 행과 컬럼이 같은지 확인한다.
