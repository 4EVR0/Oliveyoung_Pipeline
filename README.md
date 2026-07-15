# Oliveyoung_Pipeline

- 올리브영 크롤링 데이터를 Bronze → Silver → Gold 로 가공하는 ETL 파이프라인
- 원시 제품 데이터에서 성분을 정규화·집계해 Apache Iceberg 테이블로 적재하고,
변경분을 Neo4j 그래프에 증분 반영한다

## 데이터 흐름

```
Bronze (S3 JSON)
  └ DuckDB 로 서브카테고리별 최신 run_id 파일만 로드
Silver (Iceberg · oliveyoung_db)
  ├ oliveyoung_silver_current    최신 스냅샷 (overwrite)
  ├ oliveyoung_silver_history    시계열 누적 (append)
  └ oliveyoung_silver_error      처리 실패 DLQ (overwrite)
Gold (Iceberg · oliveyoung_db)
  ├ gold_product_ingredients     성분 × INCI 메타데이터 mart (overwrite)
  ├ gold_ingredient_frequency    카테고리별 Top 50 성분 (append)
  └ gold_product_change_log      CDC 변경 이력 NEW/REMOVED/CHANGED (append)
Neo4j (GraphDB)
  └ change_log 미처리 배치를 읽어 Product/CONTAINS 증분 반영 (checkpoint 기반)
```

성분 매칭에 필요한 KCIA·CosIng 표준 데이터는 별도 레포(INCI 파이프라인)가 만든
`inci_db` Iceberg 테이블에서 읽어온다

## 처리 단계

1. **Bronze 로드** — S3 glob 으로 서브카테고리별 최신 `run_id` JSON 탐색 (DuckDB)
2. **정제** — 노이즈 제거, 성분 문자열 정규화, 번들 제품 감지, 오타 교정
3. **매칭** — Aho-Corasick 오토마타로 KCIA 표준명 일괄 매칭 (O(n+m))
4. **분기** — 정상 → Silver current/history, 실패 → Silver error(DLQ)
5. **Gold** — Silver × INCI 조인으로 성분 mart, 카테고리별 빈도 집계
6. **CDC** — Silver 스냅샷 비교로 NEW/REMOVED/CHANGED 추출 → change_log
7. **Neo4j** — change_log 미처리분을 그래프에 증분 적용

## 디렉터리

```
config/settings.py          S3 / Glue Catalog / DuckDB 설정 (oliveyoung_db, inci_db)
oliveyoung_common/          공용 S3 경로 상수 (git submodule)
data/                       오타·불량키워드·커스텀 성분 사전 (JSON)
src/
  bronze_to_silver/         정제 + KCIA 매칭 (cleaner, ac_builder)
  silver_to_gold/           CDC + Gold 집계
  silver_to_neo4j_csv/      Neo4j 초기 적재용 CSV 익스포트
reference_pipeline/         사전 JSON → Iceberg 동기화
silver_pipeline/            Silver 테이블 스키마·생성·write
gold_pipeline/              Gold 테이블·CDC·Neo4j CSV write
neo4j_incremental.py        change_log → Neo4j 증분 반영
scripts/entrypoint.sh       컨테이너 실행 모드 분기
dags/                       Airflow DAG (DockerOperator)
```

> `oliveyoung_common` 은 서브모듈이므로 클론 시 `git clone --recurse-submodules`,
> 이미 받았다면 `git submodule update --init`.

## 실행

설정은 코드에 상수로 두고, AWS 인증은 EC2 IAM Role 로 처리한다(키 주입 불필요)

### 테이블 초기화 (최초 1회)

```bash
python silver_pipeline/create_silver.py
python silver_pipeline/create_category_master.py
python reference_pipeline/create_reference_tables.py
python reference_pipeline/sync_reference_data.py
python gold_pipeline/create_gold_tables.py all   # dq_metrics 포함 Gold 전체 (개별: dq_metrics 등 인자 지정)
python gold_pipeline/create_gold_product_ingredients.py
```

### Docker

EC2 IAM Role 을 그대로 쓰기 위해 `--network host` 로 실행한다.
인자는 `entrypoint.sh` 의 모드명이다

```bash
docker build -t oliveyoung-pipeline .

docker run --network host oliveyoung-pipeline sync_reference
docker run --network host oliveyoung-pipeline bronze_to_silver
docker run --network host oliveyoung-pipeline silver_to_gold
docker run --network host oliveyoung-pipeline neo4j_incremental
```

| 모드 | 동작 |
|------|------|
| `sync_reference` | 사전 JSON → Iceberg 동기화 |
| `bronze_to_silver` | Bronze 로드 → 정제 → Silver |
| `silver_to_gold` | CDC + Gold 집계/mart |
| `neo4j_incremental` | change_log → Neo4j 증분 반영 |
| `silver_to_neo4j_csv` | Neo4j 초기 적재용 CSV → S3 |
| `create_reference_tables` / `create_gold_product_ingredients` | 테이블 생성 |

## Airflow

DockerOperator 기반 DAG 두 개로 운영한다

```
oliveyoung_pipeline             (schedule=None — 크롤링 DAG 가 트리거)
  sync_reference → bronze_to_silver → silver_to_gold → neo4j_incremental

oliveyoung_silver_to_neo4j_csv  (수동 — 그래프 초기 벌크 적재용)
  silver_to_neo4j_csv
```

정상 운영에서 Neo4j 는 `neo4j_incremental` 로 변경분만 반영하고,
CSV 익스포트는 그래프를 처음 채울 때만 쓴다

## 주요 테이블

### Silver (`oliveyoung_silver_current` / `_history`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `category_id` | string | 카테고리 (파티션 키) |
| `product_id` | string | 제품 ID (UUID v5) |
| `product_brand` / `product_name` | string | 브랜드 / 정제된 제품명 |
| `product_ingredients` | list\<string\> | KCIA 표준명으로 정규화된 성분 |
| `rating` / `review_count` | float / int | 평점 / 리뷰 수 |
| `review_stats` | map | 리뷰 통계 |
| `crawled_at` | timestamptz | 크롤링 시각 |
| `batch_job` / `batch_date` | string / timestamptz | 배치 메타 |

### Gold

- `gold_product_ingredients` — 성분 × INCI 메타데이터(INCI/한글명/기능/규제/사용수). overwrite
- `gold_ingredient_frequency` — 카테고리별·전체(TOTAL) Top 50 성분. append
- `gold_product_change_log` — `change_type` = NEW / REMOVED / CHANGED. append

### Silver Error (DLQ)

처리 실패 레코드를 사유와 함께 적재해 사후 재처리·분석에 쓴다
`INCOMPLETE_DATA` · `OPTION_BUNDLE` · `INVALID_METADATA` · `HETEROGENEOUS_BUNDLE`
· `DUPLICATE_PRODUCT` · `UNMAPPED_RESIDUAL` · `HIDDEN_BUNDLE`.

### 정합성 메트릭 (`dq_metrics`)

파이프라인 각 단계가 남기는 데이터 정합성 수치를 모으는 **key/value(EAV) 테이블**.
스키마 진화 없이 지표를 추가할 수 있고, 여러 파이프라인(crawl·bronze_to_silver·silver_to_gold 등)이 같은 테이블에 적재한다.
`batch_date`(YYYY-MM-DD) · `run_id` · `stage` · `metric_name` · `metric_value` · `target_table` · `created_at`.

- 스키마·writer는 `oliveyoung_common/dq_metrics.py`가 소유(순수함수 `write_dq_metrics`), 생성은 `create_gold_tables.py dq_metrics`.
- 각 단계가 `log_dq`(Loki 로그) + `write_dq_metrics`(테이블)로 **같은 수치를 이중 기록**(테이블 적재는 비치명적).
- `batch_date`(단계 관통 논리 배치 날짜)로 crawl·bronze_to_silver·silver_to_gold를 한 배치로 묶어, 대시보드 그래프의 한 시점을 클릭하면 그 배치의 silver 행으로 드릴다운한다. `run_id`는 초단위 유니크 실행 식별.
- 테이블 실제 위치는 `GOLD_PATH`(`olive_young_gold/dq_metrics/`). 조회는 **dq_api**(pyiceberg+DuckDB)가 읽어 Grafana(Infinity)에 노출.

## 설계 메모

- **current / history 분리** — 최신 조회용(`current`, overwrite)과 시계열 추적용(`history`, append)을 나눠 재읽기 없이 변화를 본다.
- **CDC = Iceberg 스냅샷 비교** — 전체 스캔 없이 최신 2개 스냅샷의 `product_id` 집합·추적 필드 차이로 변경분을 뽑는다.
- **Neo4j 증분 + checkpoint** — `neo4j_sync_checkpoint` 에 마지막 처리 배치를 남겨 change_log 의 신규분만 그래프에 반영한다.
- **Aho-Corasick 매칭** — 수천 개 표준명을 한 번의 스캔으로 동시 탐색. 성분명 내부 쉼표는 마스킹으로 분리.
- **사전 분리 관리** — 오타·불량키워드·커스텀 성분 사전을 Git 의 JSON 으로 두고 `sync_reference_data.py` 로 Iceberg 동기화. 코드 재배포 없이 수정.
- **정합성 = 로그 + 테이블 이중 소스** — 각 단계가 `log_dq`(Loki)와 `write_dq_metrics`(Iceberg `dq_metrics`)로 같은 수치를 남긴다. 로그 기반 대시보드는 유지하고, 테이블 소스는 dq_api→Grafana로 별도 대시보드에 노출(장기 추세·드릴다운). 지표 추가는 key/value라 스키마 진화 불필요.

## 인프라

- 스토리지: AWS S3 / 메타스토어: AWS Glue Catalog / 리전: `ap-northeast-2`
- 런타임: Python 3.12, EC2 (IAM Role 인증)
- 주요 의존성: `pyiceberg`, `duckdb`, `pyahocorasick`, `pandas`/`pyarrow`, `boto3`, `neo4j`
