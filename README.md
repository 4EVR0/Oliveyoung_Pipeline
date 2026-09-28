# Iceberg Pipeline — Olive Young 성분 데이터 파이프라인

Olive Young 크롤링 데이터를 **Medallion Architecture (Bronze → Silver → Gold)** 로 처리하는 ETL 파이프라인입니다.
원시 제품 데이터에서 성분 정보를 정규화·집계하여 Apache Iceberg 테이블로 저장합니다.

---

## 데이터 파이프라인 아키텍처

이 파이프라인은 크롤러가 S3에 적재한 Bronze JSON을 입력으로 받아, 정제·표준화된 Silver Iceberg 테이블과 분석·그래프 연동용 Gold 산출물을 생성합니다.
저장 계층은 S3, 메타스토어는 AWS Glue Catalog, 테이블 포맷은 Apache Iceberg를 사용합니다.

```text
Oliveyoung Crawling
    |
    v
Bronze Layer (S3 JSON)
    |  최신 run_id 탐색 / DuckDB 로드
    v
Reference + INCI Layer
    |  typo map / garbage keywords / custom ingredients
    |  inci_db.silver_kcia_cosing_graphrag_current
    v
Bronze to Silver
    |  정제 / 성분 표준화 / 오류 분리 / batch metadata 부여
    v
Silver Layer (Iceberg — oliveyoung_db)
    |-- oliveyoung_silver_current   최신 제품 상태 (overwrite)
    |-- oliveyoung_silver_history   시계열 감사 로그 (append)
    |-- oliveyoung_silver_error     처리 실패 DLQ (overwrite)
    |
    v
Silver to Gold
    |  INCI 메타데이터 조인 / 성분 빈도 집계 / 변경 이력 생성
    v
Gold Layer (Iceberg — oliveyoung_db)
    |-- gold_product_ingredients    unique 성분 x INCI 메타데이터 (overwrite)
    |-- gold_ingredient_frequency   카테고리별 성분 빈도 (append)
    |-- gold_product_change_log     CDC 변경 이력 (append)
    |
    v
Neo4j Export (S3 CSV)
    |-- nodes/Product/{run_id}/
    |-- rels/CONTAINS/{run_id}/
```

### 레이어별 책임

| 레이어 | 저장 위치 | 책임 |
|--------|-----------|------|
| Bronze | S3 JSON | 크롤러 원본 결과 보존, run_id 단위 입력 제공 |
| Reference | Git JSON + Iceberg | 오타 보정, 불량 키워드, 커스텀 성분 사전 관리 |
| INCI | `inci_db` Iceberg | KCIA/COSING 기반 표준 성분 메타데이터 제공 |
| Silver | `oliveyoung_db` Iceberg | 제품 단위 정제 결과, 오류 DLQ, 감사 로그 저장 |
| Gold | `oliveyoung_db` Iceberg | 성분 분석, INCI 확장 정보, 변경 이력 저장 |
| Neo4j CSV | S3 CSV | 그래프 DB 초기 적재용 노드·관계 파일 생성 |

### 처리 흐름

1. **Load Bronze** — S3 glob으로 서브카테고리별 최신 `run_id` JSON 파일 탐색
2. **Load Metadata** — `inci_db.silver_kcia_cosing_graphrag_current`(Iceberg) + Reference 테이블 로드
3. **Clean** — 노이즈 제거, 성분 문자열 정규화, 번들 제품 감지, 오타 교정
4. **Match** — Aho-Corasick 오토마타로 KCIA 표준명 매칭 (O(n+m))
5. **Split** — 정상 레코드 → Silver current/history, 오류 레코드 → Silver error
6. **Join** — Silver unique 성분 × `inci_db.gold_kcia_cosing_ingredients_current` LEFT JOIN → `gold_product_ingredients`
7. **Aggregate** — 카테고리별 성분 빈도 집계 → `gold_ingredient_frequency`
8. **Export** — Iceberg(Parquet) + S3 CSV 저장
9. **Neo4j CSV** — Silver × Gold 조인으로 Product 노드·CONTAINS 관계 CSV → S3 (`gold/neo4j/`)

---

## 디렉토리 구조

```
Iceberg_pipeline/
├── Dockerfile                         # 파이프라인 컨테이너 이미지 빌드
├── requirements.txt                   # 파이프라인 의존성 (Docker 빌드용)
├── requirements-dev.txt               # 분석·로컬 개발용 의존성 (jupyter 포함)
├── scripts/
│   └── entrypoint.sh                  # 컨테이너 실행 모드 분기
├── dags/
│   └── oliveyoung_pipeline.py         # Airflow DAG (DockerOperator)
├── config/
│   └── settings.py                    # AWS / S3 / OliveyoungIceberg / INCIIceberg / DuckDB 설정
├── data/                              # 로컬 Reference JSON 파일
│   ├── typo_map.json                  # 정확 매칭 오타 사전
│   ├── typo_map_regex.json            # 정규식 기반 오타 패턴
│   ├── product_name_norm_map.json     # 제품명 정규화 규칙
│   ├── garbage_keywords.json          # 불량 키워드 필터
│   └── custom_ingredient_dict.json    # KCIA 미등재 성분 보정
├── models/
│   └── pipeline_models.py             # Dataclass: Dictionaries, ErrorRecord
├── src/
│   ├── bronze_to_silver/              # Bronze → Silver ETL
│   │   ├── main.py                    # 실행 진입점
│   │   ├── pipeline.py                # 오케스트레이션
│   │   ├── cleaner.py                 # 데이터 정제 로직
│   │   └── ac_builder.py             # Aho-Corasick 오토마타 빌더
│   ├── silver_to_gold/               # Silver → Gold ETL
│   │   ├── main.py                    # 실행 진입점
│   │   └── pipeline.py               # 오케스트레이션
│   └── silver_to_neo4j_csv/          # Silver → Neo4j CSV
│       ├── main.py                    # 실행 진입점
│       └── pipeline.py               # 오케스트레이션
├── reference_pipeline/                # Reference 데이터 관리
│   ├── schemas.py
│   ├── create_reference_tables.py
│   └── sync_reference_data.py         # JSON → Iceberg 동기화
├── silver_pipeline/                   # Silver Iceberg 테이블 관리
│   ├── schemas.py
│   ├── create_silver.py
│   ├── create_category_master.py
│   └── write_silver.py
├── gold_pipeline/                     # Gold Iceberg 테이블 관리 + Neo4j CSV 작성
│   ├── schemas.py
│   ├── create_gold_tables.py
│   ├── create_gold_product_ingredients.py
│   ├── write_gold.py
│   ├── write_gold_product_ingredients.py
│   └── write_neo4j_csv.py             # Neo4j 노드·관계 CSV 빌더 (Product, CONTAINS)
└── jupyter/                           # 탐색용 노트북
```

---

## 주요 라이브러리 버전

Docker 이미지는 `python:3.12-slim`을 기준으로 빌드하며, 런타임 의존성은 `requirements.txt`에서 관리합니다.
현재 대부분의 패키지는 버전을 고정하지 않고 빌드 시점의 호환 가능한 최신 버전을 설치하며, `neo4j`만 `5.0.0` 이상으로 제한합니다.

| 패키지 | 버전 정책 | 용도 |
|--------|-----------|------|
| Python | `3.12` | 파이프라인 런타임 |
| `pandas` | unpinned | DataFrame 정제, 집계, 샘플링 |
| `pyahocorasick` | unpinned | KCIA 성분명 고속 다중 문자열 매칭 |
| `pyiceberg` | unpinned | Glue Catalog 연결, Iceberg 테이블 생성·조회·쓰기 |
| `duckdb` | unpinned | S3 JSON 로드, in-process SQL 조인, Neo4j CSV 생성 쿼리 |
| `boto3` | unpinned | S3 객체 조회·업로드, AWS SDK 연동 |
| `pyarrow` | unpinned | Iceberg 스키마 기준 Arrow Table 변환 |
| `s3fs` | unpinned | pandas/DuckDB 주변 S3 파일 접근 보조 |
| `neo4j` | `>=5.0.0` | Neo4j 연동 호환성 확보 |
| `ipykernel` | dev only, unpinned | 로컬 분석·노트북 커널 |
| `streamlit` | dev only, unpinned | 운영 UI 로컬 실행 |

분석·로컬 개발 시에는 `requirements-dev.txt`를 사용합니다. 재현 가능한 배포가 필요하면 운영 배포 시점의 lock file 또는 이미지 digest를 별도로 고정하는 것을 권장합니다.

---

## 설정 (config/settings.py)

설정값은 환경 변수 없이 코드에 하드코딩됩니다. EC2 IAM Role이 AWS 인증을 자동으로 처리합니다.

| 클래스 | 역할 |
|--------|------|
| `S3` | 버킷명, 리전, bronze/silver/gold/reference 경로 상수 |
| `OliveyoungIceberg` | `oliveyoung_db` 테이블명 상수 + `get_catalog()` |
| `INCIIceberg` | `inci_db` 테이블명 상수 + `get_catalog()` |
| `DataPath` | EC2 로컬 JSON 사전 파일 경로 |
| `DuckDB` | S3 읽기용 커넥션 헬퍼, bronze 파일 탐색 |

---

## 실행 방법

### Iceberg 테이블 초기화 (최초 1회)

```bash
# Silver
python silver_pipeline/create_silver.py
python silver_pipeline/create_category_master.py

# Reference
python reference_pipeline/create_reference_tables.py
python reference_pipeline/sync_reference_data.py

# Gold
python gold_pipeline/create_gold_tables.py
python gold_pipeline/create_gold_product_ingredients.py
```

### EC2에서 직접 실행

```bash
python reference_pipeline/sync_reference_data.py
python src/bronze_to_silver/main.py
python src/silver_to_gold/main.py
```

---

## Docker

### 이미지 빌드

```bash
docker build -t oliveyoung-pipeline .
```

### 컨테이너 실행

EC2 IAM Role 인증을 그대로 사용하기 위해 `--network host`로 실행합니다.

```bash
# Reference 테이블 초기 생성 (최초 1회 또는 신규 테이블 추가 시)
docker run --network host oliveyoung-pipeline create_reference_tables

# Reference 동기화
docker run --network host oliveyoung-pipeline sync_reference

# Bronze → Silver
docker run --network host oliveyoung-pipeline bronze_to_silver

# Silver → Gold
docker run --network host oliveyoung-pipeline silver_to_gold

# Silver → Neo4j CSV
docker run --network host oliveyoung-pipeline silver_to_neo4j_csv
```

전체 파이프라인 순서:

```bash
docker run --network host oliveyoung-pipeline sync_reference
docker run --network host oliveyoung-pipeline bronze_to_silver
docker run --network host oliveyoung-pipeline silver_to_gold
docker run --network host oliveyoung-pipeline silver_to_neo4j_csv
```

---

## Airflow 연동

DockerOperator 기반 DAG가 두 개로 분리되어 있습니다.

- `dags/oliveyoung_pipeline.py` — 메인 ETL 파이프라인
- `dags/oliveyoung_silver_to_neo4j_csv.py` — Neo4j CSV 익스포트 (초기 적재용, 독립 DAG)

```
oliveyoung_pipeline:
  sync_reference_data  →  bronze_to_silver  →  silver_to_gold

oliveyoung_silver_to_neo4j_csv:
  silver_to_neo4j_csv   (초기 적재용 — 필요 시 수동 트리거)
```

- `oliveyoung_pipeline`은 `schedule=None` — 크롤링 DAG 완료 후 `TriggerDagRunOperator`로 트리거됩니다.
- `oliveyoung_silver_to_neo4j_csv`는 초기 1회 적재 용도로, 트리거 없이 수동으로 실행합니다.

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_oliveyoung_pipeline",
    trigger_dag_id="oliveyoung_bronze_to_silver",
)
```

---

## 테이블 스키마

### Silver (`oliveyoung_silver_current` / `oliveyoung_silver_history`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `category_id` | string | 카테고리 식별자 (파티션 키) |
| `product_id` | string | 제품 고유 ID (UUID v5) |
| `product_brand` | string | 브랜드명 |
| `product_name` | string | 정제된 제품명 |
| `product_name_raw` | string | 원본 제품명 |
| `product_ingredients` | list\<string\> | KCIA 표준명으로 정규화된 성분 목록 |
| `product_ingredients_raw` | string | 원본 성분 문자열 |
| `rating` | float | 평점 |
| `review_count` | int | 리뷰 수 |
| `crawled_at` | timestamptz | 크롤링 시각 (정렬 키) |
| `batch_job` | string | 배치 작업 ID |
| `batch_date` | timestamptz | 배치 처리 시각 |

### Gold (`gold_product_ingredients`)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `ingredient_name` | string | KCIA 한글 표준명 |
| `inci_name` | string | INCI 표준명 (uppercase) |
| `kor_name` | string | INCI 한글명 |
| `eng_name` | string | 영문명 |
| `cosing_functions` | string | 기능 목록 (`;` 구분자) |
| `status` | string | Active / inactive |
| `cosmetic_restriction` | string | 화장품 규제 정보 |
| `other_restrictions` | string | 기타 규제 정보 |
| `usage_count` | long | 사용 프로덕트 수 |
| `batch_job` | string | 배치 작업 ID |
| `batch_date` | timestamptz | 배치 처리 시각 |

### Silver Error DLQ (`oliveyoung_silver_error`)

**오류 타입**: `INCOMPLETE_DATA_REJECTED` · `OPTION_BUNDLE_REJECTED` · `INVALID_METADATA_REJECTED` · `HETEROGENEOUS_BUNDLE_REJECTED` · `DUPLICATE_PRODUCT_REJECTED` · `UNMAPPED_RESIDUAL` · `HIDDEN_BUNDLE_REJECTED`

---

## Neo4j CSV 출력 형식

`silver_to_neo4j_csv` 파이프라인은 `neo4j-admin import` 포맷의 CSV를 S3에 업로드합니다.

**S3 경로 규칙:**
```
s3://oliveyoung-crawl-data/gold/neo4j/oliveyoung/{nodes|rels}/{Label}/{run_id}/
├── header.csv       # neo4j-admin 헤더 (1행)
└── part-00000.csv   # 데이터 행
```

### 노드

| 파일 | 헤더 | 데이터 소스 |
|------|------|------------|
| `nodes/Product/` | `product_id:ID(Product),product_name,brand,category` | `oliveyoung_silver_current` |

### 관계

| 파일 | 헤더 | 데이터 소스 |
|------|------|------------|
| `rels/CONTAINS/` | `:START_ID(Product),:END_ID(Ingredient)` | `silver_current` × `gold_product_ingredients` |

CONTAINS 관계는 `product_ingredients`(한국어 성분명)를 UNNEST해 `gold_product_ingredients`와 INNER JOIN, INCI 매핑이 없는 성분은 제외합니다.

---

## 인프라

| 항목 | 값 |
|------|-----|
| 스토리지 | AWS S3 (`oliveyoung-crawl-data`) |
| 메타스토어 | AWS Glue Catalog |
| oliveyoung_db 웨어하우스 | `s3://oliveyoung-crawl-data/olive_young_iceberg_metadata/` |
| inci_db 웨어하우스 | `s3://oliveyoung-crawl-data/inci_iceberg_metadata/` |
| 리전 | `ap-northeast-2` (서울) |
| 런타임 | Python 3.12, EC2 (IAM Role 기반 인증) |

---

## 주요 기능 설계

### 1. 최신 Bronze run 자동 선택
크롤링 결과는 서브카테고리와 run_id 단위로 S3에 누적됩니다. 파이프라인은 DuckDB/S3 glob 기반으로 최신 run_id 파일을 탐색해 입력을 구성하고, 동일 배치 안에서 처리된 레코드에 공통 batch metadata를 부여합니다.

### 2. Reference Data 기반 정제 정책
오타 사전, 제품명 정규화 규칙, 불량 키워드, 커스텀 성분 사전을 코드와 분리해 관리합니다. JSON으로 관리되는 룰은 `sync_reference_data.py`를 통해 Iceberg Reference 테이블에 반영되며, 파이프라인은 실행 시점의 Reference 테이블을 읽어 정제 정책으로 사용합니다.

### 3. 성분 표준화와 잔여 문자열 검증
성분 문자열은 노이즈 제거와 쉼표 마스킹을 거친 뒤 Aho-Corasick 오토마타로 KCIA 표준명에 매칭됩니다. 매칭되지 않은 잔여 문자열은 오류 후보로 분리해 `UNMAPPED_RESIDUAL` 등 DLQ 유형으로 추적할 수 있게 설계했습니다.

### 4. 제품 식별자 안정화
제품 ID는 브랜드명과 정제된 제품명을 기반으로 UUID v5를 생성합니다. 동일 제품이 재수집되어도 안정적인 product_id를 유지해 current/history 비교, CDC, Neo4j 관계 생성의 기준 키로 사용할 수 있습니다.

### 5. Current / History / Error 분리 저장
Silver는 최신 상태를 위한 `current`, 감사와 재처리를 위한 `history`, 품질 점검을 위한 `error`로 나뉩니다. `current`와 `error`는 overwrite로 현재 상태를 명확히 유지하고, `history`는 append로 배치별 이력을 누적합니다.

### 6. Gold 분석 테이블 생성
Silver의 unique 성분을 INCI Gold 테이블과 조인해 `gold_product_ingredients`를 만들고, 카테고리별 성분 빈도와 제품 변경 이력을 별도 Gold 테이블로 저장합니다. 분석·추천·검색·그래프 적재가 Silver 원본을 반복 스캔하지 않도록 목적별 산출물을 분리합니다.

### 7. CDC 기반 변경 추적
Iceberg snapshot과 Silver current 데이터를 활용해 제품 변경 로그를 생성합니다. 신규, 변경, 삭제 관점의 변경 이력을 Gold에 append하여 downstream 시스템이 전체 재처리 없이 변경분을 추적할 수 있게 합니다.

### 8. Neo4j import 파일 생성
Silver 제품 데이터와 Gold 성분 매핑을 조인해 `neo4j-admin import` 형식의 Product 노드와 CONTAINS 관계 CSV를 생성합니다. 헤더와 데이터 파일을 S3 run_id 경로로 분리해 그래프 DB 초기 적재와 재적재를 지원합니다.

### 9. Airflow / Docker 실행 단위 분리
Reference 동기화, Bronze→Silver, Silver→Gold, Silver→Neo4j CSV를 컨테이너 실행 모드로 분리했습니다. Airflow DAG는 주요 ETL 단계를 순차 실행하고, Neo4j CSV Export는 초기 적재 또는 필요 시 수동 실행할 수 있도록 독립 DAG로 관리합니다.

---

## 설계 결정 사항

### OliveyoungIceberg / INCIIceberg 분리
`oliveyoung_db`와 `inci_db`는 동일한 AWS Glue Catalog에 있지만, warehouse 경로와 소유 관심사가 다르므로 설정 클래스를 분리합니다.

### Current / History 이중 테이블
최신 데이터(`current`)와 시계열 감사 로그(`history`)를 분리. 재읽기 없이 시계열 추적 보장.

### Aho-Corasick 성분 매칭
`inci_db.silver_kcia_cosing_graphrag_current`(Iceberg)를 소스로, O(n+m) 복잡도로 다중 성분명 동시 탐색. 쉼표 마스킹으로 성분명 내 쉼표 포함 케이스 처리.

### DLQ 패턴 (Dead Letter Queue)
처리 실패 레코드를 `silver_error`로 분리 저장. 데이터 손실 없이 사후 재처리·분석 가능.

### Overwrite + Iceberg 스냅샷
`gold_product_ingredients`는 overwrite 방식으로 항상 최신 상태를 유지하며, 과거 시점 조회는 Iceberg 스냅샷 타임트래블로 처리합니다.

### Reference 테이블 분리 관리
오타 사전·불량 키워드·커스텀 성분 보정을 Git 관리 JSON으로 편집하고, `sync_reference_data.py`로 Iceberg에 동기화. 코드 재배포 없이 사전 업데이트 가능.
