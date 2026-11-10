# Iceberg Sort와 Binpack 실측 비교

같은 최신 history 스냅샷으로 6개 실험 테이블을 구성하고 Athena에서 6종 조회를
각각 3회 실행했다. 날짜 정렬은 최신 하루 조회의 스캔량을 binpack보다 82.4%
줄였지만 평균 전체 시간 감소는 3.6%였다. 작은 row group을 추가하면 스캔량은
더 줄어도 지연과 다른 조회의 읽기량이 악화됐다. 현재 데이터에서는 날짜 정렬과
큰 row group 조합이 후속 검증 후보이며, 작은 row group을 일괄 적용할 근거는 없다.

## 사전 판단 지표 재확인

| 항목 | 이번 스냅샷 |
| --- | ---: |
| Source snapshot | `7389069542196458125` |
| 행 수 | 309,755 |
| 활성 파일 수 | 53 |
| 파일 크기 합계 | 15,374,750 bytes |
| 파일 크기 중앙값 | 103,438 bytes |
| 1 MiB 미만 파일 | 48/53, 약 90.6% |
| category 파티션 | 19개 |
| 파티션별 파일 수 | 17개 category에 각 3개, 나머지 2개에 각 1개 |
| product_id 고유값 수 | 4,548 |
| batch_date 고유 timestamp 수 | 103 |

이전 2026-11-04 진단 이후 append가 발생해 파일이 19개에서 53개로 늘었다.
따라서 과거 성능 수치와 직접 비교하지 않고 최신 스냅샷의 복제본에서 binpack을
다시 실행했다. 같은 파티션 안의 작은 파일 누적이라는 binpack 적용 근거를 확인했다.
날짜/ID는 실제 WHERE 조건으로 비교하고 파일 및 row group의 min/max를 원시
JSON에 기록했다. 이 날짜/ID 쿼리들은 실제 업무 빈도 표본이 아닌 대표 실험 조건이다.

## 실험 설계

운영 테이블에 쓰지 않고 `oliveyoung_db.bench_sort_20261110_013633_*` 6개와
`s3://oliveyoung-crawl-data/benchmark/iceberg_sort/20261110_013633/` 아래에 구성했다.
운영 history 스냅샷은 실험 시작과 종료 시 동일했다.

| 코드 이름 | 물리 배치 | 파일 수 | Row group 수 | 활성 bytes |
| --- | --- | ---: | ---: | ---: |
| binpack | 복제본에 실제 Athena BIN_PACK 실행 | 19 | 19 | 12,569,775 |
| rewrite_large | 비정렬 PyArrow 재작성 대조군 | 19 | 19 | 8,388,653 |
| date_large | batch_date, product_id 오름차순 | 19 | 19 | 7,576,334 |
| rewrite_small | 비정렬, row group 4,096행 | 19 | 88 | 18,328,468 |
| date_small | 날짜 정렬, row group 4,096행 | 19 | 88 | 20,014,882 |
| id_small | product_id, batch_date 정렬, row group 4,096행 | 19 | 88 | 7,247,566 |

PyArrow 배치들은 같은 writer, ZSTD level 3, page index 비활성화, Bloom 미설정,
동일 category 파티션을 사용했다. 4,096행은 실험 변수이며 운영 권장값이 아니다.
정렬은 PyArrow sort를 사용한 물리 배치 실험이다. Spark
`rewrite_data_files(strategy='sort')`의 구현 또는 작업 실행시간 비교는 아니다.
Athena binpack과 PyArrow의 writer 차이를 분리하기 위해 rewrite_large와
rewrite_small 대조군을 추가했다. 모든 테이블은 full Iceberg file metrics를 수집한다.

조회 조건은 최신 UTC 하루 2026-11-07, 최근 7일 2026-11-01~07, 상품 ID
`81db6ba2-a486-5c67-abd4-6fc6009f55f1`, category 미스트를 사용했다.
각 SQL은 category별 행 수, 상품명 길이 합계, review_count 합계를 계산한다.
기존 compaction 보고서와 SELECT 컬럼이 달라 과거 스캔량과 직접 비교하면 안 된다.

## Binpack 대비 주요 결과

값은 워밍업을 제외한 3회 평균이다. 스캔량은 해당 반복에서 일정했다.

| 조회 | 비교 배치 | Binpack bytes | 비교 bytes | 스캔 감소 | Binpack 전체 시간 | 비교 전체 시간 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 전체 집계 | date_large | 641,719 | 584,523 | 8.9% | 0.962초 | 0.913초 |
| 최신 하루 | date_large | 840,984 | 148,189 | 82.4% | 0.886초 | 0.854초 |
| 최근 7일 | date_large | 840,984 | 327,663 | 61.0% | 0.877초 | 0.781초 |
| 상품 ID | id_small | 132,934 | 37,204 | 72.0% | 0.854초 | 0.880초 |
| category + 최신 하루 | date_large | 25,980 | 1,809 | 93.0% | 0.825초 | 0.847초 |
| category 단독 | date_large | 17,439 | 15,948 | 8.5% | 1.001초 | 1.006초 |

스캔량 감소가 시간 감소로 이어지지 않는 사례가 있다. 특히 category + 하루는
93% 적게 스캔했지만 평균 시간은 소폭 증가했다. 작은 데이터와 3회 표본에서
큐/계획/서비스/엔진 변동을 통계적으로 분리할 수 없으므로 확정적 지연 개선을
주장하지 않는다. 원시 기록에는 개별 시간과 중앙값도 있다.

## 정렬과 Row Group 크기 효과 분리

아래는 같은 PyArrow writer 사이 비교다. 표의 감소율이 음수이면 증가를 뜻한다.

| 비교 | 조회 | 대조 bytes | 적용 bytes | 해석 |
| --- | --- | ---: | ---: | --- |
| rewrite_large → date_large | 최신 하루 | 835,444 | 148,189 | 정렬만으로 약 82.3% 감소 |
| rewrite_small → date_small | 최신 하루 | 337,670 | 109,852 | 작은 group 고정 시 정렬로 약 67.5% 감소 |
| rewrite_large → rewrite_small | 최신 하루 | 835,444 | 337,670 | 비정렬에서 group 세분화로 약 59.6% 감소 |
| date_large → date_small | 최신 하루 | 148,189 | 109,852 | 추가 스캔 감소는 약 25.9%, 평균 시간은 0.854 → 1.742초 |
| rewrite_small → id_small | 상품 ID | 607,315 | 37,204 | 작은 group 고정 시 ID 정렬로 약 93.9% 감소 |
| rewrite_small → date_small | 상품 ID | 607,315 | 684,398 | 다른 조회에 맞춘 정렬이 오히려 약 12.7% 증가 |

최신 하루에서 date_small은 binpack 대비 86.9% 적게 읽지만 평균 시간은
0.886초에서 1.742초로 약 96.7% 증가했다. 전체 집계 스캔량도 641,719 bytes에서
1,239,411 bytes로 약 93.1% 증가했고, 활성 파일 크기도 12,569,775 bytes에서
20,014,882 bytes로 증가했다. 파일 수만 같은 채 row group을 늘려도 비용이 생긴다.

상품 ID 정렬도 모든 조회에 좋은 것은 아니다. id_small은 ID 스캔량이 작지만,
날짜 정렬 배치보다 최신 하루 스캔량이 크다. 주요 WHERE 조건과 정렬 키의
일치 여부가 중요하다.

## 사전 가정에서 수정할 점

이전에 파일/row group이 하나이면 정렬에 따른 해당 단위의 min/max pruning
여지가 작다고 판단했다. 이 부분은 맞지만, 그것만으로 실제 스캔 바이트 개선이
작다고 예측하면 안 된다. date_large와 rewrite_large는 파일 19개, row group
19개로 같고 page index도 꺼져 있지만 날짜 조건의 스캔 바이트는 크게 달랐다.

정렬은 압축과 인코딩 배치 및 reader의 필터 처리에도 영향을 줄 수 있다.
이번에는 page별 I/O를 추적하거나 각 원인을 분리한 실험까지 하지 않았으므로
82.3% 감소를 특정 pruning 단계 하나의 효과라고 단정하지 않는다.
사전 판단 지표는 후보를 고르는 근거이며, 실제 스캔량 측정을 대신하지 않는다.

## 검증과 실행 비용

- 모든 배치의 행 수는 309,755로 동일하다.
- 전체 행을 JSON 직렬화한 SHA256과 중복 수를 집계한 multiset fingerprint가 6개 모두 원본과 일치했다.
- 6종 조회의 모든 배치/반복 결과 해시가 일치했다.
- Athena 실행은 setup 2건 + 워밍업 36건 + 측정 108건, 총 146건이다.
- 모든 측정은 각 실험 테이블의 snapshot ID로 고정했고 결과 재사용을 껐다.
- 배치 실행 순서를 고정 seed로 회차별 섞었다. 입력/출력 검증은 조회 측정에서 제외했다.
- 복제본 BIN_PACK 자체의 Athena 전체 시간은 6.953초, 스캔량은 10,508,561 bytes였다.
- 실험 binpack의 rewrite 파일 수 기준은 2로 낮췄다. 운영 테이블 속성은 변경하지 않았다.
- 준비 도중 Python 행 변환 검증을 DuckDB 해시 집계로 바꾸고 체크포인트에서 재개했다.
- 따라서 JSON의 build_seconds는 일부 재개 구간만 포함하며 전략별 재작성 비용 순위를 매기는 데 사용하지 않는다.
- 관련 로컬 테스트 8개와 문법 검사가 통과했다.

GET 요청 수는 이번 별도 실험 prefix에서 측정하지 않았다. JSON의 Athena API 호출
수와 S3 GET은 다르다. Athena 최소 과금 등의 정책 때문에 스캔 감소율을 그대로
청구액 감소율로 해석하지 않는다. 실험 테이블과 데이터는 검토를 위해 남겨뒀으며
운영 데이터나 과거 snapshot을 삭제하지 않았다.

## 다음 운영 판단

1. 전체 집계와 category 조회 중심이면 binpack을 기본 유지한다.
2. 날짜 조회가 자주 실행되면 date_large를 우선 검증 후보로 삼는다. 이번 스캔 감소는 명확하지만 지연 이득은 작거나 변동 범위 안이다.
3. row group 4,096행을 일괄 적용하지 않는다. 이번에는 날짜 조회 시간·전체 스캔량·파일 크기 측면의 손해가 확인됐다.
4. 상품 ID 조회가 중요하면 id_small을 별도 후보로 보고 여러 ID와 더 많은 반복으로 검증한다. 이번 ID는 하나라 일반화하지 않는다.
5. 실제 쿼리 빈도에 따라 가중한 스캔량·지연과 재작성/저장 비용을 평가한 뒤 운영 적용을 결정한다.

## 결과물

- [자동 생성 전체 비교표](iceberg_sort_comparison_20261110_013633.md)
- [원시 SQL·Query ID·통계·파일 범위](iceberg_sort_comparison_20261110_013633.json)
- [실험 코드](../scripts/benchmark_iceberg_sort.py)
- [재현 방법과 배치별 통제 조건](../scripts/iceberg_sort_benchmark.md)
- [검증 테스트](../scripts/test_iceberg_sort.py)

참고: [Iceberg 파일 등록](https://py.iceberg.apache.org/api/#add-files),
[Parquet writer 옵션](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html),
[Athena OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html).
