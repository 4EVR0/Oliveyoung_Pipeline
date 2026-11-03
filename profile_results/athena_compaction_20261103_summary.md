# Athena silver_history Compaction 성능 비교

2026-11-03 서울 리전의 `oliveyoung-ops` 워크그룹(Athena engine version 3)에서
`oliveyoung_db.oliveyoung_silver_history`를 BIN_PACK으로 compaction했다.
동일 SQL을 전후 각각 3회 실행한 결과, 평균 조회 시간은 2.179초에서 1.295초로
40.6% 감소했고 스캔량은 83.1%, 해당 측정 시간대의 S3 GET 관측값은 85.0% 감소했다.

## 결과

| 지표 | 개선 전 | 개선 후 | 감소율 |
| --- | ---: | ---: | ---: |
| 평균 전체 조회 시간 | 2.179초 | 1.295초 | 40.6% |
| 평균 엔진 실행 시간 | 2.019초 | 1.145초 | 43.3% |
| 평균 계획 시간 | 0.416초 | 0.169초 | 59.5% |
| 회당 스캔 바이트 | 47,104,552 | 7,981,509 | 83.1% |
| 활성 Parquet 파일 수 | 1,722개 | 19개 | 98.9% |
| 활성 데이터 바이트 | 141,573,089 | 12,234,394 | 91.4% |
| 활성 데이터 크기 | 135.01 MiB | 11.67 MiB | 91.4% |
| 전체 행 수 | 302,939행 | 302,939행 | 동일 |
| S3 GET 관측값: 3회 조회 구간 합계 | 6,269회 | 939회 | 85.0% |
| 클라이언트 Athena API 호출: 3회 합계 | 20회 | 17회 | 참고 지표 |

계획 시간은 엔진 실행 시간에 포함된다. 클라이언트 API 호출 수는 polling 주기와
실행 시간에 영향을 받으며 S3 GET 횟수와 다른 지표다.
활성 데이터 크기는 현재 스냅샷이 참조하는 파일의 합이며, 과거 스냅샷을 포함한
S3 전체 사용량이 이만큼 줄었다는 뜻은 아니다.

## 회차별 조회

| 회차 | 개선 전 전체 시간 | 개선 후 전체 시간 | 개선 전 스캔 바이트 | 개선 후 스캔 바이트 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1.504초 | 1.277초 | 47,104,552 | 7,981,509 |
| 2 | 3.458초 | 1.641초 | 47,104,552 | 7,981,509 |
| 3 | 1.575초 | 0.967초 | 47,104,552 | 7,981,509 |
| 평균 | 2.179초 | 1.295초 | 47,104,552 | 7,981,509 |
| 중앙값 | 1.575초 | 1.277초 | 47,104,552 | 7,981,509 |

전후 각각 워밍업 1회를 제외했고 Athena 결과 재사용은 비활성화했다.
개선 전 2회차의 실행 시간이 상대적으로 길어 평균 개선율에 영향을 준다.
중앙값 기준 조회 시간 감소는 18.9%이며, 3회 표본을 일반적인 성능 보장으로
해석하면 안 된다. Athena 내부 캐시나 공유 실행 환경을 완전히 통제하지는 않았다.

## S3 요청 측정

- 버킷: `oliveyoung-crawl-data`
- 필터 ID: `SilverHistoryMetrics`
- 실제 적용 prefix: `silver/history/`
- 메트릭: CloudWatch `AWS/S3`, `GetRequests`, `Sum`, 60초 단위
- 개선 전: `2026-11-03 04:47:00~04:48:00 UTC` / `13:47~13:48 KST`
- 개선 후: `2026-11-03 04:52:00~04:53:00 UTC` / `13:52~13:53 KST`

각 구간에는 측정 대상 SELECT 3회가 들어간다. 이 스크립트의 워밍업, 파일 구성
조회, 활성화 확인용 1바이트 GET, OPTIMIZE는 위 구간 밖에서 실행했다.
GET 값은 개별 쿼리 귀속 수치가 아닌 prefix 전체의 관측값이다. 같은 시간대의 다른
reader는 포함될 수 있으며, CloudWatch의 best-effort 수집 및 게시 지연 한계가 있다.
초기 미수집 값을 0으로 처리하지 않고 실제 데이터 포인트를 기다렸다.

## 실행한 개선

```sql
OPTIMIZE oliveyoung_db.oliveyoung_silver_history
REWRITE DATA USING BIN_PACK;
```

- OPTIMIZE Query ID: `6d67096d-74e6-4836-a908-b5cc772ec880`
- OPTIMIZE 전체 시간: `13.444초`
- OPTIMIZE 스캔 바이트: `126,941,534` (약 121.06 MiB)
- 이전 스냅샷: `5281192804406411760`
- 이후 스냅샷: `7648899446181468433`

기존 category 파티션 안의 작은 파일들을 병합했다. 행 삭제, 중복 제거, 스냅샷 만료는
수행하지 않았다. 전후 행 수 및 측정 SQL의 집계 결과 해시가 일치했다. 이 집계 검사는
전체 행의 완전한 동일성 증명은 아니다. 전후 대표 파일 한 개씩의 압축 코덱은 모두
ZSTD로 확인했다. 파일 병합으로 재인코딩과 압축 단위도 바뀌므로, 측정된 효과에는
파일 접근 감소와 물리 데이터 크기 감소가 함께 포함된다.

## 코드와 원시 기록

- [실행 코드](../scripts/benchmark_athena_compaction.py): 같은 SQL의 before/after 실행, OPTIMIZE, 통계 수집
- [실행 및 재현 방법](../scripts/athena_compaction_benchmark.md)
- [원본 JSON](athena_compaction_20261103_044230.json): SQL, 모든 Query ID, 원시 실행 통계, GET 데이터 포인트, 파일 구성
- [자동 생성 상세 표](athena_compaction_20261103_044230.md)
- [지표 해석 검사](../scripts/test_athena_compaction.py): 4개 테스트 통과

이번 기록 이전의 지표 활성화 대기 및 중단된 baseline은 최종 비교에서 제외했다.
OPTIMIZE SQL 제출 시 처음 사용한 식별자 표기가 거절되어 공식 예제 표기로 수정한
뒤 baseline 기록을 재사용했다. 성공한 OPTIMIZE 실행은 위 Query ID의 한 번이다.

## 해석

현재 history의 작은 파일 문제는 이 조회에서 실제 지연 및 스캔량 증가와 관련이
있었다. 다만 이번 SQL은 전체 history를 category별로 집계하는 형태다. 날짜 범위나
특정 상품 조회까지 같은 개선율이라고 판단할 수는 없다.
Athena의 실제 청구 스캔량에는 최소 과금 및 반올림 정책이 적용되므로 위 83.1%를
그대로 청구액 감소율로 환산하지 않는다. OPTIMIZE 자체의 실행 비용도 별도다.

참고: [Athena OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html),
[실행 통계](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html),
[S3 요청 메트릭](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html).
