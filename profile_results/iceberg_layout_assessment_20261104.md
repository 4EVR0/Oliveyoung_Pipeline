# Iceberg 데이터 배치 및 최적화 적합성 진단

진단일: 2026-11-04. 대상은 `oliveyoung_db.oliveyoung_silver_history`,
스냅샷은 `7648899446181468433`이다. 현재 상태에서는 추가 binpack의 우선순위가
낮고, 날짜 Sort는 저장 단위 설계와 함께 검토해야 한다. 상품 ID의 Bloom filter는
데이터 분포상 검토 근거가 있지만 현재 PyIceberg writer 경로의 지원이 선행 조건이다.

## 진단 방법과 범위

활성 파일 19개의 Parquet footer와 `category`, `product_id`, `batch_date`,
`crawled_at` 네 컬럼을 읽었다. PyIceberg의 snapshot 고정 파일 계획 결과,
Parquet row group min/max, 실제 일치 행을 비교했다. 운영 테이블의 속성,
파티션, 파일을 변경하거나 Athena 쿼리를 실행하지 않았다. 아래 후보 파일 수와
후보 파일 바이트는 Athena의 실제 스캔 바이트 또는 GET 횟수가 아니다.

- [원시 진단 JSON](iceberg_layout_audit_20261104_103608.json)
- [재현 코드](../scripts/audit_iceberg_layout.py)
- 실행: `docker run --rm --network host --env-file .env -v /home/jaewon/Oliveyoung_Pipeline:/app -w /app --entrypoint python oliveyoung-pipeline-local:latest scripts/audit_iceberg_layout.py`

## 방식별 판단

| 방식 | 사전에 확인할 지표 | 현재 관측 | 판단 |
| --- | --- | --- | --- |
| Binpack | 파티션별 파일 수와 크기 분포 | 19개 category에 파일 각 1개 | 추가 병합 우선순위 낮음 |
| 날짜 Sort | 조건 선택도, 파일·row group 범위, page index | 최신 날짜 행은 1.125%지만 17/19 파일이 후보. 파일마다 row group 1개, 조사 컬럼 page index 없음 | 단순 재정렬만으로 파일·row group pruning 개선 어려움. 저장 단위 변경까지 포함한 실험 필요 |
| 상품 ID Sort | 동등 조건에서 범위 통계가 남기는 후보 수 | 중간 분위 ID 3개는 실제 1개 파일에 있지만 19개 파일이 후보 | ID 범위 분리 가능성은 있으나 현재 파일·row group 단위가 고정되면 효과 제한 |
| Bloom filter | 동등 조회 패턴, 실제 없는 파일 수, writer/reader 지원 | 위 ID 3개에서 후보 19개 중 18개에 값이 없음 | 논리적 후보. 현재 PyIceberg writer 미지원과 Athena reader 활용 여부를 먼저 해결·검증 |
| 파티션 조정 | 실제 필터 패턴, 파티션별 행·용량 편차 | category 조건은 19개 중 1개로 정상 pruning | category 조회에는 유효. 날짜 파티션 추가는 작은 파일 재증가와 함께 검토 |

## 현재 파일 및 컬럼 분포

| 항목 | 관측값 |
| --- | ---: |
| 행 수 | 302,939 |
| 활성 파일 / row group | 19 / 19 |
| category 파티션 수 | 19 |
| 파티션별 파일 수 | 모두 1 |
| 활성 파일 합계 | 12,234,394 bytes / 약 11.67 MiB |
| 파일 크기 최소 / 중앙값 / 최대 | 39,736 / 264,666 / 2,423,037 bytes |
| 파일 크기 P95 | 약 2,219,729 bytes |
| 1 MiB 미만 파일 | 14개, 약 73.7% |
| product_id 고유값 수 | 4,530 |
| batch_date 고유 timestamp / UTC 날짜 수 | 101 / 65 |
| crawled_at이 널 제외 오름차순인 파일 | 18/19 |
| batch_date가 널 제외 오름차순인 파일 | 1/19 |
| product_id, batch_date, crawled_at의 column page index | 조사한 모든 row group에서 없음 |

작은 파일 비율 73.7%만 보고 재차 compaction을 결정하면 안 된다. 현재는 같은
category 안에 합칠 파일이 더 없고, 다른 category를 합치려면 파티션 설계를
바꿔야 한다. 고정된 큰 목표 파일 크기에 모든 파티션을 맞출 수는 없다.

## 대표 조건의 Pruning 진단

상품 ID는 정렬된 고유 ID의 최소, 25%, 50%, 75%, 최대 위치에서 하나씩 골랐다.
실제 쿼리 이력을 표본화한 것은 아니며, 5개 결과를 전체 조회의 효과로 일반화하지 않는다.
전체 UUID는 원시 JSON에 있으며 아래는 식별용 앞부분이다.

| 조건 | 파일 후보 / 19 | 후보 내 row group 후보 | 실제 일치 파일 | 실제 일치 행 |
| --- | ---: | ---: | ---: | ---: |
| ID 최소값 `0004f92b…` | 1 | 1 | 1 | 1 |
| ID 25% `42a6ed45…` | 19 | 19 | 1 | 46 |
| ID 50% `81a94b37…` | 19 | 19 | 1 | 92 |
| ID 75% `c03412d0…` | 19 | 19 | 1 | 2 |
| ID 최대값 `ffe83be9…` | 1 | 1 | 1 | 92 |
| 값 범위 내부에 만든 미존재 ID | 19 | 19 | 0 | 0 |
| 최신 UTC 날짜 `2026-11-01` | 17 | 17 | 17 | 3,408 |
| category = 크림 | 1 | 1 | 1 | 57,056 |
| category = 크림 + 최신 UTC 날짜 | 1 | 1 | 1 | 558 |

ID 중간값 3개는 후보 파일 총 바이트가 전체 12,234,394 bytes로 남는다.
Bloom filter가 생성되고 reader가 활용한다면, 이 중 실제 값이 없는 18개
row group을 추가 제외할 여지가 있다. 18/19라는 비율은 후보 개수의 이론적
여지이며, 거짓 양성·Bloom 자체 읽기 비용·컬럼 크기 차이를 고려한 시간 또는
비용 개선율은 아니다. 미존재 ID에는 진단용 suffix를 붙였으며 실제 업무 요청은 아니다.

날짜 조회는 전체 행의 1.125%만 필요하지만 후보 파일 바이트가 12,077,838 bytes로
전체의 약 98.7%다. 다만 실제로도 해당 날짜가 17개 파일에 존재한다. 같은 행을
같은 파일과 같은 row group에 둔 채 정렬만 바꿔서는 이 단위의 min/max를 좁힐 수
없다. 더 작은 row group, page index와 reader 지원, 또는 날짜 기준 데이터 분배를
같이 실험해야 한다. 분할을 늘리면 파일·메타데이터 처리 부담이 커지는 반대 효과도 있다.

## 설정과 엔진 제약

| 항목 | 확인 상태 | 의미 |
| --- | --- | --- |
| Iceberg table properties | `{}` | 통계·Bloom·목표 파일 크기 속성을 명시하지 않음. 기본 통계가 없다는 뜻은 아님 |
| 파티션 | category identity | category 조건에서 파일 계획상 정상 pruning 확인 |
| 정렬 메타데이터 | crawled_at ASC NULLS LAST | batch_date 또는 product_id 정렬 설정은 아님 |
| PyIceberg | 0.11.1 | 설치본의 writer 코드에서 Bloom 속성 미구현 확인 |
| PyArrow | 24.0.0 | write_table에 bloom_filter_options 인자는 존재 |
| 실제 파일의 Bloom 존재 여부 | 미확인 | 사용한 PyArrow column metadata API에 Bloom 노출 속성 없음 |
| Athena의 해당 파일 Bloom 활용 | 미검증 | 별도 writer/reader 조합의 A/B 확인 필요 |

설치본 `pyiceberg.io.pyarrow._get_parquet_writer_kwargs()`는
`write.parquet.bloom-filter-max-bytes`와
`write.parquet.bloom-filter-enabled.column.*`에 대해
`Parquet writer option(s) ... not implemented` 경고를 내도록 되어 있다.
즉, PyArrow 자체 API에 인자가 있어도 현재 PyIceberg 경로에서는 테이블 속성만
추가해 생성된다고 볼 수 없다. 기존 파일에 Bloom이 없다고 단정한 것은 아니다.

Iceberg의 `write.metadata.metrics.column.<컬럼>`은 수집 통계의 범위를 조정하는
설정이다. `full` 통계를 수집해도 파일 내용이 넓은 범위를 포함하면 후보가 남을 수
있으며, 통계 설정 변경이 물리적 Sort 또는 Bloom을 대신하지 않는다.

## 적용 순서

1. 실제 Athena 쿼리 이력에서 전체 집계, 날짜 범위, ID 동등 조회의 빈도를 확인한다.
2. 현재 binpack 결과를 기준선으로 유지하고, 파티션별 파일 누적을 관찰한다.
3. 날짜 조회가 중요하면 복제 실험 테이블에서 batch_date 정렬과 row group/page 설정을 함께 비교한다.
4. ID 동등 조회가 중요하면 Bloom을 생성할 writer를 선정하고 Athena가 활용하는지 확인한다.
5. 검증 시 동일 데이터·동일 SQL로 스캔 바이트, 계획/실행 시간, GET 관측값, 재작성 비용을 비교한다.

추가 Sort/Bloom 적용이나 운영 테이블의 설정 변경은 수행하지 않았다.
이 리포트는 배치 적합성 진단이며, 신규 Sort/Bloom의 실제 Athena 성능 개선을
측정한 보고서는 아니다. 이전 binpack의 실측 효과는
[별도 전후 보고서](athena_compaction_20261103_summary.md)에 있다.

## 운영에서 다음에 할 일

현재 history의 활성 데이터는 약 11.67 MiB이고 파일은 19개다. 현재 규모에서
추가 저장 엔진을 도입하거나 매 배치마다 재작성하기보다는 기존 binpack 효과를
유지하고 실제 사용 쿼리에서 다음 병목을 확인하는 것이 우선이다.

| 순서 | 실행할 일 | 다음 단계로 넘어갈 근거 |
| --- | --- | --- |
| 1 | 기존 compaction 결과 유지, 추가 Sort/Bloom 설정은 보류 | 현재 조회의 불편이나 목표 지연을 먼저 정의 |
| 2 | Athena 사용 쿼리를 전체 집계 / category / 날짜 / ID 조회로 분류하고 빈도와 실행 통계 기록 | 자주 실행되며 실제 비용·지연을 만드는 패턴 확인 |
| 3 | 파티션별 파일 수·크기 분포와 대표 쿼리 GET·실행 시간 모니터링 | 같은 파티션에 작은 파일이 재누적되고 조회 부담도 증가하면 선택적으로 binpack |
| 4A | 날짜 조회가 주력이면 복제 테이블에서 batch_date 정렬 + 저장 단위 변경을 비교 | 실제 스캔량·지연 감소가 재작성 및 파일 수 증가 비용을 상회 |
| 4B | ID 조회가 주력이면 생성 가능한 writer와 Athena reader의 Bloom 활용부터 소규모 검증 | 값이 없는 row group의 읽기 감소가 실측으로 확인됨 |

4A와 4B는 둘 다 바로 수행할 작업이 아니라 실제 쿼리 사용 패턴에 따른 분기다.
전체 집계나 category 조회가 주력이면 현재 상태를 유지하는 선택도 타당하다.
날짜 파티션을 바로 추가하면 65일치 데이터가 더 작은 파티션들로 분산되므로,
파일 수가 다시 증가하는 비용을 먼저 계산해야 한다.

후속 실험은 운영 테이블의 물리 배치를 반복해서 바꾸기보다 동일 스냅샷의
복제 테이블에서 수행한다. 쿼리 결과 일치, 결과 재사용 비활성화, 동일한 워밍업과
반복 횟수를 맞춘다. 최소 비교 항목은 스캔 바이트, 계획/엔진/전체 시간,
분리한 시간 구간의 GET 관측값, 파일·row group 수, 재작성 비용이다.
특정 상품을 자주 찾고 category도 아는 호출이라면 category 조건을 함께 넘기는
방법도 먼저 검토할 수 있다. 잘못된 category를 붙이면 결과가 달라지므로,
호출 측이 정확한 category를 확보한 경우에만 적용한다.

## 참고 자료

- [Iceberg 파일 메타데이터](https://iceberg.apache.org/docs/latest/spark-queries/#files)
- [Iceberg 통계·Bloom 설정](https://iceberg.apache.org/docs/latest/configuration/)
- [Parquet Bloom filter](https://parquet.apache.org/docs/file-format/bloomfilter/)
- [PyIceberg writer API 및 소스](https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/)

설치본 지원 상태는 공개 문서의 최신 버전과 구분하여 실제 0.11.1 소스를 확인했다.
