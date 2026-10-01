# Bronze to Silver Optimized 3-run Summary

## 실행 조건

- 실행일: `2026-10-01`
- 실행 명령: `docker run ... bronze_to_silver`
- 적용 옵션:
  - `BRONZE_LOAD_FORMAT=parquet`
  - `ICEBERG_PARALLEL_WRITES=1`
- 입력: 최신 Bronze run `253개 JSON 파일`, `4840 rows`
- 출력: `silver 2973 rows`, `error 2114 rows`
- 주의: 전체 파이프라인 3회 실행은 실제 Iceberg write와 S3 CSV 저장을 수행했다. `current`와 `error`는 overwrite, `history`는 append 방식이다.

## 전체 파이프라인 3회 평균

| stage | run1_s | run2_s | run3_s | avg_s | 비고 |
| --- | ---: | ---: | ---: | ---: | --- |
| `duckdb_connection` | `2.03` | `2.10` | `2.25` | `2.12` | DuckDB 연결 |
| `bronze_load` | `20.66` | `20.21` | `20.44` | `20.44` | Parquet materialize 후 read |
| `dictionary_load` | `6.06` | `4.97` | `5.06` | `5.36` | Iceberg reference dictionaries |
| `process_pipeline` | `5.14` | `4.99` | `4.97` | `5.03` | clean, dedup, ingredient match |
| `iceberg_write` | `6.61` | `6.74` | `6.82` | `6.73` | current/history/error 병렬 write |
| `csv_s3_write` | `2.34` | `1.97` | `1.89` | `2.06` | S3 CSV upload |
| `total` | `42.84` | `40.98` | `41.44` | `41.75` | end-to-end |

## Bronze load 포맷 3회 평균

이 표는 `scripts/benchmark_bronze_load_formats.py`를 3회 실행한 결과다. 같은 최신 Bronze file set `253 files / 4840 rows` 기준이며, cached read는 optimized 파일이 이미 존재한다고 보고 읽기만 측정한 값이다.

| stage | run1_s | run2_s | run3_s | avg_s | 비고 |
| --- | ---: | ---: | ---: | ---: | --- |
| `json_direct_read` | `35.53` | `18.75` | `17.35` | `23.88` | 253개 small JSON 직접 읽기 |
| `compacted_json_materialize` | `10.56` | `10.87` | `10.94` | `10.79` | single NDJSON 생성 |
| `compacted_json_cached_read` | `1.48` | `1.41` | `1.43` | `1.44` | single NDJSON 읽기 |
| `parquet_materialize` | `10.45` | `10.82` | `10.35` | `10.54` | Snappy Parquet 생성 |
| `parquet_cached_read` | `0.78` | `0.72` | `0.84` | `0.78` | single Parquet 읽기 |

## 요약

- 전체 파이프라인 평균은 `41.75s`였다.
- 두 최적화를 모두 켠 상태에서 평균 `bronze_load`는 `20.44s`, 평균 `iceberg_write`는 `6.73s`였다.
- Bronze 포맷만 따로 보면 direct JSON 평균 `23.88s` 대비 cached Parquet read 평균은 `0.78s`로 가장 빠르다.
- 현재 전체 파이프라인의 `bronze_load`는 매번 optimized Parquet을 다시 materialize하므로 cached Parquet read 시간보다 크다. optimized 파일 재사용 정책을 넣으면 입력 단계의 추가 단축 여지가 있다.
- Iceberg write는 current/history/error를 `ThreadPoolExecutor`로 병렬화해 세 테이블 commit 대기 시간을 겹치게 만든다.
