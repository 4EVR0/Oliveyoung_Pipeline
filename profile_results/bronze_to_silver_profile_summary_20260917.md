# Bronze -> Silver Profiling Summary

## 실행 요약

- 실행 작업: `iceberg_bronze_to_silver`
- 실행 ID: `iceberg_bronze_to_silver_20260917_020429`
- 실행 결과: 성공
- 전체 소요 시간: `71.07s`
- 입력: `279개 S3 JSON 파일 -> 5373건 로드`
- 출력: `silver 3306건`, `error 2333건`
- 최대 RSS: `657.6 MB`

## 주요 병목

| stage | wall time | 전체 비중 | 해석 |
| --- | ---: | ---: | --- |
| `bronze_load` | `33.70s` | `47.4%` | `279개 S3 JSON 파일 -> 5373건 로드`; 작은 JSON 파일 다수 읽기와 S3/DuckDB httpfs 대기 비용이 큼 |
| `iceberg_write` | `17.08s` | `24.0%` | Iceberg current/history/error 쓰기와 S3 commit/metadata 작업 비용 |
| `process_pipeline` | `7.84s` | `11.0%` | 정규식/문자열 정제, 중복 제거, 성분 매칭 처리 |
| `dictionary_load` | `6.27s` | `8.8%` | KCIA/typo/garbage 사전 로드와 Aho-Corasick 빌드 |
| `csv_s3_write` | `2.70s` | `3.8%` | CSV 변환 및 S3 업로드 |

## 단계별 상세

| stage | wall_s | cpu_s | cpu/wall | rows_in | rows_out | peak_rss_mb |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `duckdb_connection` | `3.49` | `2.33` | `0.67` |  |  | `248.0` |
| `bronze_load` | `33.70` | `8.23` | `0.24` |  | `5373` | `325.8` |
| `dictionary_load` | `6.27` | `2.59` | `0.41` |  |  | `373.0` |
| `clean_rows` | `5.61` | `4.64` | `0.83` | `5373` | `5373` | `376.8` |
| `dedup` | `0.39` | `0.19` | `0.48` | `4779` | `4779` | `423.0` |
| `ingredient_match` | `1.42` | `0.98` | `0.69` | `3385` | `3651` | `431.3` |
| `build_output_dataframes` | `0.40` | `0.28` | `0.71` | `5639` | `5639` | `439.2` |
| `process_pipeline` | `7.84` | `6.11` | `0.78` | `5373` | `5639` | `439.2` |
| `pandas_to_arrow_current` | `0.31` | `0.15` | `0.50` | `3306` | `3306` | `445.7` |
| `iceberg_current_write` | `6.36` | `1.32` | `0.21` | `3306` | `3306` | `559.9` |
| `pandas_to_arrow_history` | `0.30` | `0.15` | `0.48` | `3306` | `3306` | `564.4` |
| `iceberg_history_write` | `3.16` | `0.64` | `0.20` | `3306` | `3306` | `595.9` |
| `pandas_to_arrow_error` | `0.16` | `0.11` | `0.71` | `2333` | `2333` | `597.6` |
| `iceberg_error_write` | `4.07` | `0.80` | `0.20` | `2333` | `2333` | `612.5` |
| `iceberg_write` | `17.08` | `3.71` | `0.22` | `5639` | `5639` | `612.5` |
| `csv_silver_prepare` | `0.08` | `0.06` | `0.74` | `3306` | `3306` | `612.9` |
| `csv_silver_upload` | `1.70` | `0.50` | `0.29` | `3306` | `3306` | `657.6` |
| `csv_error_upload` | `0.92` | `0.21` | `0.23` | `2333` | `2333` | `657.6` |
| `csv_s3_write` | `2.70` | `0.77` | `0.28` | `5639` | `5639` | `657.6` |
| `total` | `71.07` | `23.75` | `0.33` |  | `5639` | `657.6` |

## 해석

이번 실행의 최우선 병목은 전처리 CPU가 아니라 Bronze 로드입니다. `279개 S3 JSON 파일`에서 `5373건`만 읽는데 `33.70초`가 걸렸고, `cpu/wall=0.24`라 CPU 계산보다 S3 object 요청, 네트워크 대기, DuckDB httpfs JSON 읽기 비용이 큰 상태입니다.

두 번째 병목은 Iceberg write입니다. Pandas -> Arrow 변환은 각 단계가 `0.16s~0.31s` 수준이라 크지 않고, 실제 시간은 `iceberg_current_write`, `iceberg_history_write`, `iceberg_error_write`에서 발생합니다. 즉 Parquet/S3 쓰기, Iceberg commit, metadata 작업 비용이 큽니다.

전처리 내부에서는 `clean_rows`가 `5.61초`, `cpu/wall=0.83`으로 가장 CPU-heavy합니다. 정규식과 문자열 정규화 비용이 주된 이유지만, 전체 시간 기준으로는 Bronze 로드와 Iceberg write보다 후순위입니다.

## 개선 우선순위

1. Bronze JSON small file 문제 완화
   - 현재: `279개 S3 JSON 파일 -> 5373건 -> 33.70s`
   - 추천: run 단위 Parquet 또는 compacted JSON 산출물 생성

2. Iceberg write 비용 분리 개선
   - current/history/error 3회 write로 총 `17.08s`
   - current overwrite와 history append 정책, commit 빈도, 파일 크기 확인 필요

3. 전처리 CPU 최적화
   - `clean_rows`가 내부 CPU 병목
   - 정규식 호출 수, 문자열 치환 순서, 불필요한 반복 처리 확인
