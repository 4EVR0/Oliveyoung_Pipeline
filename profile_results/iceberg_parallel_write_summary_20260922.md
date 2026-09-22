# Iceberg Parallel Write Profiling Summary

## 실행 요약

- 실행 작업: `iceberg_bronze_to_silver`
- 실행 ID: `iceberg_bronze_to_silver_20260922_071646`
- 실행 결과: 성공
- 전체 소요 시간: `57.94s`
- 입력: `278개 S3 JSON 파일 -> 5348건 로드`
- 출력: `silver 3270건`, `error 2343건`
- 최대 RSS: `642.4 MB`

## CSV 산출물

원본 CSV는 생성 데이터 덤프이므로 Git에는 포함하지 않고 S3 경로만 기록합니다.

- Silver CSV: `s3://oliveyoung-crawl-data/data_csv/oliveyoung_silver_current_oliveyoung_silver_20260922_071741.csv`
- Error CSV: `s3://oliveyoung-crawl-data/data_csv/oliveyoung_silver_error_oliveyoung_silver_20260922_071741.csv`

## Iceberg Write 병렬화 결과

| stage | wall_s | cpu_s | cpu/wall | rows_out | 비고 |
| --- | ---: | ---: | ---: | ---: | --- |
| `iceberg_history_load_table` | `0.33` | `0.21` | `0.62` |  | `schema_evolved=false` |
| `iceberg_error_load_table` | `0.38` | `0.21` | `0.57` |  | `schema_evolved=false` |
| `iceberg_current_load_table` | `0.39` | `0.23` | `0.58` |  | `schema_evolved=false` |
| `pandas_to_arrow_error` | `0.44` | `0.41` | `0.93` | `2343` | Arrow 변환 |
| `pandas_to_arrow_history` | `0.58` | `0.51` | `0.88` | `3270` | Arrow 변환 |
| `pandas_to_arrow_current` | `0.56` | `0.52` | `0.92` | `3270` | Arrow 변환 |
| `iceberg_history_write` | `4.78` | `2.77` | `0.58` | `3270` | append |
| `iceberg_error_write` | `5.03` | `2.83` | `0.56` | `2343` | overwrite |
| `iceberg_current_write` | `5.16` | `2.73` | `0.53` | `3270` | overwrite |
| `iceberg_write` | `6.60` | `3.93` | `0.60` | `5613` | 병렬 wrapper |

## 이전 실행과 비교

| 항목 | 이전 순차 실행 | 병렬 실행 | 개선 |
| --- | ---: | ---: | ---: |
| `iceberg_write` | `17.08s` | `6.60s` | `-10.48s (-61.4%)` |
| 전체 시간 | `71.07s` | `57.94s` | `-13.13s (-18.5%)` |

## 해석

`current`, `history`, `error` 세 Iceberg 테이블 write를 병렬화하면서 commit 대기 시간이 누적되지 않고 겹쳐졌습니다. 개별 write는 `4.78s~5.16s`였지만 wrapper인 `iceberg_write`는 `6.60s`로 끝났습니다. 이제 가장 큰 병목은 다시 `bronze_load`이며, 이번 실행에서도 `278개 JSON`을 읽는 데 `34.47s`가 걸렸습니다.
