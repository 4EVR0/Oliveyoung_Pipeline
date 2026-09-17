# Bronze Format Benchmark Summary

## 실행 요약

- 입력 파일 수: `268`
- 입력 행 수: `5174`
- compacted JSON: `s3://oliveyoung-crawl-data/profile_results/bronze_format_benchmark/bronze_format_benchmark_20260917_032256/bronze_compacted.ndjson`
- compacted Parquet: `s3://oliveyoung-crawl-data/profile_results/bronze_format_benchmark/bronze_format_benchmark_20260917_032256/bronze_compacted.parquet`

## 읽기 성능 비교

| format | wall_s | cpu_s | cpu/wall | rows | file_size_mb | 비고 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| original_json_parts | `29.93` | `6.87` | `0.23` | `5174` | `` | 268 S3 JSON files |
| compacted_json | `1.87` | `1.35` | `0.72` | `5174` | `23.76` | single NDJSON file |
| parquet | `1.13` | `0.96` | `0.85` | `5174` | `4.14` | single Snappy Parquet file |

## 해석

현재 small JSON 방식은 268개 S3 JSON 파일에서 5174건을 읽는 데 29.93s가 걸렸다. compacted JSON은 1.87s, Parquet은 1.13s로 측정됐다. 파일 수를 줄이는 것만으로도 S3 object 요청과 JSON 파싱 오버헤드가 줄고, Parquet은 컬럼형 저장/압축 덕분에 Bronze->Silver 입력 포맷으로 가장 유리하다.

## 생성 비용

| stage | wall_s | cpu_s | size_mb |
| --- | ---: | ---: | ---: |
| `build_compacted_json` | `0.91` | `0.81` | `23.76` |
| `build_parquet` | `0.22` | `0.16` | `4.14` |
