# Athena History Compaction Benchmark

- Workgroup: `oliveyoung-ops`
- Table: `oliveyoung_db.oliveyoung_silver_history`
- Each phase has one excluded warmup; result reuse disabled.
- Aggregate results match: `True`
- S3 GET: see prefix-level metric windows below; separate from client API calls.

| Phase | Run | Scanned bytes | Total ms | Engine ms | Planning ms | Queue ms | API calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| before | 1 | 47104552 | 1504 | 1339 | 323 | 99 | 6 |
| before | 2 | 47104552 | 3458 | 3261 | 649 | 97 | 8 |
| before | 3 | 47104552 | 1575 | 1457 | 277 | 54 | 6 |
| before | mean | 47104552.00 | 2179.00 | 2019.00 | 416.33 | 83.33 | |
| after | 1 | 7981509 | 1277 | 1107 | 159 | 104 | 6 |
| after | 2 | 7981509 | 1641 | 1487 | 197 | 72 | 6 |
| after | 3 | 7981509 | 967 | 841 | 150 | 55 | 5 |
| after | mean | 7981509.00 | 1295.00 | 1145.00 | 168.67 | 77.00 | |

Raw execution statistics, query IDs, SQL, layout counts and OPTIMIZE cost are in the companion JSON.
Aggregate equality is a sanity check, not a full row-level equality proof.
Run during a pause in ingestion; concurrent writes invalidate the comparison.
Compaction preserves rows, adds a snapshot, and does not expire historical files.

## S3 GET Observations

CloudWatch best-effort prefix totals during isolated measurement minutes; not query-attributed. Other readers of this prefix may contribute. Warmups, layout queries and OPTIMIZE run outside these windows.

| Phase | Start UTC | End UTC | GET sum | Complete minutes |
| --- | --- | --- | ---: | --- |
| before | 2026-11-03T04:47:00+00:00 | 2026-11-03T04:48:00+00:00 | 6269.0 | True |
| after | 2026-11-03T04:52:00+00:00 | 2026-11-03T04:53:00+00:00 | 939.0 | True |

## Layout

| Phase | Files | Bytes | Records |
| --- | ---: | ---: | ---: |
| before | 1722 | 141573089 | 302939 |
| after | 19 | 12234394 | 302939 |

## Mean Comparison

| Metric | Before | After | Reduction % |
| --- | ---: | ---: | ---: |
| DataScannedInBytes | 47104552.00 | 7981509.00 | 83.06 |
| TotalExecutionTimeInMillis | 2179.00 | 1295.00 | 40.57 |
| EngineExecutionTimeInMillis | 2019.00 | 1145.00 | 43.29 |
| QueryPlanningTimeInMillis | 416.33 | 168.67 | 59.49 |
| QueryQueueTimeInMillis | 83.33 | 77.00 | 7.60 |

## Compaction

Query ID: `6d67096d-74e6-4836-a908-b5cc772ec880`

Statistics: `{"DataManifestLocation": "s3://oliveyoung-crawl-data/athena-results/tables/6d67096d-74e6-4836-a908-b5cc772ec880-manifest.csv", "DataScannedInBytes": 126941534, "EngineExecutionTimeInMillis": 13237, "QueryPlanningTimeInMillis": 315, "QueryQueueTimeInMillis": 98, "ResultReuseInformation": {"ReusedPreviousResult": false}, "ServicePreProcessingTimeInMillis": 86, "ServiceProcessingTimeInMillis": 23, "TotalExecutionTimeInMillis": 13444}`

Timing results are a small three-run sample, not a general performance guarantee.
