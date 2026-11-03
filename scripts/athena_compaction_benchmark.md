# Athena Compaction Benchmark

`benchmark_athena_compaction.py` executes the same category aggregation on
`oliveyoung_db.oliveyoung_silver_history` before and after BIN_PACK compaction.
The SQL is stored in every execution record. Each phase excludes one warmup,
disables result reuse, and measures three runs by default.

## Execute

Baseline queries only:

```bash
python scripts/benchmark_athena_compaction.py
```

Full before/after comparison with S3 request observations:

```bash
docker run --rm --network host --env-file .env \
  -v /home/jaewon/Oliveyoung_Pipeline:/app -w /app \
  --entrypoint python oliveyoung-pipeline-local:latest \
  scripts/benchmark_athena_compaction.py --apply \
  --metrics-filter SilverHistoryMetrics --metrics-wait 600
```

Required runtime permissions include Athena query submission/status/results,
the workgroup's normal Glue/S3 access, and CloudWatch GetMetricStatistics.
No credentials are written to the artifacts. Results use the workgroup's S3
result location unless `--output-location` overrides it and the workgroup allows it.

The S3 request metric configuration must cover `silver/history/` in bucket
`oliveyoung-crawl-data`, region `ap-northeast-2`. Enable it ahead of the run.
Pause other reads and ingestion on this prefix during measurement.
The script validates the prefix and waits for an observed GET datapoint using
one-byte GET probes before measuring. Probe requests run outside the comparison
windows. New S3 metric configurations can take about 15 minutes to activate;
`--metrics-ready-wait` defaults to 1200 seconds.

## Before and After

Before code executes the aggregation against the existing files. The change is:

```sql
OPTIMIZE oliveyoung_db.oliveyoung_silver_history
REWRITE DATA USING BIN_PACK;
```

After code executes exactly the same aggregation against the new snapshot.
This rewrites history files; current/error tables are not targets. It creates
a snapshot, incurs Athena rewrite scan charges, and does not expire snapshots
or delete historical rows. Re-running the experiment after compaction measures
the already compacted baseline, not the original layout.

If submission failed before an OPTIMIZE query ID was created, use
`--resume profile_results/<original-run>.json` with the same workgroup and run
count to retain the baseline. Resume refuses a record with an OPTIMIZE already
submitted. Resume only while ingestion remains paused; aggregate checks after
compaction cannot replace a full row-level audit.

## Records

Timestamped JSON and Markdown are saved in `profile_results/`. JSON is checkpointed
after each execution, including failures, and retains query IDs, exact SQL,
raw execution/runtime statistics, client API call counts and retries, layout
metadata results, and result hashes. Compaction and warmup statistics are separate
from the three measured SELECTs. Aggregate hashes check the query results, not
full row-level equality. A mismatch fails the comparison.

CloudWatch GET uses UTC minute-aligned windows that exclude this script's warmups,
metadata layout queries and compaction. It is a best-effort prefix-level total,
not an exact per-query count. It includes metadata GETs under the prefix and
any unrelated readers. Missing datapoints are not treated as zero. The script
waits for baseline datapoints before OPTIMIZE and stops without compaction if
they remain missing. Published datapoints can be delayed or revised later.

Athena client API requests include status polling; they are not S3 GET counts.
Query planning time is a subset of engine time. Athena total time includes queue
and service time; client wall time also includes network and polling overhead.

References:

- [Athena OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html)
- [Athena execution statistics](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html)
- [S3 request metrics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html)
