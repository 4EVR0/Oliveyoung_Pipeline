# Physical Sort versus Binpack Experiment

This experiment pins the latest history snapshot and writes only new Glue tables
and new S3 objects beneath `benchmark/iceberg_sort/<run-id>/`. It does not change
the source history or run OPTIMIZE against it. Athena SELECT result files are
written to the configured workgroup result prefix.

## Variants

| Variant | Writer | Row order within category | Row group size |
| --- | --- | --- | --- |
| binpack | Athena OPTIMIZE after cloning source row chunks | Athena binpack output | Athena output |
| rewrite_large | PyArrow | Same sequence as the binpack input for controls | One group per category file |
| date_large | PyArrow | batch_date, product_id ascending | One group per category file |
| rewrite_small | PyArrow | Same sequence as rewrite_large | 4096 rows |
| date_small | PyArrow | batch_date, product_id ascending | 4096 rows |
| id_small | PyArrow | product_id, batch_date ascending | 4096 rows |

Compare date_large to rewrite_large for sorting with a large group; date_small
to rewrite_small for sorting at a fixed smaller group size. Compare rewrite_small
to rewrite_large for the group-size effect. The PyArrow variants use identical
ZSTD level 3, page index disabled, no Bloom configuration, and one file/category.
4096 rows is an experimental factor, not a production recommendation. The Athena
baseline has its own encoding and layout; rewrite_large controls for that writer
change. All variants explicitly collect full Iceberg file metrics.

SORT is a physical PyArrow sort plus PyIceberg add_files registration. It is not
a Spark rewrite_data_files(strategy=sort) implementation/runtime benchmark.
Athena OPTIMIZE is used for the actual BIN_PACK baseline. The experimental
baseline lowers optimize_rewrite_data_file_threshold to 2 because source
partitions can have fewer files than Athena's default selection threshold.
This setting affects the isolated baseline only.

## Execute

```bash
docker run --rm --network host --env-file .env \
  -v /home/jaewon/Oliveyoung_Pipeline:/app -w /app \
  --entrypoint python oliveyoung-pipeline-local:latest \
  scripts/benchmark_iceberg_sort.py --runs 3
```

Output: timestamped JSON and Markdown under `profile_results/`. JSON checkpoints
table identifiers, locations, source snapshot, setup query IDs, schemas/layout
metadata, row fingerprints, exact SQL, and individual Athena execution statistics.
No table rows are saved as a local dump.

```bash
python -m unittest scripts.test_iceberg_sort
```

## Validation and Comparison

All input rows, including duplicate multiplicity, are hashed into an order-independent
row-multiset fingerprint. DuckDB computes SHA256 of column-ordered row JSON and
groups the hashes with duplicate counts; the ordered hash/count pairs are hashed
again. Every layout is read back and validated. Source delete
files are rejected so cloning raw source files cannot silently include deleted rows.
Parquet imports omit old field IDs and use the new Iceberg schema's name mapping.
The original schema and category partition semantics are retained.

Each workload has one excluded warmup for every layout. Measured layout order is
shuffled with a recorded fixed seed policy, with result reuse disabled. All queries
pin each variant's snapshot. Three repetitions are the default. Per-workload
aggregate result hashes must match across variants and repetitions.

Workloads: full category aggregation, latest UTC day, latest seven UTC days,
one deterministic product ID, known category + latest day, and category alone.
These are representative experiments, not samples of measured business traffic.
The median ID is used; category selection is recorded in SQL. Full row fingerprints
provide stronger data validation than the aggregate query results alone.

File/row-group bounds and source partition file counts are retained for precheck.
Inspect these together with measured scanned bytes and latency rather than treating
candidate file bytes as Athena scanned bytes. Sort may affect compression as well
as pruning, so the fixed-writer controls are essential for attribution.

## Resume and Retention

Preparation or measurement can resume from a checkpoint:

```bash
python scripts/benchmark_iceberg_sort.py --runs 3 \
  --resume profile_results/iceberg_sort_comparison_<run-id>.json
```

Use the same run count and retain all generated snapshots. Existing committed
tables are reused and validated again; missing layouts are built with unique object
names. Uncommitted files from an interrupted upload may remain under the isolated
prefix and require explicit cleanup. The script leaves experiment tables/data in place
for review. No automatic cleanup, production snapshot expiration, or file deletion
is performed. Any cleanup must be limited to the recorded isolated prefix/tables.

GET counts are not measured by default. Athena client API call counts are retained
but are not S3 request counts. To measure GET, create filters for each experiment
prefix, allow metric activation, isolate time windows and account for other readers.

Build times include serialization, registration and validation; baseline also
includes clone preparation and OPTIMIZE on a fresh run. A resumed preparation's
times cover only the resumed portion. They are not directly comparable engine
compaction job costs. Athena OPTIMIZE execution/scan statistics are recorded separately.

References:

- [Iceberg add_files](https://py.iceberg.apache.org/api/#add-files)
- [Parquet writer options](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)
- [Athena OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html)
