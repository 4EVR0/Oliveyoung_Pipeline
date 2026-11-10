"""Isolated physical SORT versus Athena BIN_PACK layout experiment.

Not a benchmark of Spark rewrite_data_files implementation or job runtime.
Run in the pipeline image with AWS credentials. No production table writes.
"""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import random
import statistics
import sys
import time
from uuid import uuid4
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import duckdb
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import IdentityTransform

from config.settings import OliveyoungIceberg, S3
from scripts.benchmark_athena_compaction import Runner


VARIANTS = {
    "binpack": (None, None),
    "rewrite_large": (None, None),
    "date_large": ("batch_date", None),
    "rewrite_small": (None, 4096),
    "date_small": ("batch_date", 4096),
    "id_small": ("product_id", 4096),
}


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def fingerprint(data):
    fields = []
    for name in sorted(data.column_names):
        quoted = '"' + name.replace('"', '""') + '"'
        fields.append(f"{quoted} := {quoted}")
    with duckdb.connect() as connection:
        connection.register("input_rows", data)
        hashes = connection.execute(
            "SELECT digest, count(*) FROM (SELECT sha256(to_json(struct_pack("
            + ", ".join(fields) + "))) AS digest FROM input_rows) GROUP BY digest ORDER BY digest"
        ).fetchall()
    digest = hashlib.sha256(json.dumps(hashes).encode()).hexdigest()
    return {"rows": data.num_rows, "row_multiset_sha256": digest}


def sort_partition(data, column):
    if column is None:
        return data
    tie = "product_id" if column == "batch_date" else "batch_date"
    return data.sort_by([(column, "ascending"), (tie, "ascending")])


def normalize(data, schema):
    arrays = []
    for field in schema:
        if field.name in data.column_names:
            column = pc.cast(data[field.name], field.type)
        elif field.nullable:
            column = pa.nulls(data.num_rows, type=field.type)
        else:
            raise ValueError(f"Required field is missing: {field.name}")
        if not field.nullable and column.null_count:
            raise ValueError(f"Required field contains nulls: {field.name}")
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=schema)


def write_file(target, data, index, row_group_rows):
    path = f"{target.location()}/data/part-{index:05d}-{uuid4().hex}.parquet"
    schema = schema_to_pyarrow(target.schema(), include_field_ids=False)
    data = data.cast(schema)
    with target.io.new_output(path).create(overwrite=False) as stream:
        pq.write_table(data, stream, compression="zstd", compression_level=3,
                       row_group_size=row_group_rows or data.num_rows,
                       write_statistics=True, write_page_index=False)
    return path


def describe_layout(table):
    tasks = list(table.scan().plan_files())
    files = []
    for task in tasks:
        f = task.file
        with table.io.new_input(f.file_path).open() as stream:
            parquet = pq.ParquetFile(stream)
            groups = []
            for n in range(parquet.num_row_groups):
                group = parquet.metadata.row_group(n)
                bounds = {}
                for c in range(group.num_columns):
                    col = group.column(c)
                    if col.path_in_schema in {"batch_date", "product_id"}:
                        s = col.statistics
                        bounds[col.path_in_schema] = [s.min, s.max] if s and s.has_min_max else None
                groups.append({"rows": group.num_rows, "bounds": bounds})
            files.append({"path": f.file_path, "bytes": f.file_size_in_bytes,
                          "rows": f.record_count, "partition": list(f.partition), "groups": groups})
    return {"files": len(files), "bytes": sum(f["bytes"] for f in files),
            "rows": sum(f["rows"] for f in files),
            "row_groups": sum(len(f["groups"]) for f in files), "file_details": files}


def summary(runner):
    report = runner.report
    summary_rows = []
    for workload in report["workloads"]:
        for variant in VARIANTS:
            items = [x for x in report["executions"] if x["label"].startswith(f"measure:{workload}:{variant}:")]
            if not items:
                continue
            if len({x["result_sha256"] for x in items}) != 1:
                raise RuntimeError("Repeated results changed")
            summary_rows.append({
                "workload": workload, "variant": variant,
                "scan_bytes": statistics.mean(x["execution"]["Statistics"]["DataScannedInBytes"] for x in items),
                "total_ms": statistics.mean(x["execution"]["Statistics"]["TotalExecutionTimeInMillis"] for x in items),
                "total_ms_median": statistics.median(x["execution"]["Statistics"]["TotalExecutionTimeInMillis"] for x in items),
                "engine_ms": statistics.mean(x["execution"]["Statistics"]["EngineExecutionTimeInMillis"] for x in items),
                "planning_ms": [x["execution"]["Statistics"].get("QueryPlanningTimeInMillis") for x in items],
            })
        hashes = {x["result_sha256"] for x in report["executions"] if x["label"].startswith(f"measure:{workload}:")}
        if len(hashes) != 1:
            raise RuntimeError(f"Cross-variant result mismatch: {workload}")
    report["summary"] = summary_rows
    report["query_results_match"] = True
    runner.save()
    lines = ["# Iceberg Physical Sort vs Binpack", "",
             f"Source snapshot: `{report['source_snapshot']}`", "",
             "Six isolated layouts; same snapshot, category partitioning, no result reuse, one excluded warmup per query.",
             "Sort is implemented with PyArrow, not Spark rewrite_data_files. Small row groups use 4096 rows, page index disabled.",
             "Binpack uses Athena OPTIMIZE. rewrite_large controls for the writer/encoding change.", "",
             "| Layout | Files | Row groups | Active bytes | Build seconds |",
             "| --- | ---: | ---: | ---: | ---: |"]
    for variant, info in report["tables"].items():
        layout = info["layout"]
        lines.append(f"| {variant} | {layout['files']} | {layout['row_groups']} | {layout['bytes']} | {info['build_seconds']:.3f} |")
    lines += ["", "| Workload | Layout | Mean scanned bytes | Mean total ms | Median total ms | Mean engine ms |",
              "| --- | --- | ---: | ---: | ---: | ---: |"]
    for row in summary_rows:
        lines.append(f"| {row['workload']} | {row['variant']} | {row['scan_bytes']:.0f} | {row['total_ms']:.2f} | {row['total_ms_median']:.2f} | {row['engine_ms']:.2f} |")
    lines += ["", "All output row-multiset fingerprints and per-workload query result hashes matched.",
              "GET counts are not measured; API counts in JSON are not S3 request counts.",
              "Build timings cover preparation/validation in the completing attempt; resumed runs exclude earlier work. Use the separate OPTIMIZE statistics for Athena compaction runtime.",
              "Experiment tables and data remain isolated under the recorded benchmark prefix. Do not expire snapshots on production as cleanup.",
              "No statistical significance claim from three runs. Workloads are representative experiments, not measured business traffic."]
    runner.output.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--resume", help="Resume preparation or measurement from an existing JSON checkpoint")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output = Path(args.resume).with_suffix("") if args.resume else Path("profile_results") / f"iceberg_sort_comparison_{run_id}"
    runner_args = SimpleNamespace(region=S3.REGION, workgroup="oliveyoung-ops", runs=args.runs,
                                  resume=args.resume, output_location=None, timeout=600)
    runner = Runner(runner_args, output)
    if args.resume:
        runner.report["resumed_at"] = datetime.now(timezone.utc).isoformat()
    catalog = OliveyoungIceberg.get_catalog()
    try:
        if not args.resume or not runner.report.get("layouts_ready"):
            source = catalog.load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
            snapshot = runner.report.get("source_snapshot", source.current_snapshot().snapshot_id)
            source_tasks = list(source.scan(snapshot_id=snapshot).plan_files())
            if any(t.delete_files for t in source_tasks):
                raise RuntimeError("Source has delete files; raw-file cloning would not preserve visible rows")
            prefix = runner.report.get("benchmark_prefix", f"s3://{S3.BUCKET}/benchmark/iceberg_sort/{run_id}")
            if args.resume:
                run_id = prefix.rsplit("/", 1)[-1]
            runner.report.update(source_snapshot=snapshot, source_table=OliveyoungIceberg.SILVER_HISTORY_TABLE,
                                 benchmark_prefix=prefix, tables=runner.report.get("tables", {}),
                                 experiment_note="Physical layout experiment; no Spark sort procedure used")
            runner.report["fingerprint_algorithm"] = "DuckDB SHA256 canonical column-ordered row JSON, grouped by digest with duplicate counts; SHA256 ordered digest/count pairs"
            runner.save()
            source_parts = []
            canonical_schema = schema_to_pyarrow(source.schema(), include_field_ids=False)
            for n, task in enumerate(source_tasks):
                with source.io.new_input(task.file.file_path).open() as stream:
                    source_parts.append(normalize(pq.ParquetFile(stream).read(), canonical_schema))
                print(f"Read source file {n + 1}/{len(source_tasks)}", flush=True)
            data = pa.concat_tables(source_parts)
            reference = fingerprint(data)
            sizes = [t.file.file_size_in_bytes for t in source_tasks]
            runner.report["precheck"] = {
                "source_files": len(source_tasks), "bytes": sum(sizes),
                "file_size_median": statistics.median(sizes),
                "files_under_1MiB": sum(s < 1048576 for s in sizes),
                "files_per_partition": dict(Counter(str(list(t.file.partition)) for t in source_tasks)),
                "rows": data.num_rows, "batch_date_ndv": len(pc.unique(data["batch_date"])),
                "product_id_ndv": len(pc.unique(data["product_id"])),
            }
            runner.report["source_fingerprint"] = reference
            runner.save()

            def create(variant, column):
                if variant in runner.report["tables"]:
                    return catalog.load_table(runner.report["tables"][variant]["identifier"])
                order = SortOrder() if column is None else SortOrder(SortField(
                    source_id=source.schema().find_field(column).field_id,
                    transform=IdentityTransform(), direction=SortDirection.ASC,
                    null_order=NullOrder.NULLS_LAST))
                name = f"{OliveyoungIceberg.DATABASE}.bench_sort_{run_id}_{variant}"
                table = catalog.create_table(name, schema=source.schema(), partition_spec=source.spec(),
                                             sort_order=order, location=f"{prefix}/{variant}",
                                             properties={"format-version": "2", "write.metadata.metrics.default": "full"})
                runner.report["tables"][variant] = {"identifier": name, "location": table.location()}
                runner.save()
                return table

            start = time.perf_counter()
            baseline = create("binpack", None)
            if baseline.current_snapshot() is None:
                paths = [write_file(baseline, part, i, None) for i, part in enumerate(source_parts)]
                baseline.add_files(paths)
            # Low-volume source now has ~3 files/partition; threshold 2 allows an actual rewrite.
            if not any(x["label"] == "setup:binpack:optimize" and x.get("execution", {}).get("Status", {}).get("State") == "SUCCEEDED" for x in runner.report["executions"]):
                runner.execute("setup:binpack:threshold", f"ALTER TABLE {runner.report['tables']['binpack']['identifier']} SET TBLPROPERTIES ('optimize_rewrite_data_file_threshold'='2')")
                runner.execute("setup:binpack:optimize", f"OPTIMIZE {runner.report['tables']['binpack']['identifier']} REWRITE DATA USING BIN_PACK")
            baseline.refresh()
            packed = baseline.scan().to_arrow()
            if fingerprint(packed) != reference:
                raise RuntimeError("Binpack row multiset mismatch")
            info = runner.report["tables"]["binpack"]
            info.update(snapshot_id=baseline.current_snapshot().snapshot_id,
                        build_seconds=time.perf_counter() - start, fingerprint=reference,
                        layout=describe_layout(baseline))
            runner.save()
            del source_parts, data
            partitions = []
            for value in pc.unique(packed["category"]).to_pylist():
                mask = pc.is_null(packed["category"]) if value is None else pc.equal(packed["category"], value)
                partitions.append(packed.filter(mask))
            latest = pc.max(packed["batch_date"]).as_py().replace(hour=0, minute=0, second=0, microsecond=0)
            end = latest + timedelta(days=1)
            week = latest - timedelta(days=6)
            ids = sorted(pc.unique(packed["product_id"]).to_pylist())
            product_id = ids[len(ids) // 2]
            category = packed.filter(pc.equal(packed["product_id"], product_id))["category"][0].as_py()
            projection = "category, count(*) AS rows, sum(length(product_name)) AS name_chars, sum(review_count) AS reviews"
            day_sql = f"batch_date >= from_iso8601_timestamp({literal(latest.isoformat())}) AND batch_date < from_iso8601_timestamp({literal(end.isoformat())})"
            week_sql = f"batch_date >= from_iso8601_timestamp({literal(week.isoformat())}) AND batch_date < from_iso8601_timestamp({literal(end.isoformat())})"
            runner.report["workloads"] = {
                "full": "TRUE", "latest_day": day_sql, "latest_week": week_sql,
                "product_id": f"product_id = {literal(product_id)}",
                "category_day": f"category = {literal(category)} AND {day_sql}",
                "category": f"category = {literal(category)}",
            }
            runner.report["projection"] = projection
            for variant, (column, rows) in VARIANTS.items():
                if variant == "binpack":
                    continue
                start = time.perf_counter()
                target = create(variant, column)
                if target.current_snapshot() is None:
                    paths = [write_file(target, sort_partition(part, column), i, rows)
                             for i, part in enumerate(partitions)]
                    target.add_files(paths)
                actual = fingerprint(target.scan().to_arrow())
                if actual != reference:
                    raise RuntimeError(f"Row multiset mismatch: {variant}")
                runner.report["tables"][variant].update(
                    snapshot_id=target.current_snapshot().snapshot_id, fingerprint=actual,
                    build_seconds=time.perf_counter() - start, layout=describe_layout(target))
                runner.save()
                print(f"Validated {variant}", flush=True)
            runner.report["layouts_ready"] = True
            runner.save()

        for workload, predicate in runner.report["workloads"].items():
            variants = list(VARIANTS)
            for variant in variants:
                info = runner.report["tables"][variant]
                sql = (f"SELECT {runner.report['projection']} FROM {info['identifier']} "
                       f"FOR VERSION AS OF {info['snapshot_id']} WHERE {predicate} GROUP BY category ORDER BY category")
                label = f"warmup:{workload}:{variant}"
                if not any(x["label"] == label for x in runner.report["executions"]):
                    runner.execute(label, sql)
            for n in range(args.runs):
                random.Random(20261110 + n).shuffle(variants)
                for variant in variants:
                    label = f"measure:{workload}:{variant}:{n + 1}"
                    if any(x["label"] == label and x.get("execution", {}).get("Status", {}).get("State") == "SUCCEEDED" for x in runner.report["executions"]):
                        continue
                    info = runner.report["tables"][variant]
                    sql = (f"SELECT {runner.report['projection']} FROM {info['identifier']} "
                           f"FOR VERSION AS OF {info['snapshot_id']} WHERE {predicate} GROUP BY category ORDER BY category")
                    runner.execute(label, sql)
        source = catalog.load_table(runner.report["source_table"])
        runner.report["production_snapshot_at_end"] = source.current_snapshot().snapshot_id
        summary(runner)
        print(runner.output.with_suffix(".md"), flush=True)
    finally:
        runner.save()


if __name__ == "__main__":
    main()
