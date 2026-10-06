"""Controlled Bronze -> Silver comparison using real, isolated S3/Glue outputs.

Run inside the pipeline image. See profile_results/pipeline_comparison.md.
Preparation and validation are deliberately outside the measured pipeline timer.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import DuckDB, INCIIceberg, OliveyoungIceberg, S3
from models.batch_metadata import BatchMetadata
from silver_pipeline import write_silver
from src.bronze_to_silver import pipeline
from src.bronze_to_silver.profiler import PipelineProfiler, _step_to_dict

MODES = ("before", "after_materialized", "after_cached")
STAGES = ("duckdb_connection", "bronze_discovery", "bronze_load",
          "dictionary_load", "process_pipeline", "iceberg_write", "csv_s3_write")
TABLE_ATTRS = ("SILVER_CURRENT_TABLE", "SILVER_HISTORY_TABLE", "SILVER_ERROR_TABLE")


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def canonical(value):
    if isinstance(value, dict):
        return {str(k): canonical(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [canonical(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def row_counter(df):
    # Row ordering is irrelevant; duplicate multiplicity and list order are not.
    columns = sorted(df.columns)
    return Counter(json.dumps(canonical(row), ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"), allow_nan=False)
                   for row in df[columns].to_dict("records"))


def fingerprint(df):
    rows = row_counter(df)
    payload = json.dumps(sorted(rows.items()), ensure_ascii=False).encode()
    return {"rows": len(df), "columns": sorted(df.columns),
            "sha256": hashlib.sha256(payload).hexdigest()}


def object_inventory(files):
    client = boto3.client("s3", region_name=S3.REGION)
    def inspect(uri):
        bucket, key = uri.removeprefix("s3://").split("/", 1)
        head = client.head_object(Bucket=bucket, Key=key)
        return {"uri": uri, "bytes": head["ContentLength"],
                "etag": head["ETag"], "version": head.get("VersionId")}
    with ThreadPoolExecutor(max_workers=8) as executor:
        return list(executor.map(inspect, files))


def pinned_catalog(catalog, snapshots):
    class PinnedTable:
        def __init__(self, table, snapshot):
            self.table, self.snapshot = table, snapshot

        def scan(self, *args, **kwargs):
            kwargs["snapshot_id"] = self.snapshot
            return self.table.scan(*args, **kwargs)

    class PinnedCatalog:
        def load_table(self, identifier):
            return PinnedTable(catalog.load_table(identifier), snapshots[identifier])

    return PinnedCatalog()


def load_fixed_dictionaries(manifest):
    from unittest.mock import patch
    olive = OliveyoungIceberg.get_catalog()
    inci = INCIIceberg.get_catalog()
    with patch.object(OliveyoungIceberg, "get_catalog", return_value=pinned_catalog(olive, manifest["snapshots"])), \
         patch.object(INCIIceberg, "get_catalog", return_value=pinned_catalog(inci, manifest["snapshots"])):
        return pipeline.load_dictionaries()


def transform(raw, dictionaries, manifest, profiler=None):
    return pipeline.process_pipeline(
        raw, dictionaries.ac_automaton, dictionaries.typo_list,
        dictionaries.typo_regex_list, dictionaries.garbage_config,
        dictionaries.product_name_norm_list,
        batch=BatchMetadata("bronze_to_silver", datetime.fromisoformat(manifest["batch_date"])),
        profiler=profiler,
    )


def configure_paths(manifest, label):
    prefix = manifest["s3_prefix"]
    assert prefix.startswith("benchmark/pipeline_comparison/")
    S3.BRONZE_OPTIMIZED_PATH = f"s3://{S3.BUCKET}/{prefix}/optimized"
    S3.DATA_CSV_PATH = f"s3://{S3.BUCKET}/{prefix}/{label}/csv/"
    for attr, kind in zip(TABLE_ATTRS, ("current", "history", "error")):
        setattr(OliveyoungIceberg, attr, f"{OliveyoungIceberg.DATABASE}.bench_{manifest['id']}_{label}_{kind}")


def prepare(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        raise ValueError("Use a new output directory; an existing manifest cannot be overwritten")
    catalog = OliveyoungIceberg.get_catalog()
    inci = INCIIceberg.get_catalog()
    con = DuckDB.get_connection()
    files = DuckDB.get_latest_bronze_files(con)
    refs = [(catalog, OliveyoungIceberg.TYPO_MAP_TABLE),
            (catalog, OliveyoungIceberg.GARBAGE_KEYWORDS_TABLE),
            (catalog, OliveyoungIceberg.CUSTOM_INGREDIENT_DICT_TABLE),
            (inci, INCIIceberg.SILVER_GRAPHRAG_CURRENT_TABLE)]
    now = datetime.now(timezone.utc)
    manifest = {
        "id": now.strftime("%Y%m%d_%H%M%S"), "batch_date": now.isoformat(),
        "s3_prefix": f"benchmark/pipeline_comparison/{now.strftime('%Y%m%d_%H%M%S')}",
        "files": files, "objects": object_inventory(files),
        "snapshots": {name: cat.load_table(name).current_snapshot().snapshot_id for cat, name in refs},
        "baseline_writer_sha256": hashlib.sha256(Path(args.baseline_writer).read_bytes()).hexdigest(),
        "baseline_revision": "135398e", "after_revision": args.revision,
        "image": args.image,
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for folder in ("src/bronze_to_silver", "silver_pipeline", "config", "oliveyoung_common")
                          for p in sorted((ROOT / folder).glob("*.py"))},
        "environment": environment(),
        "cache_policy": {
            "before": "fresh process/connection; direct JSON; no application input cache",
            "after_materialized": "fresh process/connection; Parquet regenerated inside timer on every run",
            "after_cached": "fresh process/connection; validated Parquet prepared outside timer and reused",
            "shared": "OS, DNS, S3/Glue service caches uncontrolled; DuckDB extensions installed; no dictionary cache",
        },
    }
    configure_paths(manifest, "prepare")
    raw, _ = pipeline.load_bronze_data_from_files(con, files, "json")
    dictionaries = load_fixed_dictionaries(manifest)
    silver, error = transform(raw, dictionaries, manifest)
    manifest["input"] = fingerprint(raw)
    manifest["input"]["bytes"] = sum(obj["bytes"] for obj in manifest["objects"])
    manifest["expected"] = {"silver": fingerprint(silver), "error": fingerprint(error)}
    manifest["table_layouts"] = {}
    # Local row data is never committed; it seeds identical output state per run.
    for attr, df, builder, kind in (
        (TABLE_ATTRS[0], silver, write_silver._build_arrow_table_for_silver, "current"),
        (TABLE_ATTRS[1], silver, write_silver._build_arrow_table_for_silver, "history"),
        (TABLE_ATTRS[2], error, write_silver._build_arrow_table_for_error, "error"),
    ):
        production_name = f"{OliveyoungIceberg.DATABASE}.oliveyoung_silver_{kind}"
        table = catalog.load_table(production_name)
        manifest["table_layouts"][kind] = {
            "schema": table.schema().model_dump(mode="json"),
            "partition_spec": table.spec().model_dump(mode="json"),
            "sort_order": table.sort_order().model_dump(mode="json"),
            "properties": table.properties,
        }
        pq.write_table(builder(df, table), output / f"seed_{kind}.parquet")
    cached, _ = pipeline.load_bronze_data_from_files(con, files, "parquet")
    assert fingerprint(cached) == fingerprint(raw), (
        f"Parquet materialization changed input: json={fingerprint(raw)} parquet={fingerprint(cached)}"
    )
    cache_uri = pipeline.bronze_optimized_path(files, "parquet")
    manifest["optimized_object"] = object_inventory([cache_uri])[0]
    assert object_inventory(files) == manifest["objects"], "Bronze changed during preparation"
    con.close()
    dump(manifest_path, manifest)
    print(f"PREPARED {manifest_path}", flush=True)


def environment():
    def read(path):
        return Path(path).read_text().strip() if Path(path).exists() else None
    return {
        "python": sys.version, "platform": platform.platform(), "machine": platform.machine(),
        "cpu_count": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), None),
        "mem_total": Path("/proc/meminfo").read_text().splitlines()[0],
        "cpu_max": read("/sys/fs/cgroup/cpu.max"), "memory_max": read("/sys/fs/cgroup/memory.max"),
        "packages": {p: importlib.metadata.version(p) for p in ("duckdb", "pandas", "pyarrow", "pyiceberg", "boto3", "pyahocorasick")},
    }


def worker(args):
    output = Path(args.output)
    manifest = json.loads((output / "manifest.json").read_text())
    label = f"{args.mode}_{args.iteration}"
    configure_paths(manifest, label)
    assert object_inventory(manifest["files"]) == manifest["objects"], "Bronze input changed"
    catalog = OliveyoungIceberg.get_catalog()
    seeds = {}
    for attr, kind in zip(TABLE_ATTRS, ("current", "history", "error")):
        seed = pq.read_table(output / f"seed_{kind}.parquet")
        production = catalog.load_table(f"{OliveyoungIceberg.DATABASE}.oliveyoung_silver_{kind}")
        assert {"schema": production.schema().model_dump(mode="json"),
                "partition_spec": production.spec().model_dump(mode="json"),
                "sort_order": production.sort_order().model_dump(mode="json"),
                "properties": production.properties} == manifest["table_layouts"][kind], "Table layout changed"
        if any("path" in key or "location" in key for key in production.properties):
            raise ValueError("Review output path overrides before cloning table properties")
        table = catalog.create_table(
            getattr(OliveyoungIceberg, attr), schema=production.schema(),
            location=f"s3://{S3.BUCKET}/{manifest['s3_prefix']}/{label}/{kind}",
            partition_spec=production.spec(), sort_order=production.sort_order(),
            properties=production.properties,
        )
        # New Iceberg tables assign fresh field IDs, including nested map IDs.
        seed = seed.cast(table.schema().as_arrow())
        seeds[kind] = seed
        table.append(seed)
    writer = write_silver
    if args.mode == "before":
        spec = importlib.util.spec_from_file_location("baseline_writer", args.baseline_writer)
        writer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(writer)
        assert hashlib.sha256(Path(args.baseline_writer).read_bytes()).hexdigest() == manifest["baseline_writer_sha256"]
    os.environ["ICEBERG_PARALLEL_WRITES"] = "1"
    profiler = PipelineProfiler()
    load_before = os.getloadavg()
    with profiler.step("total"):
        with profiler.step("duckdb_connection"):
            con = DuckDB.get_connection()
        with profiler.step("bronze_discovery"):
            assert DuckDB.get_latest_bronze_files(con) == manifest["files"], "Latest input set changed"
        with profiler.step("bronze_load"):
            raw, metadata = pipeline.load_bronze_data_from_files(
                con, manifest["files"], "json" if args.mode == "before" else "parquet",
                materialize_optimized=args.mode != "after_cached",
            )
        with profiler.step("dictionary_load"):
            dictionaries = load_fixed_dictionaries(manifest)
        with profiler.step("process_pipeline"):
            silver, error = transform(raw, dictionaries, manifest, profiler)
        with profiler.step("iceberg_write"):
            writer.write_to_iceberg(silver, error, profiler=profiler)
        with profiler.step("csv_s3_write"):
            writer.write_csv_to_s3(silver, error, profiler=profiler)
    con.close()
    actual = {"silver": fingerprint(silver), "error": fingerprint(error)}
    assert actual == manifest["expected"], "Transformed output mismatch"
    assert fingerprint(raw) == {k: v for k, v in manifest["input"].items() if k != "bytes"}
    persisted = {}
    for attr, kind in zip(TABLE_ATTRS, ("current", "history", "error")):
        readback = catalog.load_table(getattr(OliveyoungIceberg, attr)).scan().to_arrow()
        expected = seeds[kind] if kind != "history" else pa.concat_tables([seeds[kind], seeds[kind]])
        assert readback.schema.equals(expected.schema, check_metadata=False), f"{kind} schema mismatch"
        assert row_counter(readback.to_pandas()) == row_counter(expected.to_pandas()), f"{kind} persisted mismatch"
        persisted[kind] = fingerprint(readback.to_pandas())
    client = boto3.client("s3", region_name=S3.REGION)
    prefix = S3.DATA_CSV_PATH.removeprefix(f"s3://{S3.BUCKET}/")
    objects = client.list_objects_v2(Bucket=S3.BUCKET, Prefix=prefix).get("Contents", [])
    assert len(objects) == 2, "Expected two CSV outputs"
    csv_hashes = {}
    for obj in objects:
        kind = "error" if "_error_" in obj["Key"] else "silver"
        content = client.get_object(Bucket=S3.BUCKET, Key=obj["Key"])["Body"].read()
        csv_hashes[kind] = hashlib.sha256(content).hexdigest()
    assert object_inventory(manifest["files"]) == manifest["objects"], "Bronze input changed"
    result = {"mode": args.mode, "iteration": args.iteration, "started_at": profiler.started_at,
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "load_before": load_before, "load_after": os.getloadavg(),
              "steps": [_step_to_dict(s) for s in profiler.steps], "bronze": metadata,
              "output": actual, "persisted": persisted, "csv_sha256": csv_hashes,
              "validation": "passed", "tables": [getattr(OliveyoungIceberg, a) for a in TABLE_ATTRS]}
    dump(output / f"{label}.json", result)
    print(f"COMPLETE {label} total={profiler._total_wall_seconds():.3f}s", flush=True)


def stats(values):
    median = statistics.median(values)
    return {"n": len(values), "median": median, "sample_stdev": statistics.stdev(values),
            "mad": statistics.median(abs(v - median) for v in values), "min": min(values), "max": max(values)}


def summarize(args):
    output = Path(args.output)
    manifest = json.loads((output / "manifest.json").read_text())
    runs = [json.loads((output / f"{mode}_{i}.json").read_text())
            for i in range(1, args.repeats + 1) for mode in MODES]
    assert all(run["validation"] == "passed" for run in runs)
    assert len({run["harness_sha256"] for run in runs}) == 1, "Harness changed during measurement"
    assert len({json.dumps(run["output"], sort_keys=True) for run in runs}) == 1
    assert len({json.dumps(run["persisted"], sort_keys=True) for run in runs}) == 1
    assert len({json.dumps(run["csv_sha256"], sort_keys=True) for run in runs}) == 1, "CSV bytes differ"
    summary = {}
    for mode in MODES:
        selected = [run for run in runs if run["mode"] == mode]
        timing = [{s["stage"]: s["wall_seconds"] for s in run["steps"]} for run in selected]
        summary[mode] = {}
        for stage in (*STAGES, "total"):
            values = [t[stage] for t in timing]
            summary[mode][stage] = stats(values)
            summary[mode][stage]["median_share_pct"] = statistics.median(100 * t[stage] / t["total"] for t in timing)
        summary[mode]["unattributed"] = stats([t["total"] - sum(t[s] for s in STAGES) for t in timing])
    report = {"manifest": manifest, "summary": summary, "runs": runs,
              "validation": "all transformed outputs, persisted rows/schemas, and CSV bytes identical",
              "excluded_warmups": [json.loads((output / f"{m}_0.json").read_text()) for m in MODES]}
    dump(output / "comparison.json", report)
    lines = ["# Bronze -> Silver controlled comparison", "", "All times in seconds. Five measured repetitions per mode; one excluded warm-up per mode.", "",
             "| Mode | Median | Sample SD | MAD | Min | Max |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for mode in MODES:
        s = summary[mode]["total"]
        lines.append(f"| {mode} | {s['median']:.3f} | {s['sample_stdev']:.3f} | {s['mad']:.3f} | {s['min']:.3f} | {s['max']:.3f} |")
    lines.extend(["", "Stage medians and median per-run shares (non-overlapping top-level stages only).", "",
                  "| Stage | Before seconds (%) | After materialized seconds (%) | After cached seconds (%) |",
                  "| --- | ---: | ---: | ---: |"])
    for stage in STAGES:
        cells = [f"{summary[m][stage]['median']:.3f} ({summary[m][stage]['median_share_pct']:.2f}%)" for m in MODES]
        lines.append(f"| {stage} | " + " | ".join(cells) + " |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "worker", "run", "summarize"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--baseline-writer", default="/baseline/write_silver.py")
    parser.add_argument("--revision", default="unknown")
    parser.add_argument("--image", default="unknown")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--mode", choices=MODES)
    parser.add_argument("--iteration", type=int)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("At least two measured repetitions required")
    if args.action == "prepare":
        prepare(args)
    elif args.action == "worker":
        worker(args)
    elif args.action == "summarize":
        summarize(args)
    else:
        for iteration in range(args.repeats + 1):
            # Rotate first position to reduce systematic time/order bias.
            order = MODES[iteration % len(MODES):] + MODES[:iteration % len(MODES)]
            for mode in order:
                path = Path(args.output) / f"{mode}_{iteration}.log"
                with path.open("w") as log:
                    subprocess.run([sys.executable, __file__, "worker", "--output", args.output,
                                    "--baseline-writer", args.baseline_writer, "--mode", mode,
                                    "--iteration", str(iteration)], stdout=log, stderr=subprocess.STDOUT, check=True)
                result = json.loads((Path(args.output) / f"{mode}_{iteration}.json").read_text())
                total = next(s["wall_seconds"] for s in result["steps"] if s["stage"] == "total")
                print(f"COMPLETE {mode} repetition={iteration} total={total:.3f}s", flush=True)
        summarize(args)


if __name__ == "__main__":
    main()
