"""Read-only history layout audit; reads four columns, writes local metrics only."""

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from config.settings import OliveyoungIceberg


def main():
    table = OliveyoungIceberg.get_catalog().load_table(OliveyoungIceberg.SILVER_HISTORY_TABLE)
    snapshot_id = table.current_snapshot().snapshot_id
    tasks = list(table.scan(snapshot_id=snapshot_id).plan_files())
    files = []
    groups = []
    for index, task in enumerate(tasks):
        file = task.file
        frames = []
        page_indexes = []
        with table.io.new_input(file.file_path).open() as stream:
            parquet = pq.ParquetFile(stream)
            for n in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(n)
                frame = parquet.read_row_group(n, columns=["category", "product_id", "batch_date", "crawled_at"]).to_pandas()
                frame["batch_date"] = pd.to_datetime(frame["batch_date"], utc=True)
                stats = {}
                for c in range(metadata.num_columns):
                    column = metadata.column(c)
                    if column.path_in_schema in {"product_id", "batch_date", "crawled_at"}:
                        page_indexes.append({
                            "row_group": n, "column": column.path_in_schema,
                            "has_column_index": column.has_column_index,
                            "has_offset_index": column.has_offset_index,
                        })
                    if column.path_in_schema in {"product_id", "batch_date"}:
                        s = column.statistics
                        stats[column.path_in_schema] = (s.min, s.max) if s is not None and s.has_min_max else None
                groups.append((index, frame, stats))
                frames.append(frame)
            data = pd.concat(frames, ignore_index=True)
            files.append({
                "index": index, "bytes": file.file_size_in_bytes, "rows": file.record_count,
                "category": data["category"].iloc[0], "row_groups": parquet.num_row_groups,
                "product_id_ndv": int(data["product_id"].nunique()),
                "batch_date_min": data["batch_date"].min(), "batch_date_max": data["batch_date"].max(),
                "batch_date_monotonic": bool(data["batch_date"].dropna().is_monotonic_increasing),
                "crawled_at_monotonic": bool(data["crawled_at"].dropna().is_monotonic_increasing),
                "iceberg_bound_field_ids": sorted(file.lower_bounds or {}),
                "page_indexes": page_indexes,
            })
        print(f"Read file {index + 1}/{len(tasks)}", flush=True)
    all_data = pd.concat([frame for _, frame, _ in groups], ignore_index=True)
    ids = sorted(all_data["product_id"].dropna().unique())
    latest = all_data["batch_date"].max().normalize()
    end = latest + pd.Timedelta(days=1)
    category = all_data["category"].value_counts().index[0]
    representative = sorted({ids[int((len(ids) - 1) * q)] for q in (0, .25, .5, .75, 1)})
    missing = ids[len(ids) // 2] + "~audit_missing"
    while missing in ids:
        missing += "x"
    queries = []

    def measure(label, predicate, mask, bounds_candidate):
        candidates = list(table.scan(snapshot_id=snapshot_id, row_filter=predicate).plan_files())
        candidate_paths = {t.file.file_path for t in candidates}
        matched_files, matching_groups, candidate_groups = set(), 0, 0
        matched_rows = 0
        for file_index, frame, stats in groups:
            count = int(mask(frame).sum())
            matched_rows += count
            if count:
                matched_files.add(file_index)
                matching_groups += 1
            if tasks[file_index].file.file_path in candidate_paths and bounds_candidate(stats, frame):
                candidate_groups += 1
        return {"label": label, "iceberg_candidate_files": len(candidates),
                "iceberg_candidate_file_bytes": sum(t.file.file_size_in_bytes for t in candidates),
                "actual_matching_files": len(matched_files), "actual_matching_rows": matched_rows,
                "row_selectivity": matched_rows / len(all_data),
                "row_groups_minmax_candidates": candidate_groups, "row_groups_with_matches": matching_groups}

    for product_id in representative + [missing]:
        queries.append(measure(
            f"product_id = {product_id}", EqualTo("product_id", product_id),
            lambda f, value=product_id: f["product_id"] == value,
            lambda s, f, value=product_id: s.get("product_id") is None or s["product_id"][0] <= value <= s["product_id"][1],
        ))
    date_predicate = And(GreaterThanOrEqual("batch_date", latest.isoformat()), LessThan("batch_date", end.isoformat()))
    queries.append(measure(
        f"latest UTC day: {latest.isoformat()}", date_predicate,
        lambda f: (f["batch_date"] >= latest) & (f["batch_date"] < end),
        lambda s, f: s.get("batch_date") is None or (pd.Timestamp(s["batch_date"][0]) < end and pd.Timestamp(s["batch_date"][1]) >= latest),
    ))
    queries.append(measure(
        f"category = {category}", EqualTo("category", category), lambda f: f["category"] == category,
        lambda s, f: bool((f["category"] == category).any()),
    ))
    queries.append(measure(
        f"category = {category} AND latest UTC day", And(EqualTo("category", category), date_predicate),
        lambda f: (f["category"] == category) & (f["batch_date"] >= latest) & (f["batch_date"] < end),
        lambda s, f: bool((f["category"] == category).any()) and (s.get("batch_date") is None or (pd.Timestamp(s["batch_date"][0]) < end and pd.Timestamp(s["batch_date"][1]) >= latest)),
    ))
    sizes = pd.Series([f["bytes"] for f in files])
    report = {
        "at_utc": datetime.now(timezone.utc).isoformat(), "table": OliveyoungIceberg.SILVER_HISTORY_TABLE,
        "snapshot_id": snapshot_id, "properties": dict(table.properties), "sort_order": str(table.sort_order()),
        "partition_spec": str(table.spec()), "pyarrow_version": pyarrow.__version__,
        "active_files": len(files), "active_bytes": int(sizes.sum()), "rows": len(all_data),
        "size_min": int(sizes.min()), "size_p50": float(sizes.median()), "size_p95": float(sizes.quantile(.95)),
        "size_max": int(sizes.max()), "files_under_1MiB": int((sizes < 1048576).sum()),
        "files_per_category": dict(Counter(f["category"] for f in files)),
        "row_groups": len(groups), "product_id_ndv": len(ids),
        "batch_date_ndv": int(all_data["batch_date"].nunique()),
        "batch_date_days": int(all_data["batch_date"].dt.normalize().nunique()),
        "files": files, "representative_predicates": queries,
        "scope": "PyIceberg file planning + Parquet min/max + exact matches in four read columns; not an Athena latency benchmark or Bloom implementation test.",
    }
    output = Path("profile_results") / ("iceberg_layout_audit_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + ".json")
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(output, flush=True)
    print(json.dumps({k:v for k,v in report.items() if k != "files"}, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
