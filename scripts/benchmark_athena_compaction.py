"""Measure the same history query before and after Athena OPTIMIZE.

Run with runtime AWS credentials; see the companion Markdown for usage.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import statistics
import time

import boto3


DATABASE = "oliveyoung_db"
TABLE = "oliveyoung_silver_history"


class Runner:
    def __init__(self, args, output):
        self.args = args
        self.output = output
        self.client = boto3.client("athena", region_name=args.region)
        self.calls = Counter()
        self.cloudwatch = boto3.client("cloudwatch", region_name=args.region)
        self.report = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "workgroup": args.workgroup,
            "table": f"{DATABASE}.{TABLE}",
            "runs": args.runs,
            "executions": [],
            "metric_windows": {},
            "s3_get_requests": None,
            "s3_get_requests_note": "Not exposed by Athena query statistics. File count is not GET count.",
        }
        if args.resume:
            self.report = json.loads(Path(args.resume).read_text(encoding="utf-8"))
            self.calls.update(self.report.get("api_calls_total", {}))

    def call(self, operation, **kwargs):
        self.calls[operation] += 1
        response = getattr(self.client, operation)(**kwargs)
        self.calls["sdk_retries"] += response.get("ResponseMetadata", {}).get("RetryAttempts", 0)
        return response

    def save(self):
        self.report["api_calls_total"] = dict(self.calls)
        self.output.with_suffix(".json").write_text(
            json.dumps(self.report, indent=2, default=str) + "\n", encoding="utf-8"
        )

    def execute(self, label, sql):
        before = self.calls.copy()
        start = time.perf_counter()
        request = dict(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE, "Catalog": "AwsDataCatalog"},
            WorkGroup=self.args.workgroup,
            ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
        )
        if self.args.output_location:
            request["ResultConfiguration"] = {"OutputLocation": self.args.output_location}
        try:
            query_id = self.call("start_query_execution", **request)["QueryExecutionId"]
        except Exception as exc:
            self.report.setdefault("submission_errors", []).append({
                "label": label, "sql": sql, "error": str(exc),
                "at": datetime.now(timezone.utc).isoformat(),
            })
            self.save()
            raise
        item = {"label": label, "query_id": query_id, "sql": sql}
        self.report["executions"].append(item)
        self.save()
        try:
            while True:
                execution = self.call("get_query_execution", QueryExecutionId=query_id)["QueryExecution"]
                state = execution["Status"]["State"]
                if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    break
                if time.perf_counter() - start > self.args.timeout:
                    self.call("stop_query_execution", QueryExecutionId=query_id)
                    raise TimeoutError(f"Timed out: {query_id}")
                time.sleep(1)
            item["execution"] = execution
            item["client_wall_seconds"] = time.perf_counter() - start
            if state != "SUCCEEDED":
                raise RuntimeError(str(execution["Status"]))
            rows = []
            token = None
            while True:
                params = {"QueryExecutionId": query_id}
                if token:
                    params["NextToken"] = token
                result = self.call("get_query_results", **params)
                rows.extend(result["ResultSet"]["Rows"])
                token = result.get("NextToken")
                if not token:
                    break
            item["result_sha256"] = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
            try:
                item["runtime_statistics"] = self.call(
                    "get_query_runtime_statistics", QueryExecutionId=query_id
                )["QueryRuntimeStatistics"]
            except self.client.exceptions.ClientError as exc:
                item["runtime_statistics_error"] = str(exc)
            print(label, query_id, execution.get("Statistics", {}), flush=True)
            return rows
        finally:
            item["api_calls"] = dict(self.calls - before)
            self.save()

    def layout(self, label):
        rows = self.execute(label, f'''SELECT count(*) AS files,
            sum(file_size_in_bytes) AS bytes, sum(record_count) AS records
            FROM "{DATABASE}"."{TABLE}$files" WHERE content = 0''')
        self.report[label] = rows
        self.save()

    def minute_boundary(self):
        delay = 60 - time.time() % 60 + 2
        print(f"Waiting {delay:.0f}s for an isolated metric minute", flush=True)
        time.sleep(delay)

    def wait_for_metrics(self):
        if not self.args.metrics_filter:
            return
        s3 = boto3.client("s3", region_name=self.args.region)
        config = s3.get_bucket_metrics_configuration(
            Bucket=self.args.metrics_bucket, Id=self.args.metrics_filter
        )["MetricsConfiguration"]
        if config.get("Filter", {}).get("Prefix") != "silver/history/":
            raise ValueError("Metric filter must cover the actual silver/history/ table prefix")
        self.report["metrics_configuration"] = config
        objects = s3.list_objects_v2(Bucket=self.args.metrics_bucket, Prefix="silver/history/", MaxKeys=1)
        key = objects["Contents"][0]["Key"]
        started = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        deadline = time.monotonic() + self.args.metrics_ready_wait
        while True:
            # A single-byte probe confirms the newly configured prefix is being observed.
            response = s3.get_object(Bucket=self.args.metrics_bucket, Key=key, Range="bytes=0-0")
            response["Body"].close()
            data = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/S3", MetricName="GetRequests",
                Dimensions=[{"Name": "BucketName", "Value": self.args.metrics_bucket},
                            {"Name": "FilterId", "Value": self.args.metrics_filter}],
                StartTime=started, EndTime=datetime.now(timezone.utc), Period=60, Statistics=["Sum"],
            )
            if any(p["Sum"] > 0 for p in data.get("Datapoints", [])):
                self.report["metrics_ready_at"] = datetime.now(timezone.utc).isoformat()
                self.save()
                print("GET metrics are active; starting baseline", flush=True)
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("S3 request metrics did not activate; no compaction executed")
            print("Waiting for S3 metric activation; one-byte GET probe outside benchmark windows", flush=True)
            time.sleep(30)

    def phase(self, phase, sql):
        self.execute(f"warmup_{phase}", sql)
        if self.args.metrics_filter:
            self.minute_boundary()
        start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for n in range(self.args.runs):
            self.execute(f"{phase}_{n + 1}", sql)
        end = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
        self.report["metric_windows"][phase] = {"start": start.isoformat(), "end": end.isoformat()}
        self.save()
        if self.args.metrics_filter:
            self.minute_boundary()

    def collect_metrics(self):
        if not self.args.metrics_filter:
            return
        self.report["s3_get_requests_note"] = (
            "CloudWatch best-effort prefix totals during isolated measurement minutes; "
            "not query-attributed. Other readers of this prefix may contribute. "
            "Warmups, layout queries and OPTIMIZE run outside these windows."
        )
        deadline = time.monotonic() + self.args.metrics_wait
        while True:
            complete = True
            for phase, window in self.report["metric_windows"].items():
                response = self.cloudwatch.get_metric_statistics(
                    Namespace="AWS/S3", MetricName="GetRequests",
                    Dimensions=[{"Name": "BucketName", "Value": self.args.metrics_bucket},
                                {"Name": "FilterId", "Value": self.args.metrics_filter}],
                    StartTime=datetime.fromisoformat(window["start"]),
                    EndTime=datetime.fromisoformat(window["end"]),
                    Period=60, Statistics=["Sum"],
                )
                points = response.get("Datapoints", [])
                window["datapoints"] = points
                expected = int((datetime.fromisoformat(window["end"]) - datetime.fromisoformat(window["start"])).total_seconds() / 60)
                window["expected_minutes"] = expected
                window["complete_minutes"] = len(points) == expected
                window["get_requests_sum"] = sum(p["Sum"] for p in points) if points else None
                complete = complete and window["complete_minutes"]
            self.report["s3_get_requests"] = {
                phase: window["get_requests_sum"] for phase, window in self.report["metric_windows"].items()
            }
            self.save()
            if complete or time.monotonic() >= deadline:
                break
            print("Waiting for CloudWatch GET datapoints (missing is not zero)", flush=True)
            time.sleep(min(30, max(0, deadline - time.monotonic())))

    def run(self):
        # Nontrivial column aggregation forces a data read instead of metadata-only COUNT.
        sql = f'''SELECT category, count(*) AS rows,
            sum(length(product_id)) AS product_id_chars,
            sum(length(product_name)) AS product_name_chars,
            sum(cardinality(product_ingredients)) AS ingredients,
            sum(review_count) AS reviews, min(batch_date) AS first_batch,
            max(batch_date) AS last_batch
            FROM "{DATABASE}"."{TABLE}"
            GROUP BY category ORDER BY category NULLS FIRST'''
        if not self.args.resume:
            self.report["query"] = sql
            self.wait_for_metrics()
            self.layout("layout_before")
            self.phase("before", sql)
        else:
            labels = {x["label"] for x in self.report["executions"]}
            if "optimize" in labels or any(f"before_{n + 1}" not in labels for n in range(self.args.runs)):
                raise ValueError("Resume requires a completed baseline and no submitted OPTIMIZE")
            if self.report.get("query") != sql or self.report.get("workgroup") != self.args.workgroup:
                raise ValueError("Resume SQL and workgroup must match the baseline")
        if not self.args.apply:
            self.report["note"] = "Baseline only. Pass --apply to compact and measure after."
            self.save()
            return
        if self.args.metrics_filter:
            self.collect_metrics()
            if not self.report["metric_windows"]["before"]["complete_minutes"]:
                raise RuntimeError("Baseline GET metrics are missing; OPTIMIZE was not executed. Retry after metrics activation.")
        self.execute("optimize", f'OPTIMIZE {DATABASE}.{TABLE} REWRITE DATA USING BIN_PACK')
        self.layout("layout_after")
        self.phase("after", sql)
        measured = [x for x in self.report["executions"] if x["label"].startswith(("before_", "after_"))]
        self.report["aggregate_results_match"] = len({x["result_sha256"] for x in measured}) == 1
        self.save()
        self.collect_metrics()
        self.markdown(measured)
        if not self.report["aggregate_results_match"]:
            raise RuntimeError("Aggregate results changed; do not attribute timings solely to compaction.")

    def markdown(self, measured):
        lines = ["# Athena History Compaction Benchmark", "", f"- Workgroup: `{self.args.workgroup}`",
                 f"- Table: `{DATABASE}.{TABLE}`", "- Each phase has one excluded warmup; result reuse disabled.",
                 f"- Aggregate results match: `{self.report['aggregate_results_match']}`",
                 "- S3 GET: see prefix-level metric windows below; separate from client API calls.", "",
                 "| Phase | Run | Scanned bytes | Total ms | Engine ms | Planning ms | Queue ms | API calls |",
                 "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        fields = ["DataScannedInBytes", "TotalExecutionTimeInMillis", "EngineExecutionTimeInMillis",
                  "QueryPlanningTimeInMillis", "QueryQueueTimeInMillis"]
        for phase in ("before", "after"):
            items = [x for x in measured if x["label"].startswith(phase + "_")]
            for item in items:
                stats = item["execution"]["Statistics"]
                values = [str(stats.get(f, "N/A")) for f in fields]
                calls = sum(v for k, v in item["api_calls"].items() if k != "sdk_retries")
                lines.append(f"| {phase} | {item['label'].split('_')[-1]} | " + " | ".join(values) + f" | {calls} |")
            values = [f"{statistics.mean(x['execution']['Statistics'].get(f, 0) for x in items):.2f}" for f in fields]
            lines.append(f"| {phase} | mean | " + " | ".join(values) + " | |")
        lines += ["", "Raw execution statistics, query IDs, SQL, layout counts and OPTIMIZE cost are in the companion JSON.",
                  "Aggregate equality is a sanity check, not a full row-level equality proof.",
                  "Run during a pause in ingestion; concurrent writes invalidate the comparison.",
                  "Compaction preserves rows, adds a snapshot, and does not expire historical files."]
        lines += ["", "## S3 GET Observations", "", self.report["s3_get_requests_note"], "",
                  "| Phase | Start UTC | End UTC | GET sum | Complete minutes |",
                  "| --- | --- | --- | ---: | --- |"]
        for phase, window in self.report["metric_windows"].items():
            lines.append(f"| {phase} | {window['start']} | {window['end']} | {window.get('get_requests_sum', 'unavailable')} | {window.get('complete_minutes', False)} |")
        lines += ["", "## Layout", "", "| Phase | Files | Bytes | Records |", "| --- | ---: | ---: | ---: |"]
        for phase in ("before", "after"):
            rows = self.report.get(f"layout_{phase}", [])
            if len(rows) > 1:
                values = [v.get("VarCharValue", "NULL") for v in rows[1]["Data"]]
                lines.append(f"| {phase} | " + " | ".join(values) + " |")
        lines += ["", "## Mean Comparison", "", "| Metric | Before | After | Reduction % |", "| --- | ---: | ---: | ---: |"]
        for field in fields:
            values = [statistics.mean(x["execution"]["Statistics"].get(field, 0)
                      for x in measured if x["label"].startswith(phase + "_")) for phase in ("before", "after")]
            reduction = (1 - values[1] / values[0]) * 100 if values[0] else 0
            lines.append(f"| {field} | {values[0]:.2f} | {values[1]:.2f} | {reduction:.2f} |")
        optimize = next(x for x in self.report["executions"] if x["label"] == "optimize")
        lines += ["", "## Compaction", "", f"Query ID: `{optimize['query_id']}`", "",
                  f"Statistics: `{json.dumps(optimize['execution']['Statistics'], sort_keys=True)}`", "",
                  "Timing results are a small three-run sample, not a general performance guarantee."]
        self.output.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workgroup", default="oliveyoung-ops")
    parser.add_argument("--region", default="ap-northeast-2")
    parser.add_argument("--output-location", help="Optional Athena result S3 prefix")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--apply", action="store_true", help="Run OPTIMIZE on silver_history between phases")
    parser.add_argument("--metrics-filter")
    parser.add_argument("--metrics-bucket", default="oliveyoung-crawl-data")
    parser.add_argument("--metrics-wait", type=int, default=600)
    parser.add_argument("--metrics-ready-wait", type=int, default=1200)
    parser.add_argument("--resume", help="Existing JSON with baseline completed and OPTIMIZE not submitted")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    root = Path("profile_results")
    root.mkdir(exist_ok=True)
    output = root / ("athena_compaction_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    if args.resume:
        output = Path(args.resume).with_suffix("")
    runner = Runner(args, output)
    try:
        runner.run()
    finally:
        runner.save()


if __name__ == "__main__":
    main()
