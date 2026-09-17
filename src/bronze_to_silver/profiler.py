"""
Lightweight stage profiler for the Bronze -> Silver pipeline.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import os
import resource
import time
from typing import Any, Iterator


def _peak_rss_mb() -> float:
    """Return max RSS in MiB. Linux reports ru_maxrss in KiB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _current_cpu() -> int | None:
    getcpu = getattr(os, "sched_getcpu", None)
    if getcpu is not None:
        try:
            return int(getcpu())
        except OSError:
            return None
    return None


def _cpu_affinity() -> list[int] | None:
    getaffinity = getattr(os, "sched_getaffinity", None)
    if getaffinity is None:
        return None
    try:
        return sorted(getaffinity(0))
    except OSError:
        return None


@dataclass
class ProfileStep:
    name: str
    wall_seconds: float
    cpu_seconds: float
    peak_rss_mb: float
    rows_in: int | None = None
    rows_out: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def cpu_wall_ratio(self) -> float | None:
        if self.wall_seconds <= 0:
            return None
        return self.cpu_seconds / self.wall_seconds

    @property
    def rows_per_second(self) -> float | None:
        rows = self.rows_out if self.rows_out is not None else self.rows_in
        if rows is None or self.wall_seconds <= 0:
            return None
        return rows / self.wall_seconds


class StepHandle:
    def __init__(self, rows_in: int | None = None) -> None:
        self.rows_in = rows_in
        self.rows_out: int | None = None
        self.metadata: dict[str, Any] = {}

    def set_rows_out(self, rows_out: int | None) -> None:
        self.rows_out = rows_out

    def set_metadata(self, **metadata: Any) -> None:
        self.metadata.update(metadata)


class PipelineProfiler:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.steps: list[ProfileStep] = []
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.pid = os.getpid()
        self.cpu_affinity = _cpu_affinity()
        self.current_cpu = _current_cpu()

    @contextmanager
    def step(self, name: str, rows_in: int | None = None) -> Iterator[StepHandle]:
        handle = StepHandle(rows_in=rows_in)
        if not self.enabled:
            yield handle
            return

        wall_start = time.perf_counter()
        cpu_start = time.process_time()
        try:
            yield handle
        finally:
            self.steps.append(
                ProfileStep(
                    name=name,
                    wall_seconds=time.perf_counter() - wall_start,
                    cpu_seconds=time.process_time() - cpu_start,
                    peak_rss_mb=_peak_rss_mb(),
                    rows_in=handle.rows_in,
                    rows_out=handle.rows_out,
                    metadata=handle.metadata,
                )
            )

    def print_summary(self) -> None:
        if not self.enabled:
            return

        total_wall = self._total_wall_seconds()
        print("\n=== Bronze -> Silver profiling summary ===")
        print(
            "env: "
            f"pid={self.pid}, "
            f"affinity={self.cpu_affinity}, "
            f"current_cpu={self.current_cpu}, "
            f"started_at={self.started_at}"
        )
        print(
            f"{'stage':<28} {'wall_s':>10} {'cpu_s':>10} {'cpu/wall':>9} "
            f"{'rows_in':>10} {'rows_out':>10} {'rows/s':>12} {'peak_rss_mb':>12}"
        )
        for step in self.steps:
            ratio = "" if step.cpu_wall_ratio is None else f"{step.cpu_wall_ratio:.2f}"
            rows_in = "" if step.rows_in is None else str(step.rows_in)
            rows_out = "" if step.rows_out is None else str(step.rows_out)
            rows_per_second = (
                "" if step.rows_per_second is None else f"{step.rows_per_second:,.0f}"
            )
            print(
                f"{step.name:<28} "
                f"{step.wall_seconds:>10.3f} "
                f"{step.cpu_seconds:>10.3f} "
                f"{ratio:>9} "
                f"{rows_in:>10} "
                f"{rows_out:>10} "
                f"{rows_per_second:>12} "
                f"{step.peak_rss_mb:>12.1f}"
            )

        print("\nprofiling_json:")
        for step in self.steps:
            print(json.dumps(_step_to_dict(step), ensure_ascii=False, sort_keys=True))

        print("\ninterpretation hints:")
        for line in self._interpret(total_wall):
            print(f"- {line}")

    def _total_wall_seconds(self) -> float:
        for step in reversed(self.steps):
            if step.name == "total":
                return step.wall_seconds
        return sum(step.wall_seconds for step in self.steps)

    def _interpret(self, total_wall: float) -> list[str]:
        if not self.steps:
            return ["No profiling data was collected."]

        hints: list[str] = []
        wrapper_names = {"total", "process_pipeline", "iceberg_write", "csv_s3_write"}
        candidates = [step for step in self.steps if step.name not in wrapper_names]
        top_steps = sorted(candidates or self.steps, key=lambda step: step.wall_seconds, reverse=True)[:3]
        for step in top_steps:
            share = 0 if total_wall <= 0 else (step.wall_seconds / total_wall) * 100
            ratio = step.cpu_wall_ratio
            if ratio is None:
                reason = "not enough timing data"
            elif ratio >= 0.75:
                reason = "CPU-bound Python/data transformation work is likely dominant"
            elif ratio <= 0.35:
                reason = "I/O, network, storage, or waiting on native libraries is likely dominant"
            else:
                reason = "mixed CPU and I/O/native work is likely"
            hints.append(f"{step.name}: {step.wall_seconds:.3f}s ({share:.1f}% of total), {reason}.")

        write_names = {
            "pandas_to_arrow_current",
            "pandas_to_arrow_history",
            "pandas_to_arrow_error",
            "iceberg_current_write",
            "iceberg_history_write",
            "iceberg_error_write",
            "csv_silver_prepare",
            "csv_silver_upload",
            "csv_error_upload",
        }
        transform_names = {
            "product_name_norm_compile",
            "clean_rows",
            "dedup",
            "ingredient_match",
            "build_output_dataframes",
        }
        write_wall = sum(step.wall_seconds for step in self.steps if step.name in write_names)
        transform_wall = sum(step.wall_seconds for step in self.steps if step.name in transform_names)
        if write_wall > transform_wall and write_wall > 0:
            hints.append("Write stages exceed in-memory processing; inspect Arrow conversion, Parquet compression, Iceberg commits, and S3 latency.")
        elif transform_wall > write_wall and transform_wall > 0:
            hints.append("In-memory processing exceeds writes; inspect regex normalization, DataFrame sort/dedup, and ingredient matching.")

        return hints


def _step_to_dict(step: ProfileStep) -> dict[str, Any]:
    return {
        "stage": step.name,
        "wall_seconds": round(step.wall_seconds, 6),
        "cpu_seconds": round(step.cpu_seconds, 6),
        "cpu_wall_ratio": None if step.cpu_wall_ratio is None else round(step.cpu_wall_ratio, 6),
        "rows_in": step.rows_in,
        "rows_out": step.rows_out,
        "rows_per_second": None if step.rows_per_second is None else round(step.rows_per_second, 3),
        "peak_rss_mb": round(step.peak_rss_mb, 3),
        "metadata": step.metadata,
    }
