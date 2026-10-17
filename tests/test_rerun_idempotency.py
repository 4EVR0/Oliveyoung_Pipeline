"""정상 파이프라인 재실행 멱등 계약 — 같은 batch_date를 다시 돌려도 결과가 한 벌."""

import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

from gold_pipeline import write_gold
from models.batch_metadata import BatchMetadata, check_batch_date_required
from silver_pipeline import write_silver
from src.bronze_to_silver import pipeline

DAY = datetime(2026, 10, 4, tzinfo=timezone.utc)
SCHEMA = Schema(
    NestedField(1, "batch_job", StringType(), required=False),
    NestedField(2, "batch_date", TimestamptzType(), required=False),
    NestedField(3, "product_id", StringType(), required=False),
)


def _arrow(rows):
    return pa.Table.from_pylist(
        [{"batch_job": j, "batch_date": d, "product_id": p} for j, d, p in rows], schema=SCHEMA.as_arrow()
    )


class LocalTableTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self._tmp.name).as_posix()
        self.catalog = SqlCatalog("t", uri=f"sqlite:///{root}/c.db", warehouse=f"file://{root}/w")
        self.catalog.create_namespace("t")
        self.table = self.catalog.create_table("t.x", schema=SCHEMA)

    def tearDown(self):
        self._tmp.cleanup()

    def rows(self):
        t = self.catalog.load_table("t.x").scan().to_arrow()
        return sorted(zip(t.column("batch_job").to_pylist(), t.column("product_id").to_pylist()))


class HistoryReplaceTest(LocalTableTest):
    """replace_history — 실제 Iceberg 교체 범위. 테이블 로드·arrow 변환만 테스트용으로 대체."""

    def _replace(self, ids, job, scope):
        df = pd.DataFrame({"product_id": ids})
        with patch.object(write_silver, "_load_and_evolve_table", return_value=self.catalog.load_table("t.x")), \
             patch.object(write_silver, "_build_arrow_table_for_silver",
                          side_effect=lambda d, t: _arrow([(job, DAY, p) for p in d["product_id"]])):
            return write_silver.replace_history(self.catalog, df, "2026-10-04", job, scope)

    def test_normal_rerun_replaces_previous_normal_run_only(self):
        other_day = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.table.append(_arrow([
            ("bronze_to_silver_20261001_010000", other_day, "old"),   # 다른 날짜
            ("backfill_20261003", DAY, "bf"),                         # 같은 날짜 백필
            ("bronze_to_silver", DAY, "legacy"),                      # 2026-10 이전 형식(작업 이름만)
        ]))
        self.assertEqual(self._replace(["a", "b"], "bronze_to_silver_20261004_010000", "normal"), 1)
        self.assertEqual(self._replace(["a", "b"], "bronze_to_silver_20261004_030000", "normal"), 2)
        self.assertEqual(self.rows(), [
            ("backfill_20261003", "bf"),
            ("bronze_to_silver_20261001_010000", "old"),
            ("bronze_to_silver_20261004_030000", "a"),
            ("bronze_to_silver_20261004_030000", "b"),
        ])

    def test_backfill_scope_replaces_same_key_only(self):
        self.table.append(_arrow([("bronze_to_silver_20261004_010000", DAY, "n")]))
        self._replace(["x"], "backfill_20261003", "backfill")
        self.assertEqual(self._replace(["y"], "backfill_20261003", "backfill"), 1)
        self.assertEqual(self.rows(), [("backfill_20261003", "y"), ("bronze_to_silver_20261004_010000", "n")])

    def test_invalid_scope_rejected(self):
        with self.assertRaises(ValueError):
            write_silver.replace_history(MagicMock(), pd.DataFrame(), "2026-10-04", "x", "all")


class GoldReplaceTest(LocalTableTest):
    def test_rerun_replaces_previous_run_of_same_day(self):
        other = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.table.append(_arrow([("iceberg_silver_to_gold_20261001_0100", other, "keep")]))
        first = BatchMetadata(batch_job="iceberg_silver_to_gold_20261004_0100", batch_date=DAY)
        second = BatchMetadata(batch_job="iceberg_silver_to_gold_20261004_0300", batch_date=DAY)
        self.assertEqual(write_gold._replace_batch(
            self.catalog.load_table("t.x"), _arrow([(first.batch_job, DAY, "a")]), first), 0)
        self.assertEqual(write_gold._replace_batch(
            self.catalog.load_table("t.x"), _arrow([(second.batch_job, DAY, "a")]), second), 1)
        self.assertEqual(self.rows(), [
            ("iceberg_silver_to_gold_20261001_0100", "keep"),
            ("iceberg_silver_to_gold_20261004_0300", "a"),
        ])


class WriteOrderTest(unittest.TestCase):
    def test_history_error_then_current_and_empty_error_clears(self):
        tables = {name: MagicMock(name=name) for name in ("error", "current")}
        order = MagicMock()
        order.attach_mock(tables["error"].overwrite, "error_overwrite")
        order.attach_mock(tables["current"].overwrite, "current_overwrite")

        def load(_, identifier):
            return tables["error"] if identifier.endswith("error") else tables["current"]

        with patch.object(write_silver.OliveyoungIceberg, "get_catalog"), \
             patch.object(write_silver, "replace_history", return_value=3) as history, \
             patch.object(write_silver, "_load_and_evolve_table", side_effect=load), \
             patch.object(write_silver, "_build_arrow_table_for_silver"):
            order.attach_mock(history, "history")
            for t in tables.values():
                t.schema.return_value.as_arrow.return_value = pa.schema([("x", pa.string())])
            replaced = write_silver.write_to_iceberg(
                pd.DataFrame({"product_id": ["a"]}), pd.DataFrame(), "2026-10-04", "bronze_to_silver_1")
        self.assertEqual(replaced, 3)
        names = [c[0] for c in order.mock_calls]
        self.assertEqual(names, ["history", "error_overwrite", "current_overwrite"])
        # 에러 0건이어도 빈 overwrite로 이전 배치 에러를 비운다
        self.assertEqual(tables["error"].overwrite.call_args.args[0].num_rows, 0)


class PipelineOrderTest(unittest.TestCase):
    def test_silver_is_written_and_verified_before_dq(self):
        order = MagicMock()
        silver = pd.DataFrame({"product_id": ["a"]})
        error = pd.DataFrame({"error_type": ["X"]})
        with patch.object(pipeline, "check_batch_date_required"), \
             patch.object(pipeline.DuckDB, "get_connection"), \
             patch.object(pipeline, "load_bronze_data", return_value=(pd.DataFrame({"x": [1, 2]}), "20261003")), \
             patch.object(pipeline, "load_dictionaries"), \
             patch.object(pipeline, "_resolve_batch_date", return_value="2026-10-04"), \
             patch.object(pipeline, "process_pipeline", return_value=(silver, error)) as proc, \
             patch.object(pipeline.OliveyoungIceberg, "get_catalog"), \
             patch.object(pipeline, "write_to_iceberg", return_value=0) as w, \
             patch.object(pipeline, "_verify_silver") as v, \
             patch.object(pipeline, "write_dq_metrics") as dq, \
             patch.object(pipeline, "write_csv_to_s3"):
            order.attach_mock(w, "write"); order.attach_mock(v, "verify"); order.attach_mock(dq, "dq")
            pipeline.run_pipeline()
        names = [c[0] for c in order.mock_calls]
        self.assertEqual(names[:2], ["write", "verify"])
        self.assertTrue(all(n == "dq" for n in names[2:]) and len(names) == 4)
        # history 행과 DQ가 같은 실행 ID를 쓰고, DQ는 이전 실행을 교체
        run_id = w.call_args.args[3]
        self.assertTrue(run_id.startswith("bronze_to_silver_"))
        # cleaner는 행에 batch.batch_job을 찍는다 → 그 값이 교체·검증에 쓰는 실행 ID와 같아야 한다
        self.assertEqual(proc.call_args.kwargs["batch"].batch_job, run_id)
        self.assertTrue(all(c.kwargs["run_id"] == run_id and c.kwargs["replace"] == "other_runs"
                            for c in dq.call_args_list))


class StampedRowsContractTest(LocalTableTest):
    """cleaner와 같은 방식(add_batch_metadata)으로 찍힌 batch_job으로 교체·검증이 맞물리는지.

    2026-10-17 버그 회귀 방지: 행에는 작업 이름만 찍히고 검증은 실행 ID로 세서 항상 0건이었다.
    """

    def test_verify_counts_rows_stamped_like_cleaner(self):
        from models.batch_metadata import add_batch_metadata
        with patch.dict(os.environ, {"BATCH_DATE": "2026-10-04"}), \
             patch.object(pipeline, "check_batch_date_required"), \
             patch.object(pipeline.DuckDB, "get_connection"), \
             patch.object(pipeline, "load_bronze_data", return_value=(pd.DataFrame({"x": [1]}), "20261003")), \
             patch.object(pipeline, "load_dictionaries"), \
             patch.object(pipeline, "_resolve_batch_date", return_value="2026-10-04"), \
             patch.object(pipeline, "process_pipeline",
                          return_value=(pd.DataFrame({"product_id": ["a"]}), pd.DataFrame())) as proc, \
             patch.object(pipeline, "write_to_iceberg", return_value=0), \
             patch.object(pipeline, "_verify_silver"), \
             patch.object(pipeline.OliveyoungIceberg, "get_catalog"), \
             patch.object(pipeline, "write_dq_metrics"), patch.object(pipeline, "write_csv_to_s3"):
            pipeline.run_pipeline()
        batch = proc.call_args.kwargs["batch"]          # 파이프라인이 cleaner에 넘긴 실제 batch
        df = pd.DataFrame({"product_id": ["a", "b"]})
        add_batch_metadata(df, batch, "2026-10-04")     # cleaner가 행에 찍는 방식 그대로
        stamped = _arrow([(j, DAY, p) for j, p in zip(df["batch_job"], df["product_id"])])
        self.table.append(stamped)
        cur = self.catalog.create_table("t.cur", schema=SCHEMA)
        cur.append(stamped)
        names = {pipeline.OliveyoungIceberg.SILVER_HISTORY_TABLE: "t.x",
                 pipeline.OliveyoungIceberg.SILVER_CURRENT_TABLE: "t.cur"}
        fake = MagicMock()
        fake.load_table.side_effect = lambda n: self.catalog.load_table(names[n])
        with patch.object(pipeline.OliveyoungIceberg, "get_catalog", return_value=fake):
            pipeline._verify_silver("2026-10-04", batch.batch_job, 2)   # 찍힌 값으로 정확히 2건
            with self.assertRaises(RuntimeError):
                pipeline._verify_silver("2026-10-04", batch.batch_job, 3)
        # 찍힌 값이 정상 교체 범위(실행 ID 접두어)에 들어가야 재실행 때 교체된다
        self.assertTrue(batch.batch_job.startswith(write_silver.NORMAL_BATCH_JOB_PREFIX))


class BatchDateRequiredTest(unittest.TestCase):
    def test_airflow_run_without_batch_date_fails(self):
        with patch.dict(os.environ, {"REQUIRE_BATCH_DATE": "1", "BATCH_DATE": ""}):
            with self.assertRaises(RuntimeError):
                check_batch_date_required()
        with patch.dict(os.environ, {"REQUIRE_BATCH_DATE": "1", "BATCH_DATE": "2026-10-04"}):
            check_batch_date_required()
        with patch.dict(os.environ, {"REQUIRE_BATCH_DATE": "", "BATCH_DATE": ""}):
            check_batch_date_required()   # Airflow 밖 단독 실행은 기존 폴백


if __name__ == "__main__":
    unittest.main()
