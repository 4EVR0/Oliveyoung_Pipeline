"""Local Iceberg safety contract: conditional overwrite preserves unrelated rows."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType


class BackfillOverwriteTest(unittest.TestCase):
    def test_partitioned_history_and_unpartitioned_dq(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).as_posix()
            catalog = SqlCatalog("test", uri=f"sqlite:///{root}/catalog.db",
                                 warehouse=f"file://{root}/warehouse")
            catalog.create_namespace("test")
            schema = Schema(
                NestedField(1, "category", StringType(), required=False),
                NestedField(2, "batch_job", StringType(), required=False),
                NestedField(3, "batch_date", TimestamptzType(), required=False),
                NestedField(4, "product_id", StringType(), required=True),
            )
            partition = PartitionSpec(PartitionField(
                source_id=1, field_id=1000, transform=IdentityTransform(), name="category"
            ))
            history = catalog.create_table("test.history", schema=schema, partition_spec=partition)
            day = datetime(2026, 7, 25, tzinfo=timezone.utc)

            def history_arrow(ids, jobs):
                return pa.Table.from_pydict({
                    "category": ["skin"] * len(ids), "batch_job": jobs,
                    "batch_date": [day] * len(ids), "product_id": ids,
                }, schema=schema.as_arrow())

            history.append(history_arrow(["old", "keep"], ["backfill_x", "normal_run"]))
            selector = And(EqualTo("batch_date", day), EqualTo("batch_job", "backfill_x"))
            history.overwrite(history_arrow(["new"], ["backfill_x"]), overwrite_filter=selector)
            self.assertEqual(
                sorted(catalog.load_table("test.history").scan().to_arrow().column("product_id").to_pylist()),
                ["keep", "new"],
            )
            history = catalog.load_table("test.history")
            history.overwrite(pa.Table.from_batches([], schema=schema.as_arrow()), overwrite_filter=selector)
            self.assertEqual(
                catalog.load_table("test.history").scan().to_arrow().column("product_id").to_pylist(),
                ["keep"],
            )

            dq_schema = Schema(
                NestedField(1, "stage", StringType(), required=False),
                NestedField(2, "run_id", StringType(), required=False),
                NestedField(3, "batch_date", StringType(), required=True),
                NestedField(4, "metric_name", StringType(), required=False),
            )
            dq = catalog.create_table("test.dq", schema=dq_schema)

            def dq_arrow(names, runs):
                return pa.Table.from_pydict({
                    "stage": ["bronze_to_silver_backfill"] * len(names),
                    "run_id": runs, "batch_date": ["2026-07-25"] * len(names),
                    "metric_name": names,
                }, schema=dq_schema.as_arrow())

            dq.append(dq_arrow(["old", "keep"], ["backfill_x", "backfill_y"]))
            dq.overwrite(dq_arrow(["new"], ["backfill_x"]), overwrite_filter=And(
                And(EqualTo("stage", "bronze_to_silver_backfill"), EqualTo("batch_date", "2026-07-25")),
                EqualTo("run_id", "backfill_x"),
            ))
            self.assertEqual(
                sorted(catalog.load_table("test.dq").scan().to_arrow().column("metric_name").to_pylist()),
                ["keep", "new"],
            )
            catalog.close()


if __name__ == "__main__":
    unittest.main()
