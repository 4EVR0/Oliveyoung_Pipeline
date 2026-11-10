"""Local checks for sort controls and duplicate-preserving validation."""

import unittest

import pyarrow as pa

from scripts.benchmark_iceberg_sort import fingerprint, sort_partition, normalize


class SortTests(unittest.TestCase):
    def test_fingerprint_ignores_order_but_preserves_duplicates(self):
        a = pa.table({"product_id": ["a", "b", "a"], "batch_date": [2, 1, 2]})
        self.assertEqual(fingerprint(a), fingerprint(a.take(pa.array([2, 0, 1]))))
        self.assertNotEqual(fingerprint(a), fingerprint(a.slice(0, 2)))

    def test_date_sort_uses_id_tie_break(self):
        data = pa.table({"product_id": ["c", "b", "a"], "batch_date": [2, 1, 2]})
        actual = sort_partition(data, "batch_date")
        self.assertEqual(actual["product_id"].to_pylist(), ["b", "a", "c"])
        self.assertEqual(fingerprint(data), fingerprint(actual))

    def test_unsorted_control_preserves_order(self):
        data = pa.table({"product_id": ["b", "a"], "batch_date": [2, 1]})
        self.assertIs(sort_partition(data, None), data)

    def test_string_width_normalization_preserves_values(self):
        narrow = pa.table({"product_id": pa.array(["a"], type=pa.string()),
                           "batch_date": [1]})
        wide = pa.table({"product_id": pa.array(["b"], type=pa.large_string()),
                         "batch_date": [2]})
        normalized = narrow.cast(wide.schema)
        combined = pa.concat_tables([normalized, wide])
        self.assertEqual(combined["product_id"].to_pylist(), ["a", "b"])
        self.assertEqual(fingerprint(narrow), fingerprint(normalized))

    def test_old_schema_nullable_field_is_filled(self):
        old = pa.table({"product_id": ["a"]})
        schema = pa.schema([pa.field("product_id", pa.large_string(), nullable=False),
                            pa.field("source_run_id", pa.large_string())])
        self.assertEqual(normalize(old, schema).to_pylist(),
                         [{"product_id": "a", "source_run_id": None}])

    def test_missing_required_field_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize(pa.table({"other": [1]}), pa.schema([pa.field("product_id", pa.string(), nullable=False)]))

    def test_fingerprint_preserves_column_associations(self):
        a = pa.table({"product_id": ["a", "b"], "batch_date": [1, 2]})
        b = pa.table({"product_id": ["a", "b"], "batch_date": [2, 1]})
        self.assertNotEqual(fingerprint(a), fingerprint(b))

    def test_fingerprint_preserves_list_order(self):
        a = pa.table({"ingredients": [["a", "b"]]})
        b = pa.table({"ingredients": [["b", "a"]]})
        self.assertNotEqual(fingerprint(a), fingerprint(b))


if __name__ == "__main__":
    unittest.main()
