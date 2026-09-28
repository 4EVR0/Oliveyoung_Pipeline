import csv
import hashlib
import io
import json
import unittest

import pandas as pd

from gold_pipeline.fragrance_evidence import label_evidence
from gold_pipeline.write_neo4j_csv import PRODUCT_COLUMNS, attach_fragrance_evidence
from oliveyoung_common.neo4j_csv import build_node_csv


class LabelEvidenceTest(unittest.TestCase):
    def test_exact_aliases_and_related_signal(self):
        for name in ("향료", "Fragrance", "PARFUM", "향료(Fragrance)"):
            row = label_evidence("p1", f"정제수, {name}, 리날룰", "https://example.org/p1", "2026-09-28")
            self.assertEqual("present", row["status"])
            self.assertEqual(["LINALOOL"], row["related_terms"])

    def test_marketing_empty_and_partial_are_never_absence_evidence(self):
        for raw in (None, "", "무향료", "정제수, 향료 무첨가", "정제수, 글리세린…", "상세페이지 참조"):
            self.assertEqual("unknown", label_evidence("p1", raw, None, None)["status"])
        row = label_evidence("p1", "정제수, 라벤더오일", None, None)
        self.assertEqual("not_listed", row["status"])
        self.assertNotIn("manufacturer_claim", row)

    def test_csv_preserves_json_and_original_raw_hash(self):
        raw = "전성분: 정제수, 글리세린"
        claim = {"quote": '제조사 표시: "향료 무첨가"', "product_id": "p1"}
        df = pd.DataFrame([{
            "product_id": "p1", "product_name": '테스트, 토너', "product_brand": "테스트",
            "category": "토너", "product_ingredients_raw": raw,
            "product_url": "https://example.org/p1", "crawled_at": pd.Timestamp("2026-09-28"),
        }])
        output = attach_fragrance_evidence(df, {"p1": claim})
        header, data = build_node_csv(output, PRODUCT_COLUMNS)
        rows = list(csv.reader(io.StringIO(header + data)))
        evidence = json.loads(rows[1][4])
        self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(), evidence["label_sha256"])
        self.assertEqual(claim, evidence["manufacturer_claim"])
        self.assertEqual("not_listed", evidence["status"])
        self.assertNotIn("fragrance_evidence", df.columns)


if __name__ == "__main__":
    unittest.main()
