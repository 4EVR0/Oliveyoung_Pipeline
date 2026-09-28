"""Read-only audit: python -m scripts.audit_fragrance_labels. No uploads/graph writes."""

from collections import Counter
from datetime import datetime, timezone
import json

from config.settings import OliveyoungIceberg
from gold_pipeline.fragrance_evidence import label_evidence


def main():
    table = OliveyoungIceberg.get_catalog().load_table(OliveyoungIceberg.SILVER_CURRENT_TABLE)
    snapshot = table.current_snapshot()
    # Pin the audit to one snapshot even if ingestion commits while we are reading.
    df = table.scan(snapshot_id=snapshot.snapshot_id, selected_fields=(
        "product_id", "product_ingredients_raw", "product_url", "crawled_at",
    )).to_pandas().dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"])
    observations = [label_evidence(row.product_id, row.product_ingredients_raw,
                                  row.product_url, row.crawled_at) for row in df.itertuples()]
    print(json.dumps({
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "table": OliveyoungIceberg.SILVER_CURRENT_TABLE,
        "snapshot_id": snapshot.snapshot_id,
        "products": len(observations),
        "label_status": dict(Counter(row["status"] for row in observations)),
        "related_signal_products": sum(bool(row["related_terms"]) for row in observations),
        "with_source_url": sum(bool(row["source_url"]) for row in observations),
        "observed_at_min": min((row["observed_at"] for row in observations), default=None),
        "observed_at_max": max((row["observed_at"] for row in observations), default=None),
        "note": "not_listed is NOT fragrance-free; no manufacturer claims reviewed by this audit",
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
