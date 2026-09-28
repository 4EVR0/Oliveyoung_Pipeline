"""Label observations, never an inference that an unlisted ingredient is absent."""

import hashlib
import re
import unicodedata

PARSER_VERSION = "fragrance-label-v1"
_FRAGRANCE = re.compile(r"^(?:향료|fragrance|parfum)(?:\s*\([^)]*\))?$", re.I)
_RELATED = {
    "리날룰": "LINALOOL", "linalool": "LINALOOL",
    "파네솔": "FARNESOL", "farnesol": "FARNESOL",
    "리모넨": "LIMONENE", "limonene": "LIMONENE",
}


def label_evidence(product_id, raw, source_url, observed_at):
    """Observe exact ingredient entries; marketing prose is not an ingredient."""
    raw = raw if isinstance(raw, str) else ""
    text = unicodedata.normalize("NFKC", raw).strip()
    text = re.sub(r"^전성분(?:명)?\s*[:：]?\s*", "", text)
    entries = [item.strip() for item in re.split(r"[,，;；\n\r]", text) if item.strip()]
    present = sorted({item for item in entries if _FRAGRANCE.fullmatch(item)})
    related = sorted({name for item in entries
                      for alias, name in _RELATED.items()
                      if re.fullmatch(re.escape(alias) + r"(?:\s*\([^)]*\))?", item, re.I)})
    # Short/partial strings and explanatory prose do not provide a usable label.
    usable = len(entries) >= 2 and not re.search(
        r"\.\.\.|…|상세\s*(?:페이지|설명)|별도\s*표기|무첨가|fragrance[- ]free|향료\s*없", text, re.I,
    )
    return {
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "product_id": str(product_id),
        "status": "present" if present else "not_listed" if usable else "unknown",
        "present_terms": present,
        "related_terms": related,
        "label_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest() if raw else "",
        "source_url": str(source_url) if isinstance(source_url, str) else "",
        "observed_at": observed_at.isoformat() if hasattr(observed_at, "isoformat") else str(observed_at or ""),
    }
