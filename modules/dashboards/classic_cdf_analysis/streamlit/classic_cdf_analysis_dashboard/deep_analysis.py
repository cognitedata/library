"""
Deep analysis: filter key selection and report building.
Port of the TypeScript deepAnalysis module.
"""

PRIMARY_FILTER_KEYS = {
    "assets": [],
    "timeseries": ["is step", "is string", "unit"],
    "events": ["type"],
    "sequences": [],
    "files": ["type", "labels", "author", "source"],
}

SORTING_SUBSTRINGS = [
    "type", "category", "level", "class", "kind", "group", "classification",
    "tier", "grade", "sort", "family", "genre", "style", "variant",
    "division", "rank", "taxonomy", "rubric", "grouping",
]

IDENTIFIER_KEY_PARTS = [
    {"term": "name"},
    {"term": "externalid"},
    {"term": "external_id"},
    {"term": "sourceid"},
    {"term": "source_id"},
    {"term": "tag"},
    {"term": "uuid"},
    {"term": "guid"},
    {"term": "id", "exact_or_suffix": True},
]

ORDER_IDENTIFIER_SUBSTRINGS = [
    "orderid", "order_id", "workorder", "work_order",
    "purchaseorder", "purchase_order", "salesorder", "sales_order",
    "joborder", "job_order",
]

DATETIME_SUBSTRINGS = [
    "date", "time", "datetime", "timestamp", "created", "modified",
    "updated", "due", "utc", "iso8601", "epoch",
]

TOP_METADATA_COUNT = 15


def _is_sorting_like_key(key: str) -> bool:
    lower = key.strip().lower()
    return bool(lower and any(term in lower for term in SORTING_SUBSTRINGS))


def _is_identifier_like_key(key: str) -> bool:
    lower = key.strip().lower()
    if not lower:
        return True
    if lower == "order":
        return True
    for entry in IDENTIFIER_KEY_PARTS:
        term = entry["term"]
        exact_or_suffix = entry.get("exact_or_suffix", False)
        if exact_or_suffix and term == "id":
            if lower == "id" or lower.endswith("_id"):
                return True
        elif lower == term or term in lower:
            return True
    return False


def _is_order_identifier_like_key(key: str) -> bool:
    lower = key.strip().lower()
    if lower == "order":
        return True
    return any(term in lower for term in ORDER_IDENTIFIER_SUBSTRINGS)


def _is_datetime_like_key(key: str) -> bool:
    lower = key.strip().lower()
    return any(term in lower for term in DATETIME_SUBSTRINGS)


def _is_eligible_metadata_key(key: str) -> bool:
    return (
        bool(key.strip())
        and _is_sorting_like_key(key)
        and not _is_identifier_like_key(key)
        and not _is_order_identifier_like_key(key)
        and not _is_datetime_like_key(key)
    )


def select_filter_keys_for_deep_analysis(
    metadata_list: list[dict],
    total_count: int,
    resource_type: str,
    coverage_pct: float = 0.6,
) -> list[str]:
    """
    Select filter keys for deep analysis:
    1. Primary keys for the resource type.
    2. From metadata keys: sorting-like, non-identifier, top 15 by count or meeting coverage threshold.
    """
    primary = PRIMARY_FILTER_KEYS.get(resource_type, [])
    seen = set()
    out = []

    for k in primary:
        key = k.strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)

    eligible = [m for m in metadata_list if _is_eligible_metadata_key(m.get("key", ""))]
    sorted_meta = sorted(eligible, key=lambda x: x.get("count", 0), reverse=True)
    threshold = coverage_pct * total_count if total_count > 0 else 0

    added = 0
    for item in sorted_meta:
        if added >= TOP_METADATA_COUNT:
            break
        key = (item.get("key") or "").strip()
        if not key or key in seen:
            continue
        count = item.get("count", 0)
        if count >= threshold:
            seen.add(key)
            out.append(key)
            added += 1

    return out


def slug_for_file_name(s: str, max_len: int = 40) -> str:
    """Sanitize a string for use in a file name (alphanumeric, dash, underscore)."""
    import re
    out = re.sub(r"\s+", "-", str(s))
    out = re.sub(r"[^a-zA-Z0-9_-]", "", out)
    out = out[:max_len] or "unnamed"
    return out
