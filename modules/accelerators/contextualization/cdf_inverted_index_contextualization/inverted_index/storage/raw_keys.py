"""RAW key helpers and posting merge utilities."""

from __future__ import annotations

import hashlib
import re

CDF_RAW_TABLE_NAME_MAX_LEN = 64
TABLE_PREFIX = "inverted_index__"


def scope_slug(match_scope_key: str) -> str:
    slug = re.sub(r"[^a-z0-9_]+", "_", match_scope_key.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "default"


def resolve_raw_partition_table(match_scope_key: str, config: dict) -> str:
    slug = scope_slug(match_scope_key)
    template = config.get("raw", {}).get(
        "table_template", "inverted_index__{scope_slug}"
    )
    name = template.format(scope_slug=slug)
    if len(name) <= CDF_RAW_TABLE_NAME_MAX_LEN:
        return name
    digest = hashlib.sha256(match_scope_key.encode("utf-8")).hexdigest()[:12]
    budget = CDF_RAW_TABLE_NAME_MAX_LEN - len(TABLE_PREFIX) - len(digest) - 1
    return f"{TABLE_PREFIX}{slug[:budget].rstrip('_')}_{digest}"


def build_raw_postings_row_key(match_scope_key: str, normalized_term: str) -> str:
    return f"{match_scope_key}::{normalized_term}"


def posting_from_index_entry(entry: dict) -> dict:
    skip = {"normalized_term", "match_scope_key", "external_id"}
    return {k: v for k, v in entry.items() if k not in skip}


def posting_dedupe_key(posting: dict) -> tuple:
    """Dedupe key for RAW postings merge; diagram hits include detection identity."""
    base = (
        posting.get("source_type"),
        posting.get("reference_external_id"),
        posting.get("source_property"),
    )
    source_type = posting.get("source_type") or ""
    if source_type.startswith("diagram_annotation_"):
        meta = posting.get("additional_metadata") or {}
        detection_key = meta.get("detection_key") or meta.get("annotation_external_id")
        if detection_key:
            return (*base, detection_key)
    return base


def merge_postings(existing: list[dict], new: list[dict]) -> list[dict]:
    merged: dict[tuple, dict] = {}
    for posting in existing + new:
        merged[posting_dedupe_key(posting)] = posting
    return list(merged.values())


def flatten_postings_to_entries(
    postings: list[dict],
    *,
    match_scope_key: str,
    normalized_term: str,
) -> list[dict]:
    entries = []
    for posting in postings:
        entry = dict(posting)
        entry["match_scope_key"] = match_scope_key
        entry["normalized_term"] = normalized_term
        entries.append(entry)
    return entries
