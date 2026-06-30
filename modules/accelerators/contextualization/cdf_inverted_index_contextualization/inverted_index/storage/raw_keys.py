"""RAW key helpers and posting merge utilities."""

from __future__ import annotations

import hashlib
import re

import regex as re_u

from inverted_index.config import (
    PARTITION_STRATEGY_TERM_FIRST_CHAR,
    PARTITION_STRATEGY_UNIFIED,
)

CDF_RAW_TABLE_NAME_MAX_LEN = 64
TABLE_PREFIX = "inverted_index__"

LATIN_BUCKETS = tuple("abcdefghijklmnopqrstuvwxyz0123456789")
SCRIPT_BUCKET_SLUGS = ("hira", "kata", "hangul", "cyrillic", "other")


def scope_slug(match_scope_key: str) -> str:
    slug = re.sub(r"[^a-z0-9_]+", "_", match_scope_key.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "default"


def _term_partition_config(config: dict) -> dict:
    return config.get("term_partition") or {}


def _bucket_mode(config: dict) -> str:
    return str(_term_partition_config(config).get("bucket_mode") or "script_aware")


def _unicode_script_bucket(char: str) -> str:
    if re_u.match(r"\p{Script=Hiragana}", char):
        return "hira"
    if re_u.match(r"\p{Script=Katakana}", char):
        return "kata"
    if re_u.match(r"\p{Script=Han}", char):
        return "han"
    if re_u.match(r"\p{Script=Hangul}", char):
        return "hangul"
    if re_u.match(r"\p{Script=Cyrillic}", char):
        return "cyrillic"
    return "other"


def term_bucket(normalized_term: str, *, bucket_mode: str = "script_aware") -> str:
    """Map normalized_term to an ASCII-safe RAW table bucket slug."""
    if not normalized_term:
        return "other"
    first = normalized_term[0]
    if "a" <= first <= "z":
        return first
    if "0" <= first <= "9":
        return first
    if bucket_mode == "ascii_first_char":
        return "other"
    script = _unicode_script_bucket(first)
    if script == "han":
        return f"han_{ord(first) // 0x100:02x}"
    if script in SCRIPT_BUCKET_SLUGS:
        return script
    return "other"


def all_term_bucket_slugs(*, bucket_mode: str = "script_aware") -> list[str]:
    """Deterministic bucket list for purge/iter across a sharded scope."""
    buckets: list[str] = list(LATIN_BUCKETS)
    if bucket_mode == "ascii_first_char":
        buckets.append("other")
        return buckets
    buckets.extend(SCRIPT_BUCKET_SLUGS)
    for block in range(256):
        buckets.append(f"han_{block:02x}")
    return buckets


def _fit_table_name(base_without_bucket: str, bucket_suffix: str, hash_source: str) -> str:
    """Ensure table name fits CDF 64-char limit; bucket_suffix includes leading ``__``."""
    candidate = f"{base_without_bucket}{bucket_suffix}"
    if len(candidate) <= CDF_RAW_TABLE_NAME_MAX_LEN:
        return candidate
    digest = hashlib.sha256(hash_source.encode("utf-8")).hexdigest()[:12]
    fixed_len = len(TABLE_PREFIX) + 1 + len(digest) + len(bucket_suffix)
    budget = CDF_RAW_TABLE_NAME_MAX_LEN - fixed_len
    slug_part = base_without_bucket[len(TABLE_PREFIX) :]
    trimmed = slug_part[: max(budget, 0)].rstrip("_")
    return f"{TABLE_PREFIX}{trimmed}_{digest}{bucket_suffix}"


def resolve_raw_partition_table(
    match_scope_key: str,
    config: dict,
    *,
    normalized_term: str | None = None,
    term_bucket_slug: str | None = None,
    partition_strategy: str = PARTITION_STRATEGY_UNIFIED,
) -> str:
    slug = scope_slug(match_scope_key)
    hash_source = match_scope_key

    if partition_strategy == PARTITION_STRATEGY_TERM_FIRST_CHAR:
        bucket = term_bucket_slug
        if bucket is None:
            if not normalized_term:
                raise ValueError(
                    "normalized_term or term_bucket_slug required for term_first_char partition"
                )
            bucket = term_bucket(normalized_term, bucket_mode=_bucket_mode(config))
        template = _term_partition_config(config).get(
            "sharded_table_template",
            "inverted_index__{scope_slug}__{term_bucket}",
        )
        name = template.format(scope_slug=slug, term_bucket=bucket)
        if len(name) <= CDF_RAW_TABLE_NAME_MAX_LEN:
            return name
        unified_template = config.get("raw", {}).get(
            "table_template", "inverted_index__{scope_slug}"
        )
        base = unified_template.format(scope_slug=slug)
        if len(base) > CDF_RAW_TABLE_NAME_MAX_LEN:
            digest = hashlib.sha256(hash_source.encode("utf-8")).hexdigest()[:12]
            budget = CDF_RAW_TABLE_NAME_MAX_LEN - len(TABLE_PREFIX) - len(digest) - 1
            base = f"{TABLE_PREFIX}{slug[:budget].rstrip('_')}_{digest}"
        return _fit_table_name(base, f"__{bucket}", hash_source)

    template = config.get("raw", {}).get("table_template", "inverted_index__{scope_slug}")
    name = template.format(scope_slug=slug)
    if len(name) <= CDF_RAW_TABLE_NAME_MAX_LEN:
        return name
    digest = hashlib.sha256(hash_source.encode("utf-8")).hexdigest()[:12]
    budget = CDF_RAW_TABLE_NAME_MAX_LEN - len(TABLE_PREFIX) - len(digest) - 1
    return f"{TABLE_PREFIX}{slug[:budget].rstrip('_')}_{digest}"


def list_scope_partition_tables(
    match_scope_key: str,
    config: dict,
    *,
    partition_strategy: str = PARTITION_STRATEGY_UNIFIED,
) -> list[str]:
    if partition_strategy != PARTITION_STRATEGY_TERM_FIRST_CHAR:
        return [resolve_raw_partition_table(match_scope_key, config)]
    mode = _bucket_mode(config)
    return [
        resolve_raw_partition_table(
            match_scope_key,
            config,
            term_bucket_slug=bucket,
            partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
        )
        for bucket in all_term_bucket_slugs(bucket_mode=mode)
    ]


def build_raw_postings_row_key(match_scope_key: str, normalized_term: str) -> str:
    return f"{match_scope_key}::{normalized_term}"


def posting_from_index_entry(entry: dict) -> dict:
    skip = {"normalized_term", "match_scope_key", "external_id"}
    return {k: v for k, v in entry.items() if k not in skip}


def posting_matches_reference(
    posting: dict,
    *,
    reference_external_id: str,
    reference_space: str,
    source_types: list[str],
) -> bool:
    """True when a posting belongs to the given reference and source-type subset."""
    if source_types and posting.get("source_type") not in source_types:
        return False
    if posting.get("reference_external_id") != reference_external_id:
        return False
    if (
        reference_space
        and posting.get("reference_space")
        and posting.get("reference_space") != reference_space
    ):
        return False
    return True


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
