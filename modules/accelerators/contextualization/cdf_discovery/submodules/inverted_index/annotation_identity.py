"""Configurable diagram annotation identity keys (detection_key, annotation_external_id)."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from inverted_index.jinja_render import render_template_string

DEFAULT_ANNOTATION_IDENTITY: dict[str, Any] = {
    "annotation_external_id_prefix": "idx_ann",
    "detection_key_term_prefix_length": 12,
    "bbox_hash_decimal_places": 4,
    "hash_hex_length": 8,
    "external_id_limit": 256,
    "detection_key_template": None,
    "annotation_external_id_template": None,
}

_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_TEMPLATE_SMOKE_CONTEXT: dict[str, Any] = {
    "file_external_id": "FILE_SMOKE",
    "normalized_term": "p101a",
    "page": 3,
    "page_label": "page3",
    "page_external_id": "3",
    "bbox": [0.1, 0.2, 0.3, 0.4],
    "bbox_hash": "abcd1234",
    "text_hash": "ef567890",
    "term_prefix": "p101a",
    "prefix": "idx_ann",
    "external_id_limit": 256,
    "digest16": "0123456789abcdef",
}


def resolve_annotation_identity(cfg: dict | None) -> dict[str, Any]:
    """Merge ``annotation_index_config.identity`` with module defaults."""
    identity = dict(DEFAULT_ANNOTATION_IDENTITY)
    if not cfg:
        return identity
    raw = cfg.get("identity")
    if isinstance(raw, dict):
        for key, val in raw.items():
            if key in DEFAULT_ANNOTATION_IDENTITY:
                identity[key] = val
    return identity


def _positive_int(value: Any, name: str, *, minimum: int = 1, maximum: int) -> str | None:
    if not isinstance(value, int) or isinstance(value, bool):
        return f"identity.{name} must be an integer"
    if value < minimum or value > maximum:
        return f"identity.{name} must be between {minimum} and {maximum}"
    return None


def validate_annotation_identity(identity: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    prefix = identity.get("annotation_external_id_prefix")
    if prefix is not None:
        prefix_str = str(prefix).strip()
        if not prefix_str or not _PREFIX_PATTERN.match(prefix_str):
            errors.append(
                "identity.annotation_external_id_prefix must be non-empty alphanumeric/underscore/dash"
            )

    for err in (
        _positive_int(
            identity.get("detection_key_term_prefix_length"),
            "detection_key_term_prefix_length",
            minimum=1,
            maximum=128,
        ),
        _positive_int(
            identity.get("bbox_hash_decimal_places"),
            "bbox_hash_decimal_places",
            minimum=0,
            maximum=12,
        ),
        _positive_int(
            identity.get("hash_hex_length"),
            "hash_hex_length",
            minimum=4,
            maximum=64,
        ),
        _positive_int(
            identity.get("external_id_limit"),
            "external_id_limit",
            minimum=32,
            maximum=512,
        ),
    ):
        if err:
            errors.append(err)

    for template_key in ("detection_key_template", "annotation_external_id_template"):
        template = identity.get(template_key)
        if template is None or template == "":
            continue
        if not isinstance(template, str):
            errors.append(f"identity.{template_key} must be a string or null")
            continue
        if len(template) > 2000:
            errors.append(f"identity.{template_key} exceeds maximum length 2000")
            continue
        try:
            rendered = render_template_string(template, _TEMPLATE_SMOKE_CONTEXT).strip()
        except Exception as exc:
            errors.append(f"identity.{template_key} template error: {exc}")
            continue
        if not rendered:
            errors.append(f"identity.{template_key} rendered to an empty string")

    return errors


def compute_bbox_hash(
    bbox: list[float] | None,
    *,
    decimal_places: int,
    hex_length: int,
) -> str:
    if not bbox or len(bbox) != 4:
        return "nobbox"
    rounded = ",".join(f"{float(v):.{decimal_places}f}" for v in bbox)
    return hashlib.sha256(rounded.encode("utf-8")).hexdigest()[:hex_length]


def compute_text_hash(normalized_term: str, *, hex_length: int) -> str:
    return hashlib.sha256(normalized_term.encode("utf-8")).hexdigest()[:hex_length]


def build_identity_context(
    file_external_id: str,
    *,
    page: int | None,
    normalized_term: str,
    bbox: list[float] | None,
    identity: dict[str, Any],
) -> dict[str, Any]:
    decimal_places = int(identity.get("bbox_hash_decimal_places", 4))
    hex_length = int(identity.get("hash_hex_length", 8))
    term_len = int(identity.get("detection_key_term_prefix_length", 12))
    prefix = str(identity.get("annotation_external_id_prefix", "idx_ann"))
    limit = int(identity.get("external_id_limit", 256))

    bbox_hash = compute_bbox_hash(
        bbox, decimal_places=decimal_places, hex_length=hex_length
    )
    text_hash = compute_text_hash(normalized_term, hex_length=hex_length)
    term_prefix = normalized_term[:term_len] if normalized_term else "noterm"
    page_label = f"page{page}" if page is not None else "page0"
    page_external_id = str(page) if page is not None else "0"

    raw_primary = (
        f"{prefix}_{file_external_id}_{page_external_id}_{text_hash}_{bbox_hash}"
    )
    digest16 = hashlib.sha256(raw_primary.encode("utf-8")).hexdigest()[:16]

    return {
        "file_external_id": file_external_id,
        "normalized_term": normalized_term,
        "page": page,
        "page_label": page_label,
        "page_external_id": page_external_id,
        "bbox": bbox,
        "bbox_hash": bbox_hash,
        "text_hash": text_hash,
        "term_prefix": term_prefix,
        "prefix": prefix,
        "external_id_limit": limit,
        "digest16": digest16,
    }


def apply_external_id_limit(
    raw: str,
    *,
    file_external_id: str,
    identity: dict[str, Any],
) -> str:
    limit = int(identity.get("external_id_limit", 256))
    prefix = str(identity.get("annotation_external_id_prefix", "idx_ann"))
    if len(raw) <= limit:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    short_file = file_external_id[:40]
    return f"{prefix}_{digest}_{short_file}"[:limit]
