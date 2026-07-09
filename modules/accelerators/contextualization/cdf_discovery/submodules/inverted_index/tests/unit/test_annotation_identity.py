"""Unit tests for annotation identity configuration and key builders."""

import pytest

from inverted_index.annotation_fields import (
    build_detection_key,
    build_deterministic_annotation_external_id,
)
from inverted_index.annotation_identity import (
    DEFAULT_ANNOTATION_IDENTITY,
    compute_bbox_hash,
    compute_text_hash,
    resolve_annotation_identity,
    validate_annotation_identity,
)
from inverted_index.config_loader import build_runtime_config


_BBOX = [0.1, 0.2, 0.3, 0.4]


def test_resolve_annotation_identity_defaults_when_omitted() -> None:
    resolved = resolve_annotation_identity(None)
    assert resolved == DEFAULT_ANNOTATION_IDENTITY


def test_builtin_detection_key_unchanged_with_defaults() -> None:
    bbox_hash = compute_bbox_hash(_BBOX, decimal_places=4, hex_length=8)
    key = build_detection_key(
        page=3,
        bbox=_BBOX,
        normalized_term="p101a",
        file_external_id="FILE_PID_12",
    )
    assert key == f"page3:bbox_{bbox_hash}:p101a"


def test_builtin_annotation_external_id_with_defaults() -> None:
    bbox_hash = compute_bbox_hash(_BBOX, decimal_places=4, hex_length=8)
    text_hash = compute_text_hash("p101a", hex_length=8)
    ann_id = build_deterministic_annotation_external_id(
        "FILE_PID_12",
        page=3,
        normalized_term="p101a",
        bbox=_BBOX,
    )
    assert ann_id == f"idx_ann_FILE_PID_12_3_{text_hash}_{bbox_hash}"


def test_custom_prefix_changes_annotation_external_id() -> None:
    cfg = {
        "identity": {
            "annotation_external_id_prefix": "custom_ann",
        }
    }
    ann_id = build_deterministic_annotation_external_id(
        "FILE",
        page=1,
        normalized_term="tag",
        bbox=_BBOX,
        annotation_config=cfg,
    )
    assert ann_id.startswith("custom_ann_FILE_")


def test_jinja_detection_key_template_override() -> None:
    cfg = {
        "identity": {
            "detection_key_template": "{{ page_label }}:custom:{{ term_prefix }}",
        }
    }
    key = build_detection_key(
        page=2,
        bbox=_BBOX,
        normalized_term="pump-a",
        file_external_id="FILE",
        annotation_config=cfg,
    )
    assert key == "page2:custom:pump-a"


def test_jinja_template_validation_rejects_undefined_variable() -> None:
    errors = validate_annotation_identity(
        {
            **DEFAULT_ANNOTATION_IDENTITY,
            "detection_key_template": "{{ missing_var }}",
        }
    )
    assert any("detection_key_template" in err for err in errors)


def test_validate_rejects_invalid_prefix() -> None:
    errors = validate_annotation_identity(
        {
            **DEFAULT_ANNOTATION_IDENTITY,
            "annotation_external_id_prefix": "bad prefix!",
        }
    )
    assert any("annotation_external_id_prefix" in err for err in errors)


def test_build_runtime_config_validates_identity() -> None:
    with pytest.raises(ValueError, match="identity.hash_hex_length"):
        build_runtime_config(
            {
                "annotation_index_config": {
                    "identity": {"hash_hex_length": 2},
                }
            }
        )
