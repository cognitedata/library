"""Unit tests for incremental RAW scope helpers."""

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.incremental_scope import (
    cohort_row_key,
    norm_workflow_status,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)


def test_scope_key_stable():
    k1 = scope_key_from_view_dict(
        {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "instance_space": "sp-x",
            "filters": [{"operator": "IN", "target_property": "mimeType", "values": ["application/pdf"]}],
        }
    )
    k2 = scope_key_from_view_dict(
        {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "instance_space": "sp-x",
            "filters": [{"operator": "IN", "target_property": "mimeType", "values": ["application/pdf"]}],
        }
    )
    assert k1 == k2
    assert len(k1) == 32


def test_cohort_row_key_includes_scope():
    rid = "20260101T000000.000000Z"
    nid = "space:uuid-1"
    sk = "abc123"
    assert cohort_row_key(rid, nid, sk) == f"{rid}:{sk}:{nid}"


def test_norm_workflow_status():
    assert norm_workflow_status("  EXTRACTED ") == "extracted"
    assert norm_workflow_status(None) == ""


def test_watermark_key():
    assert scope_watermark_row_key("deadbeef").startswith("scope_wm_")

