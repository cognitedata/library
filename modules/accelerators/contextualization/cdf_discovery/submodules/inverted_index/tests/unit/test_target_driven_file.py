"""Unit tests for target-driven subscription on CogniteFile."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.subscription import handle_aliases_subscription_event


def test_subscription_accepts_cognite_file_view() -> None:
    client = MagicMock()
    event = {
        "space": "cdf_cdm",
        "externalId": "FILE_DOC_1",
        "view_external_id": "CogniteFile",
        "changed_properties": ["aliases"],
        "after": {"properties": {"aliases": ["P-101A"]}},
    }
    with patch(
        "inverted_index.subscription.process_target_driven_contextualization",
        return_value={"references_found": 1, "links_created": 1},
    ) as proc, patch(
        "inverted_index.subscription.should_skip_target_driven",
        return_value=False,
    ), patch(
        "inverted_index.subscription.build_runtime_config",
        return_value={
            "subscription_config": {"enabled": True, "watch_view_keys": ["asset", "file"]},
            "direct_relation_config": {
                "views": {
                    "file": {
                        "space": "cdf_cdm",
                        "external_id": "CogniteFile",
                        "version": "v1",
                    },
                },
                "links": {},
            },
            "scope_config": {},
            "storage_config": {"backend": "raw", "raw": {"database": "db"}},
            "target_driven_config": {},
        },
    ):
        result = handle_aliases_subscription_event(client, event, dry_run=True)

    assert result.get("status") == "ok"
    proc.assert_called_once()
    assert proc.call_args.kwargs["incoming_view_key"] == "file"
    assert proc.call_args.kwargs["instance_external_id"] == "FILE_DOC_1"


def test_resolve_backfill_view_keys_from_subscription() -> None:
    from inverted_index.target_driven import _resolve_backfill_view_keys

    keys = _resolve_backfill_view_keys(
        direct_relation_config={
            "views": {"asset": {}, "file": {}, "equipment": {}},
        },
        subscription_config={"watch_view_keys": ["file", "equipment"]},
    )
    assert keys == ["file", "equipment"]
