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
        "inverted_index.subscription._should_skip_dedupe",
        return_value=False,
    ):
        result = handle_aliases_subscription_event(client, event, dry_run=True)

    assert result.get("status") == "ok"
    proc.assert_called_once()
    assert proc.call_args.kwargs["instance_type"] == "file"
    assert proc.call_args.kwargs["instance_external_id"] == "FILE_DOC_1"


def test_resolve_batch_views_for_file() -> None:
    from inverted_index.target_driven import _resolve_batch_views

    views = _resolve_batch_views(
        instance_type="file",
        subscription_config={"file_views": ["CogniteFile", "MyFile"]},
    )
    assert views == ["CogniteFile", "MyFile"]
