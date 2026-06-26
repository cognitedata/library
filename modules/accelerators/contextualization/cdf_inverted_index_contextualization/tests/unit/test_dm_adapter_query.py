from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from cognite.client.data_classes.data_modeling.ids import ViewId

from inverted_index.storage.dm_adapter import DmStorageAdapter


def test_query_by_terms_uses_instances_query_not_list() -> None:
    client = MagicMock()
    client.data_modeling.instances.list = MagicMock(
        side_effect=AssertionError("instances.list must not be called")
    )

    adapter = DmStorageAdapter(
        {"backend": "dm", "dm": {"space": "contextualization_idx"}},
        client,
    )

    with patch("inverted_index.dm_query.query_index_entries") as mock_query:
        mock_query.return_value = [
            {
                "normalized_term": "p101a",
                "source_type": "asset_metadata",
                "additional_metadata": {"confidence": 0.95},
            },
            {
                "normalized_term": "p102b",
                "source_type": "asset_metadata",
                "additional_metadata": {"confidence": 0.4},
            },
        ]
        results = adapter.query_by_terms(
            ["p101a", "p102b"],
            match_scope_key="global",
            source_types=["asset_metadata"],
            min_confidence=0.6,
        )

    assert len(results) == 1
    assert results[0]["normalized_term"] == "p101a"
    mock_query.assert_called_once()
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["normalized_terms"] == ["p101a", "p102b"]
    assert call_kwargs["match_scope_key"] == "global"
    assert call_kwargs["source_types"] == ["asset_metadata"]


def test_query_by_terms_passes_view_and_space_to_query_index_entries() -> None:
    client = MagicMock()
    adapter = DmStorageAdapter(
        {
            "backend": "dm",
            "dm": {
                "space": "contextualization_idx",
                "view": "InvertedIndexEntry",
                "version": "v1",
            },
        },
        client,
    )

    with patch("inverted_index.dm_query.query_index_entries", return_value=[]) as mock_query:
        adapter.query_by_terms(["term1"], match_scope_key="site:A|unit:U1")

    view_id = mock_query.call_args.kwargs["view_id"]
    assert view_id.space == "contextualization_idx"
    assert view_id.external_id == "InvertedIndexEntry"
    assert mock_query.call_args.kwargs["index_space"] == "contextualization_idx"
