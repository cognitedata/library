"""The slices of a batch, and the chunks of ids, are read at the same time and come back in order."""

import threading
from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as f
from cq import reader

VIEW = ViewId("cdf_cdm", "CogniteAsset", "v1")


def test_the_slices_of_a_batch_are_read_together_and_returned_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    together = threading.Barrier(2, timeout=2)  # a read that waits for another one only finishes if they overlap

    def read_slice(_client: Any, _type: str, _view: ViewId, _spaces: Any, part: Any) -> list[str]:
        together.wait()
        return [f"from {part['prefix']['value']}"]

    monkeypatch.setattr(reader, "_read_slice", read_slice)
    flt = f.Or(f.Prefix(["node", "externalId"], "A"), f.Prefix(["node", "externalId"], "B"))

    found = reader._list_instances(None, "node", VIEW, ["sp"], flt)  # type: ignore[arg-type]

    assert found == ["from A", "from B"]


def test_the_chunks_of_ids_are_read_together_and_returned_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    together = threading.Barrier(2, timeout=2)
    pairs = [("sp", f"id-{index}") for index in range(reader.RETRIEVE_CHUNK + 1)]
    monkeypatch.setattr(reader, "_id_pairs", lambda *_args: pairs)

    def retrieve(_client: Any, _type: str, _view: ViewId, chunk: list[tuple[str, str]]) -> list[tuple[str, str]]:
        together.wait()
        return chunk

    monkeypatch.setattr(reader, "_retrieve", retrieve)

    found = reader._list_slice_by_id(None, "node", VIEW, ["sp"], object())  # type: ignore[arg-type]

    assert found == pairs


def test_a_slice_of_a_batch_is_read_unsorted_and_a_read_with_no_filter_is_sorted() -> None:
    from unittest.mock import MagicMock

    client = MagicMock()
    client.data_modeling.instances.list.return_value = []

    reader._list_slice(client, "node", VIEW, ["sp"], {"prefix": {"property": ["node", "externalId"], "value": "3"}})
    reader._list_slice(client, "node", VIEW, ["sp"], None)

    sliced, whole = client.data_modeling.instances.list.call_args_list
    assert sliced.kwargs["sort"] is None
    assert whole.kwargs["sort"] is not None
