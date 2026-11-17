"""A slice the service times out on is cut finer and read in pieces instead of by id in one long run."""

from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.exceptions import CogniteAPIError
from cq import reader, resilience
from cq.batching import Leaf, leaf_filter

VIEW = ViewId("cdf_cdm", "CogniteAsset", "v1")
BIG = leaf_filter(("prefix", "FLOC-5"))


def fake_world(monkeypatch: pytest.MonkeyPatch, total: int) -> list[str]:
    """A service that times out on the big slice, lists a slice of the pieces' size, and counts leaves."""
    listed: list[str] = []

    def list_slice(_client: Any, _type: str, _view: ViewId, _spaces: Any, part: Any) -> list[str]:
        text = reader._prefix_of(part)
        if text == "FLOC-5":
            raise CogniteAPIError("timed out", code=408)
        listed.append(str(text))
        return [f"row {text}"]

    def tools(*_args: Any, **_kwargs: Any) -> tuple[Any, Any]:
        sizes = {"FLOC-5": total}

        def count(leaves: list[Leaf]) -> list[int]:
            # each next character holds a fifth of what its prefix holds, down to nothing
            return [sizes.get(text, total // 5) if kind == "prefix" else 0 for kind, text in leaves]

        return count, lambda _leaf: None

    monkeypatch.setattr(reader, "_list_slice", list_slice)
    monkeypatch.setattr(reader, "leaf_tools", tools)
    monkeypatch.setattr(reader, "SLICE_CUT_TARGET", 5000)
    return listed


def test_a_big_slice_that_times_out_is_read_as_finer_slices(monkeypatch: pytest.MonkeyPatch) -> None:
    listed = fake_world(monkeypatch, total=20000)
    resilience.begin_call()

    found = reader._read_slice(None, "node", VIEW, ["sp"], BIG)  # type: ignore[arg-type]

    assert len(listed) > 1  # read in pieces, none of them the big slice
    assert all(text.startswith("FLOC-5") and text != "FLOC-5" for text in listed)
    assert sorted(found) == sorted(f"row {text}" for text in listed)
    assert resilience.events().get("slice cut finer") == 1
    assert "read by id" not in resilience.events()


def test_a_slice_too_small_to_cut_is_read_by_id_as_before(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_world(monkeypatch, total=3000)
    by_id: list[Any] = []
    monkeypatch.setattr(reader, "_list_slice_by_id", lambda *args: by_id.append(args) or ["by id"])
    resilience.begin_call()

    found = reader._read_slice(None, "node", VIEW, ["sp"], BIG)  # type: ignore[arg-type]

    assert found == ["by id"]
    assert resilience.events() == {"read by id": 1}


def test_a_slice_in_several_spaces_is_not_cut(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_world(monkeypatch, total=20000)
    monkeypatch.setattr(reader, "_list_slice_by_id", lambda *_args: ["by id"])
    resilience.begin_call()

    assert reader._read_slice(None, "node", VIEW, ["a", "b"], BIG) == ["by id"]  # type: ignore[arg-type]
