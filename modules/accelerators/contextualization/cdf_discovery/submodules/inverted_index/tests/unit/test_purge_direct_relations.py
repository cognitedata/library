"""Unit tests for purge_file_timeseries_direct_relations helpers."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS.parent) not in sys.path:
    sys.path.insert(0, str(SCRIPTS.parent))

from scripts.purge_file_timeseries_direct_relations import (  # noqa: E402
    _direct_relation_is_set,
    _properties_to_clear,
)


def test_direct_relation_is_set() -> None:
    assert not _direct_relation_is_set(None)
    assert not _direct_relation_is_set([])
    assert _direct_relation_is_set([{"space": "s", "externalId": "A"}])
    assert _direct_relation_is_set({"space": "s", "externalId": "A"})


def test_properties_to_clear() -> None:
    cleared = _properties_to_clear(
        {
            "assets": [{"space": "s", "externalId": "A"}],
            "equipment": {"space": "s", "externalId": "EQ"},
            "name": "TS-1",
        },
        ("assets", "equipment"),
    )
    assert cleared == {"assets": [], "equipment": None}

    assert _properties_to_clear({"name": "TS-1"}, ("assets", "equipment")) == {}
