from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cognite.client.data_classes.data_modeling.ids import ViewId

from cdf_fn_common.etl_dm_query import (  # noqa: E402
    RESULT_SET_KEY,
    _resolve_property_names,
    query_all_view_instances,
    view_cache_key,
    ViewQueryStats,
)


def test_resolve_property_names_strips_external_id_from_explicit_list() -> None:
    names = _resolve_property_names({"include_properties": ["externalId", "name", "aliases"]})
    assert names == ["name", "aliases"]


def test_resolve_property_names_cache_avoids_second_retrieve() -> None:
    view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
    view = SimpleNamespace(properties={"name": object(), "aliases": object()})
    client = MagicMock()
    client.data_modeling.views.retrieve.return_value = [view]
    cache: dict = {}

    names1 = _resolve_property_names(
        {}, client=client, view_id=view_id, property_names_cache=cache
    )
    names2 = _resolve_property_names(
        {}, client=client, view_id=view_id, property_names_cache=cache
    )
    assert names1 == names2 == ["aliases", "name"]
    assert client.data_modeling.views.retrieve.call_count == 1
    assert view_cache_key(view_id) in cache


def test_resolve_property_names_uses_view_schema_when_available() -> None:
    view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
    view = SimpleNamespace(properties={"name": object(), "aliases": object()})
    client = MagicMock()
    client.data_modeling.views.retrieve.return_value = [view]

    names = _resolve_property_names({}, client=client, view_id=view_id)
    assert names == ["aliases", "name"]


def test_query_all_view_instances_yields_cursor_pages() -> None:
    n1 = SimpleNamespace(external_id="a1", space="sp")
    n2 = SimpleNamespace(external_id="a2", space="sp")

    page1 = MagicMock()
    page1.cursors = {RESULT_SET_KEY: "cursor-2"}
    page1.rows = [n1]

    page2 = MagicMock()
    page2.cursors = {}
    page2.rows = [n2]

    client = MagicMock()
    client.data_modeling.instances.query.side_effect = [page1, page2]

    view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
    stats = ViewQueryStats()
    nodes = list(
        query_all_view_instances(
            client,
            view_id=view_id,
            cfg={"batch_size": 500},
            stats_out=stats,
        )
    )
    assert len(nodes) == 2
    assert stats.page_count == 2
    assert stats.instances_yielded == 2
    assert stats.api == "instances.query"
    assert client.data_modeling.instances.query.call_count == 2


def test_query_all_view_instances_requires_query_api() -> None:
    client = MagicMock()
    client.data_modeling.instances.query = None
    view_id = ViewId(space="s", external_id="V", version="v1")
    with pytest.raises(TypeError, match="instances.query is required"):
        list(query_all_view_instances(client, view_id=view_id, dm_filter=MagicMock()))
