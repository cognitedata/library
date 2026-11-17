"""Times, for one slice of one kind, the requests a batch makes to read it, to see which one is slow.

A call to `probe_slice` writes nothing, and every step is bounded, so it ends in a few minutes at most.
"""

import time
from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as f
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.exceptions import CogniteAPIError

from cq import resilience
from cq.batching import CHARSET
from cq.reader import KIND_VIEWS, _in_parallel, _retrieve
from cq.resilience import watched
from cq.types import RunConfig

PAGE = 1000
MAX_PAGES = 30
MAX_PAGING_SECONDS = 40.0


def _step(steps: dict[str, Any], name: str, request: Callable[[], dict[str, Any]]) -> None:
    resilience.phase(name)
    started = time.monotonic()
    try:
        result = request()
    except CogniteAPIError as error:
        result = {"error": error.code}
    steps[name] = {"seconds": round(time.monotonic() - started, 1), **result}


def probe_slice(client: CogniteClient, _cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Payload: `space`, `kind` (assets, equipment, timeseries, ...) and `prefix` (the start of the external ids).

    -> the seconds each request took and what it returned. The ids of a slice are asked for whatever their kind (what a
    read by id lists), and the instances of the kind in different ways, to find one the service answers in time.
    """
    return watched(lambda: _probe(client, payload), reserve=0.0)


def _probe(client: CogniteClient, payload: dict[str, Any]) -> dict[str, Any]:
    space = payload["space"]
    kind = payload.get("kind") or "assets"
    prefix = payload.get("prefix") or ""
    if kind not in KIND_VIEWS:
        raise ValueError(f"Unknown kind {kind!r}. Expected one of {sorted(KIND_VIEWS)}")
    view_space, xid, edge = KIND_VIEWS[kind]
    view = ViewId(view_space, xid, "v1")
    instance_type = "edge" if edge else "node"
    ref = [instance_type, "externalId"]
    by_prefix = f.Prefix(ref, prefix) if prefix else None
    in_range = f.Range(ref, gte=prefix, lt=prefix[:-1] + chr(ord(prefix[-1]) + 1)) if prefix else None
    with_data = f.And(f.HasData(views=[view]), by_prefix) if by_prefix is not None else f.HasData(views=[view])
    order = [InstanceSort([instance_type, "space"]), InstanceSort([instance_type, "externalId"])]
    instances = client.data_modeling.instances
    steps: dict[str, Any] = {}
    listed: list[tuple[str, str]] = []

    def count() -> dict[str, Any]:
        found = instances.aggregate(
            view, Count("externalId"), filter=by_prefix, space=[space], instance_type=instance_type
        )
        return {"count": int(found.value or 0)}

    def first_ids() -> dict[str, Any]:
        page = instances.list(instance_type=instance_type, space=[space], filter=by_prefix, sort=order, limit=PAGE)
        return {"returned": len(page), "sample": [item.external_id for item in page[:3]]}

    def children() -> dict[str, Any]:
        """How many ids (up to 1000) start with the prefix and one more character, for each character."""

        def one(char: str) -> tuple[str, int]:
            page = instances.list(
                instance_type=instance_type, space=[space], filter=f.Prefix(ref, prefix + char), limit=PAGE
            )
            return char, len(page)

        sizes = dict(_in_parallel(one, list(CHARSET)))
        return {"ids per next character, capped at 1000": {c: n for c, n in sizes.items() if n}}

    def paged() -> dict[str, Any]:
        """The ids of the slice, page after page, for at most `MAX_PAGES` pages or `MAX_PAGING_SECONDS` seconds."""
        started = time.monotonic()
        cursor: str | None = None
        pages = 0
        while pages < MAX_PAGES and time.monotonic() - started < MAX_PAGING_SECONDS:
            body: dict[str, Any] = {
                "instanceType": instance_type,
                "includeTyping": False,
                "sort": [item.dump() for item in order],
                "filter": f.And(f.SpaceFilter([space], instance_type), by_prefix).dump(camel_case_property=False)
                if by_prefix is not None
                else f.SpaceFilter([space], instance_type).dump(camel_case_property=False),
                "limit": PAGE,
            }
            if cursor:
                body["cursor"] = cursor
            post = resilience.retrying(client.post)
            response = post(f"/api/v1/projects/{client.config.project}/models/instances/list", json=body).json()
            listed.extend((item["space"], item["externalId"]) for item in response["items"])
            pages += 1
            cursor = response.get("nextCursor")
            if not cursor:
                return {"ids": len(listed), "pages": pages, "more": False}
        return {"ids": len(listed), "pages": pages, "more": True}

    def retrieve_some() -> dict[str, Any]:
        found = _retrieve(client, instance_type, view, listed[:500])
        return {
            "asked": min(500, len(listed)),
            "with data in the view": sum(1 for i in found if i.properties.get(view)),
        }

    def variant(flt: Any, sources: bool, sort: bool) -> Callable[[], dict[str, Any]]:
        def request() -> dict[str, Any]:
            page = instances.list(
                instance_type=instance_type,
                sources=view if sources else None,
                space=[space],
                filter=flt,
                sort=order if sort else None,
                limit=100,
            )
            return {"returned": len(page)}

        return request

    _step(steps, "count of the kind with the prefix", count)
    _step(steps, "first 1000 ids of the space with the prefix, any kind", first_ids)
    _step(steps, "ids of the space per next character", children)
    _step(steps, f"ids of the space with the prefix, any kind, at most {MAX_PAGES} pages", paged)
    _step(steps, "properties of the first 500 of those ids, read by id", retrieve_some)
    if by_prefix is not None and in_range is not None:
        _step(steps, "100 with properties, prefix, no sort", variant(by_prefix, True, False))
        _step(steps, "100 with properties, range, sorted", variant(in_range, True, True))
        _step(steps, "100 with properties, range, no sort", variant(in_range, True, False))
        _step(steps, "100 with data in the view, prefix, no sort", variant(with_data, False, False))
    return {
        "status": "succeeded",
        "action": "probe_slice",
        "space": space,
        "kind": kind,
        "prefix": prefix,
        "steps": steps,
        "events": resilience.events(),
    }
