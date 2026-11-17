"""Times, for one slice of one kind, the requests a batch makes to read it, to see which one is slow.

A call to `probe_slice` writes nothing and takes a minute or two, where running the batch takes ten.
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
from cq.reader import KIND_VIEWS
from cq.types import RunConfig

PAGE = 1000


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

    -> the seconds each request took and what it returned: the count of the kind's instances with that prefix, the ids
    of the space with that prefix whatever their kind (what a read by id lists), the same with the kind's data, and
    the properties of a few of the kind's instances.
    """
    space, kind, prefix = payload["space"], payload.get("kind", "assets"), payload.get("prefix", "")
    view_space, xid, edge = KIND_VIEWS[kind]
    view = ViewId(view_space, xid, "v1")
    instance_type = "edge" if edge else "node"
    by_prefix = f.Prefix([instance_type, "externalId"], prefix) if prefix else None
    order = [InstanceSort([instance_type, "space"]), InstanceSort([instance_type, "externalId"])]
    instances = client.data_modeling.instances
    steps: dict[str, Any] = {}

    def count() -> dict[str, Any]:
        found = instances.aggregate(
            view, Count("externalId"), filter=by_prefix, space=[space], instance_type=instance_type
        )
        return {"count": int(found.value or 0)}

    def ids(flt: Any) -> Callable[[], dict[str, Any]]:
        def request() -> dict[str, Any]:
            page = instances.list(instance_type=instance_type, space=[space], filter=flt, sort=order, limit=PAGE)
            return {"returned": len(page), "sample": [item.external_id for item in page[:3]]}

        return request

    with_data = f.And(f.HasData(views=[view]), by_prefix) if by_prefix is not None else f.HasData(views=[view])

    def properties() -> dict[str, Any]:
        page = instances.list(
            instance_type=instance_type, sources=view, space=[space], filter=by_prefix, sort=order, limit=20
        )
        return {"returned": len(page)}

    _step(steps, "count of the kind with the prefix", count)
    _step(steps, f"{PAGE} ids of the space with the prefix, any kind", ids(by_prefix))
    _step(steps, f"{PAGE} ids with data in the kind's view and the prefix", ids(with_data))
    _step(steps, "20 instances with their properties", properties)
    return {
        "status": "succeeded",
        "action": "probe_slice",
        "space": space,
        "kind": kind,
        "prefix": prefix,
        "steps": steps,
        "events": resilience.events(),
    }
