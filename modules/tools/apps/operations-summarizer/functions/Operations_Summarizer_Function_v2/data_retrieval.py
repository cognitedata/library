"""
Data retrieval for Operations Summarizer v2.

Mirrors ``src/utils/summaryGenerationCore/fetchInstances.ts``:
- ``node.createdTime`` range filter for all data types
- Excludes instances in SUMMARY_SPACE
- Caps at MAX_INSTANCES_PER_DATATYPE per type
"""

from typing import List, Any, Optional, Tuple
from datetime import datetime
from cognite.client import CogniteClient
from cognite.client.data_classes import filters

from config import (
    DataTypeConfig,
    VIEW_ID_MAPPING,
    runtime,
)


def _to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _exclude_summary_space_filter():
    return filters.Not(filters.Equals(property=["node", "space"], value=runtime.summary_space))


def _build_created_time_filter(start_dt: datetime, end_dt: datetime) -> filters.Filter:
    """Match fetchInstances.ts base filter exactly."""
    return filters.And(
        filters.Range(
            property=["node", "createdTime"],
            gt=_to_epoch_ms(start_dt),
            lt=_to_epoch_ms(end_dt),
        ),
        _exclude_summary_space_filter(),
    )


def node_property(node, data_type: str, property_name: str, client: Optional[CogniteClient] = None):
    """Extract a property value from a CDF node."""
    try:
        view_id = VIEW_ID_MAPPING.get(data_type)
        if view_id is None:
            return None

        props = node.properties.get(view_id)
        if props is None:
            return None

        return props.get(property_name)
    except Exception:
        return None


def _paginate(
    client: CogniteClient,
    view_id,
    filter_obj: filters.Filter,
    space: Optional[str],
    max_instances: Optional[int] = None,
) -> Tuple[List[Any], bool, bool]:
    """Page through instances.list until cursor exhausted or cap reached.

    Returns (rows, truncated, had_more_in_cdf).
    """
    if max_instances is None:
        max_instances = runtime.max_instances_per_datatype
    rows: List[Any] = []
    cursor: Optional[str] = None
    truncated = False
    had_more_in_cdf = False

    while True:
        kwargs = dict(
            instance_type="node",
            space=space,
            sources=view_id,
            filter=filter_obj,
            include_typing=True,
            limit=runtime.instance_query_limit,
        )
        if cursor:
            kwargs["cursor"] = cursor

        response = client.data_modeling.instances.list(**kwargs)
        batch = list(response) if response else []

        for i, node in enumerate(batch):
            if len(rows) >= max_instances:
                truncated = True
                had_more_in_cdf = i < len(batch) or bool(
                    getattr(response, "next_cursor", None) or getattr(response, "cursor", None)
                )
                return rows, truncated, had_more_in_cdf
            rows.append(node)

        cursor = getattr(response, "next_cursor", None) or getattr(response, "cursor", None)
        if not cursor:
            break

    return rows, truncated, had_more_in_cdf


def fetch_instances_for_data_type(
    client: CogniteClient,
    data_type: str,
    config: DataTypeConfig,
    start_dt: datetime,
    end_dt: datetime,
    space: Optional[str] = None,
) -> Tuple[List[Any], bool, bool]:
    """Fetch instances for one data type using node.createdTime filter.

    Returns (rows, truncated, had_more_in_cdf).
    """
    view_id = config.view_id
    filter_obj = _build_created_time_filter(start_dt, end_dt)

    print(
        f"  FETCH {data_type} | view={view_id.space}/{view_id.external_id}/{view_id.version} "
        f"| time_path=node.createdTime | space={space}"
    )
    rows, truncated, had_more = _paginate(client, view_id, filter_obj, space)
    print(f"  -> {len(rows)} {data_type} nodes" + (" (truncated)" if truncated else ""))
    return rows, truncated, had_more
