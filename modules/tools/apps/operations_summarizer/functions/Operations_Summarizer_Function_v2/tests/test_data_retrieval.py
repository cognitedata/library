"""Instance reads must pass the whole cap to the SDK and filter inclusively on the start time."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from cognite.client.data_classes import filters
from data_retrieval import _build_created_time_filter, _paginate

_FILTER = filters.Equals(["node", "space"], "x")


def _client_with_rows(total: int) -> tuple[MagicMock, list[int | None]]:
    """Client whose ``instances.list`` has the pinned SDK 7 shape: no cursor kwarg, at most ``limit`` rows."""
    limits: list[int | None] = []

    def list_instances(
        instance_type: str = "node",
        include_typing: bool = False,
        sources: Any = None,
        space: str | None = None,
        limit: int | None = 25,
        sort: Any = None,
        filter: Any = None,
    ) -> list[int]:
        limits.append(limit)
        return list(range(min(limit if limit is not None else total, total)))

    client = MagicMock()
    client.data_modeling.instances.list.side_effect = list_instances
    return client, limits


def test_reads_past_one_page() -> None:
    client, _ = _client_with_rows(2500)
    rows, truncated, had_more = _paginate(client, None, _FILTER, None, max_instances=15_000)
    assert len(rows) == 2500
    assert not truncated
    assert not had_more


def test_cap_sets_truncation() -> None:
    client, limits = _client_with_rows(1500)
    rows, truncated, had_more = _paginate(client, None, _FILTER, None, max_instances=1000)
    assert len(rows) == 1000
    assert truncated
    assert had_more
    assert limits == [1001]


def test_exactly_at_cap_is_not_truncated() -> None:
    client, _ = _client_with_rows(1000)
    rows, truncated, had_more = _paginate(client, None, _FILTER, None, max_instances=1000)
    assert len(rows) == 1000
    assert not truncated
    assert not had_more


def test_start_is_inclusive() -> None:
    flt = _build_created_time_filter(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC))
    assert "gte" in str(flt.dump())
