"""Tests for the retry behaviour when fetching a page of instances.

Transient failures are worth retrying. A client error such as a missing view or a
malformed filter is not - the identical request fails again - so it must surface at once
instead of burning the whole backoff schedule.
"""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError

sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from pipeline import fetch_instances_by_space

VIEW_ID = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the backoff from actually sleeping."""
    monkeypatch.setattr("pipeline.time.sleep", lambda _: None)


def _fetch(client: MagicMock) -> list[Any]:
    return fetch_instances_by_space(
        client,
        CogniteFunctionLogger("ERROR"),
        "inst_location",
        VIEW_ID,
        None,
        "assets",
    )


def test_missing_view_is_not_retried() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = CogniteAPIError(
        "One or more views do not exist: 'dm_dom_oil_and_gas:CogniteAsset/v1'.", code=400
    )

    with pytest.raises(CogniteAPIError):
        _fetch(client)

    assert client.data_modeling.instances.list.call_count == 1


def test_missing_permission_is_not_retried() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = CogniteAPIError("Forbidden", code=403)

    with pytest.raises(CogniteAPIError):
        _fetch(client)

    assert client.data_modeling.instances.list.call_count == 1


def test_rate_limiting_is_retried() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = [CogniteAPIError("Too many requests", code=429), []]

    assert _fetch(client) == []
    assert client.data_modeling.instances.list.call_count == 2


def test_server_error_is_retried() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = [CogniteAPIError("Service unavailable", code=503), []]

    assert _fetch(client) == []
    assert client.data_modeling.instances.list.call_count == 2


def test_connection_error_is_retried() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = [CogniteConnectionError("Connection reset"), []]

    assert _fetch(client) == []
    assert client.data_modeling.instances.list.call_count == 2
