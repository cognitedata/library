"""Tests for the Pipeline Health data reads."""

import inspect
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.exceptions import CogniteAPIError

sys.path.insert(0, str(Path(__file__).parent))

import data_fetcher  # isort: skip
from data_fetcher import DataFetcher  # isort: skip
from data_structures import CallerType, ExtractionPipelineConfig, ViewPropertyConfig  # isort: skip

STATE_VIEW = ViewPropertyConfig("dm_sol_file_annotation", "FileAnnotationState", "v1", instance_space="files")


def _state_node(properties: dict[str, object], external_id: str = "state_1") -> MagicMock:
    node = MagicMock(external_id=external_id, space="files", created_time=0, last_updated_time=0)
    node.properties = {ViewId("dm_sol_file_annotation", "FileAnnotationState", "v1"): properties}
    return node


def test_annotation_states_without_file_links_still_have_a_status_column() -> None:
    client = MagicMock()
    client.data_modeling.instances.return_value = iter([[_state_node({"annotationStatus": "New", "attemptCount": 0})]])
    config = ExtractionPipelineConfig(annotation_state_view_cfg=STATE_VIEW)

    states = inspect.unwrap(DataFetcher.fetch_annotation_states)(client, config)

    assert states["status"].tolist() == ["New"]
    assert states["retries"].tolist() == [0]


def test_annotation_states_are_read_in_batches_of_1000() -> None:
    client = MagicMock()
    client.data_modeling.instances.return_value = iter(
        [[_state_node({"annotationStatus": "New"}, "a")], [_state_node({"annotationStatus": "Failed"}, "b")]]
    )
    config = ExtractionPipelineConfig(annotation_state_view_cfg=STATE_VIEW)

    states = inspect.unwrap(DataFetcher.fetch_annotation_states)(client, config)

    assert states["status"].tolist() == ["New", "Failed"]
    assert client.data_modeling.instances.call_args.kwargs["chunk_size"] == 1000
    assert "limit" not in client.data_modeling.instances.call_args.kwargs


def test_files_by_call_id_are_read_from_the_annotation_state_space_in_batches() -> None:
    client = MagicMock()
    client.data_modeling.instances.return_value = iter([])

    inspect.unwrap(DataFetcher.fetch_files_by_function_call_id)(client, 42, STATE_VIEW, CallerType.LAUNCH)

    assert client.data_modeling.instances.call_args.kwargs["space"] == "files"
    assert client.data_modeling.instances.call_args.kwargs["chunk_size"] == 1000


def test_pipelines_are_listed_in_batches_of_1000() -> None:
    client = MagicMock()
    client.extraction_pipelines.return_value = iter(
        [[MagicMock(external_id="ep_file_annotation")], [MagicMock(external_id="ep_other")]]
    )

    pipelines = inspect.unwrap(DataFetcher.find_pipelines)(client)

    assert pipelines == ["ep_file_annotation"]
    client.extraction_pipelines.assert_called_once_with(chunk_size=1000)


def test_rate_limited_reads_give_up_after_a_few_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(data_fetcher.time, "sleep", sleeps.append)
    func = MagicMock(side_effect=CogniteAPIError("Too many requests", code=429))

    with pytest.raises(CogniteAPIError):
        DataFetcher._call_with_retries(func)

    assert func.call_count == 3
    assert max(sleeps) <= 60
