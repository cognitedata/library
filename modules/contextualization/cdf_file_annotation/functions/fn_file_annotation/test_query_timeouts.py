"""A query that keeps timing out fails the stage after a few retries instead of spinning until the budget is spent."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError
from fa_constants import QUERY_TIMEOUT_MAX_RETRIES
from services.ConfigService import Config


def _config() -> Config:
    return Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation"},
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "assets",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "files",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )


def _timeout() -> CogniteAPIError:
    return CogniteAPIError("Graph query timed out. Reduce load or contention, or optimise your query.", code=408)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    return slept


def _claimed_file() -> MagicMock:
    file_node = MagicMock()
    file_node.space = "files"
    file_node.external_id = "file-1"
    file_node.as_id.return_value = NodeId("files", "file-1")
    file_node.properties = {ViewId("cdf_cdm", "CogniteFile", "v1"): {"tags": ["ToAnnotate", "AnnotationInProcess"]}}
    return file_node


def _launch_service(data_model_service: MagicMock, cache_service: MagicMock | None = None):
    from services.LaunchService import GeneralLaunchService

    return GeneralLaunchService(
        client=MagicMock(),
        config=_config(),
        logger=MagicMock(),
        tracker=MagicMock(),
        data_model_service=data_model_service,
        cache_service=cache_service or MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )


def _retries_then_raises(run, sleeps: list[float]) -> None:
    for _ in range(QUERY_TIMEOUT_MAX_RETRIES):
        assert run() is None
    with pytest.raises(CogniteAPIError):
        run()
    assert len(sleeps) == QUERY_TIMEOUT_MAX_RETRIES
    assert sleeps == sorted(sleeps) and sleeps[0] < sleeps[-1]


def test_launch_gives_up_on_a_files_query_that_keeps_timing_out(sleeps: list[float]) -> None:
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.side_effect = _timeout()

    _retries_then_raises(_launch_service(data_model_service).run, sleeps)


def test_launch_releases_its_files_when_the_entity_read_keeps_timing_out(sleeps: list[float]) -> None:
    file_node = _claimed_file()
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.return_value = ([file_node], {file_node.as_id(): MagicMock()})
    cache_service = MagicMock()
    cache_service.get_entities.side_effect = _timeout()

    _retries_then_raises(_launch_service(data_model_service, cache_service).run, sleeps)

    (released,) = data_model_service.update_annotation_state.call_args.args[0]
    assert released.sources[0].properties["tags"] == ["ToAnnotate"]


def test_launch_keeps_reading_entities_in_the_next_run_while_the_read_is_unfinished() -> None:
    from services.EntitySyncService import EntitySyncIncompleteError

    file_node = _claimed_file()
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.return_value = ([file_node], {file_node.as_id(): MagicMock()})
    cache_service = MagicMock()
    cache_service.get_entities.side_effect = EntitySyncIncompleteError("Read 10 entities so far")

    assert _launch_service(data_model_service, cache_service).run() is None
    data_model_service.update_annotation_state.assert_not_called()


def test_prepare_gives_up_on_a_files_query_that_keeps_timing_out(sleeps: list[float]) -> None:
    from services.PrepareService import GeneralPrepareService

    data_model_service = MagicMock()
    data_model_service.get_files_to_annotate.side_effect = _timeout()
    service = GeneralPrepareService(MagicMock(), _config(), MagicMock(), MagicMock(), data_model_service, {})

    _retries_then_raises(service.run, sleeps)


def test_finalize_gives_up_on_a_jobs_query_that_keeps_timing_out(sleeps: list[float]) -> None:
    from services.FinalizeService import GeneralFinalizeService

    retrieve_service = MagicMock()
    retrieve_service.get_job_id.side_effect = _timeout()
    service = GeneralFinalizeService(
        MagicMock(), _config(), MagicMock(), MagicMock(), retrieve_service, MagicMock(), {}
    )

    _retries_then_raises(service.run, sleeps)


def test_the_entity_read_stops_at_the_function_deadline() -> None:
    """A read that runs past the function's time budget would be killed with nothing stored."""
    from services.EntitySyncService import EntitySyncIncompleteError, EntitySyncService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    page = MagicMock()
    page.__getitem__.return_value = [MagicMock(properties=None, deleted_time=None)] * 1000
    page.cursors = {"entities": "partial"}
    client.data_modeling.instances.sync.return_value = page
    service = EntitySyncService(client, _config(), MagicMock(), deadline=time.monotonic())

    with pytest.raises(EntitySyncIncompleteError):
        service.load(_config().data_model_views.target_entities_view, "assets", ["name", "tags"], ["OMD"])

    assert client.data_modeling.instances.sync.call_count == 1
    assert client.raw.rows.insert.call_args.args[2].columns["cursor"] == "partial"
