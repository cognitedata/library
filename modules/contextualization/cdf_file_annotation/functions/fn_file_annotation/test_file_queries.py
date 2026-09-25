"""Prepare and Launch read only the file properties they use, with filters DMS can page."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import Node, NodeId, NodeList
from services.ConfigService import Config


def _config(debug_file: str | None = None) -> Config:
    parameters: dict[str, object] = {"rawDb": "db_file_annotation", "primaryScopeProperty": "site"}
    if debug_file:
        parameters["debugFileExternalId"] = debug_file
    return Config.model_validate(
        {
            "parameters": parameters,
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


def _node(space: str, external_id: str, view: tuple[str, str], properties: dict[str, object]) -> Node:
    return Node.load(
        {
            "instanceType": "node",
            "space": space,
            "externalId": external_id,
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {view[0]: {view[1]: properties}},
        }
    )


def _state(external_id: str, file_external_id: str, status: str) -> Node:
    linked = {"space": "files", "externalId": file_external_id}
    return _node(
        "files", external_id, ("sp_hdm", "FileAnnotationState/v1"), {"linkedFile": linked, "annotationStatus": status}
    )


def _file(external_id: str) -> Node:
    return _node("files", external_id, ("cdf_cdm", "CogniteFile/v1"), {"tags": ["ToAnnotate"], "site": "S1"})


def _result(sets: dict[str, list[Node]]) -> MagicMock:
    result = MagicMock()
    result.__getitem__.side_effect = lambda name: sets.get(name, [])
    result.cursors = {}
    return result


def _data_model_service(config: Config, client: MagicMock):
    from services.DataModelService import GeneralDataModelService

    return GeneralDataModelService(config, client, MagicMock())


def test_launch_reads_new_and_stuck_states_in_separate_and_only_reads() -> None:
    """One OR across the New/Retry and the stuck-state filter kept DMS from paging it with an index."""
    client = MagicMock()
    client.data_modeling.instances.query.return_value = _result(
        {
            "stuck_states": [_state("state-2", "f2", "Processing")],
            "stuck_files": [_file("f2")],
            "new_states": [_state("state-1", "f1", "New")],
            "new_files": [_file("f1")],
        }
    )

    files, state_by_file = _data_model_service(_config(), client).get_files_to_process()

    assert files is not None and state_by_file is not None
    assert sorted(file.external_id for file in files) == ["f1", "f2"]
    assert state_by_file[NodeId("files", "f1")].external_id == "state-1"
    client.data_modeling.instances.list.assert_not_called()
    client.data_modeling.instances.retrieve_nodes.assert_not_called()
    (call,) = client.data_modeling.instances.query.call_args_list
    query = call.args[0]
    for name in ("new_states", "stuck_states"):
        assert "'or'" not in str(query.with_[name].filter.dump())
    for name in ("new_files", "stuck_files"):
        assert query.with_[name].through.property == "linkedFile"
        assert set(query.select[name].sources[0].properties) == {"tags", "site"}


def test_launch_in_debug_mode_reads_only_the_debug_file_state() -> None:
    client = MagicMock()
    client.data_modeling.instances.query.return_value = _result({})

    _data_model_service(_config("PID-001"), client).get_files_to_process()

    query = client.data_modeling.instances.query.call_args.args[0]
    assert set(query.with_) == {"new_states", "new_files"}
    state_filter = str(query.with_["new_states"].filter.dump())
    assert "linkedFile" in state_filter and "PID-001" in state_filter


def test_the_reset_query_is_paged_and_reads_only_the_file_tags() -> None:
    first_page = _result({"files": [_file(f"f{i}") for i in range(1000)]})
    first_page.cursors = {"files": "next"}
    client = MagicMock()
    client.data_modeling.instances.query.side_effect = [first_page, _result({"files": [_file("last")]})]
    config = _config()
    config.prepare_function.get_files_for_annotation_reset_query = config.prepare_function.get_files_to_annotate_query

    files = _data_model_service(config, client).get_files_for_annotation_reset()

    assert files is not None and len(files) == 1001
    client.data_modeling.instances.list.assert_not_called()
    for call in client.data_modeling.instances.query.call_args_list:
        assert call.args[0].select["files"].sources[0].properties == ["tags"]


def test_finalize_reads_at_most_one_launch_batch_of_states_per_job() -> None:
    from services.RetrieveService import GeneralRetrieveService

    job_state = _node(
        "files",
        "state-1",
        ("sp_hdm", "FileAnnotationState/v1"),
        {"linkedFile": {"space": "files", "externalId": "f1"}, "diagramDetectJobId": 7},
    )
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = [NodeList([job_state]), NodeList([])]
    service = GeneralRetrieveService(client, _config(), MagicMock())

    service.get_job_id()

    job_lookup = client.data_modeling.instances.list.call_args_list[1]
    assert job_lookup.kwargs["limit"] == _config().launch_function.batch_size


def test_prepare_reads_only_the_file_tags() -> None:
    client = MagicMock()
    client.data_modeling.instances.query.return_value = _result({"files": [_file("f1")]})

    files = _data_model_service(_config(), client).get_files_to_annotate()

    assert [file.external_id for file in files or []] == ["f1"]
    client.data_modeling.instances.list.assert_not_called()
    query = client.data_modeling.instances.query.call_args.args[0]
    assert query.select["files"].sources[0].properties == ["tags"]
    assert "hasData" in str(query.with_["files"].filter.dump())
