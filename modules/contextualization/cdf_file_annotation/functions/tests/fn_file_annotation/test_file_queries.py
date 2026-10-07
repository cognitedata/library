"""Prepare and Launch read only the file properties they use, with filters DMS can page."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import Node, NodeId, NodeList
from services.ConfigService import Config


def _config(debug_file: str | None = None) -> Config:
    parameters: dict[str, object] = {"primaryScopeProperty": "site"}
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
    assert "'or'" not in str(query.with_["new_states"].filter.dump())
    for name in ("new_files", "stuck_files"):
        assert query.with_[name].through.property == "linkedFile"
        assert set(query.select[name].sources[0].properties) == {"tags", "site"}
    state_properties = {
        "annotationStatus",
        "linkedFile",
        "annotatedPageCount",
        "pageCount",
        "sourceUpdatedTime",
        "pipelineUpdatedTime",
    }
    assert set(query.select["new_states"].sources[0].properties) == state_properties
    assert query.select["stuck_states"].sources[0].properties == query.select["new_states"].sources[0].properties
    assert "diagramDetectJobToken" not in query.select["new_states"].sources[0].properties
    stuck = str(query.with_["stuck_states"].filter.dump())
    assert "pipelineUpdatedTime" in stuck
    assert "sourceUpdatedTime" in stuck
    assert "exists" in stuck


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


def test_finalize_claims_a_pattern_only_job() -> None:
    from services.ConfigService import build_filter_from_query
    from services.RetrieveService import GeneralRetrieveService

    dumped = str(build_filter_from_query(_config().finalize_function.retrieve_service.get_job_id_query).dump())
    assert "or" in dumped
    assert "diagramDetectJobId" in dumped
    assert "patternModeJobId" in dumped

    job_state = _node(
        "files",
        "state-1",
        ("sp_hdm", "FileAnnotationState/v1"),
        {
            "linkedFile": {"space": "files", "externalId": "f1"},
            "patternModeJobId": 9,
            "patternModeJobToken": "pattern-token",
        },
    )
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = [NodeList([job_state]), NodeList([job_state])]
    service = GeneralRetrieveService(client, _config(), MagicMock())

    regular, pattern, mapping = service.get_job_id()

    assert regular is None
    assert pattern == (9, "pattern-token")
    assert NodeId("files", "f1") in mapping
    sort = client.data_modeling.instances.list.call_args_list[0].kwargs["sort"]
    assert sort[0].property[-1] == "pipelineUpdatedTime"


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


def _single_query(query: object):
    from services.ConfigService import QueryConfig

    assert isinstance(query, QueryConfig)
    return query


def test_tag_filters_use_contains_any_and_status_stays_in() -> None:
    """tags and aliases are lists. annotationStatus is a scalar and stays an In filter."""
    config = _config()
    prepare = _single_query(config.prepare_function.get_files_to_annotate_query)
    prepare_ops = [(item.target_property, item.operator.value, item.negate) for item in prepare.filters]
    assert ("tags", "ContainsAny", False) in prepare_ops
    assert ("tags", "ContainsAny", True) in prepare_ops
    assert "containsAny" in str(prepare.build_filter().dump())

    launch = config.launch_function.data_model_service
    status = _single_query(launch.get_files_to_process_query)
    status_filter = next(item for item in status.filters if item.target_property == "annotationStatus")
    assert status_filter.operator.value == "In"
    for query in (launch.get_target_entities_query, launch.get_file_entities_query):
        entity_query = _single_query(query)
        assert entity_query.filters[0].operator.value == "ContainsAny"
        assert entity_query.filters[0].target_property == "tags"

    candidates = _single_query(config.promote_function.get_candidates_query)
    candidate_ops = [(item.target_property, item.operator.value, item.negate) for item in candidates.filters]
    assert ("status", "Equals", False) in candidate_ops
    assert ("tags", "ContainsAny", True) in candidate_ops
    assert "PromoteAttempted" in str(candidates.build_filter().dump())


def test_debug_prepare_excludes_in_process_tags_with_contains_any() -> None:
    client = MagicMock()
    client.data_modeling.instances.query.return_value = _result({})

    _data_model_service(_config("PID-001"), client).get_files_to_annotate()

    dumped = str(client.data_modeling.instances.query.call_args.args[0].with_["files"].filter.dump())
    assert "containsAny" in dumped
    assert "AnnotationInProcess" in dumped


def test_alias_search_uses_contains_any() -> None:
    from services.EntitySearchService import EntitySearchService

    client = MagicMock()
    client.data_modeling.instances.search.return_value = []
    service = EntitySearchService(_config(), client, MagicMock())

    service.find_global_entity(["P-101"], service.target_entities_view_id, "assets", "P-101")

    alias_call = client.data_modeling.instances.search.call_args_list[0]
    dumped = str(alias_call.kwargs["filter"].dump())
    assert "containsAny" in dumped
    assert "P-101" in dumped


def test_swapped_approval_threshold_is_rejected() -> None:
    from pydantic import ValidationError

    parameters = {
        "assetAutoApprovalThreshold": 0.5,
        "assetAutoSuggestThreshold": 0.8,
    }
    with pytest.raises(ValidationError, match="assetAutoApprovalThreshold"):
        Config.model_validate({"parameters": parameters, "data": _config().model_dump(by_alias=True)["data"]})


def test_swapped_file_threshold_is_rejected() -> None:
    from pydantic import ValidationError

    parameters = {
        "fileAutoApprovalThreshold": 0.2,
        "fileAutoSuggestThreshold": 0.5,
    }
    with pytest.raises(ValidationError, match="fileAutoApprovalThreshold"):
        Config.model_validate({"parameters": parameters, "data": _config().model_dump(by_alias=True)["data"]})


def test_pipeline_config_must_be_a_parameters_and_data_document() -> None:
    from services.ConfigService import load_config_parameters

    client = MagicMock()
    raw_config = MagicMock()
    raw_config.config = "- not-a-mapping\n"
    client.extraction_pipelines.config.retrieve.return_value = raw_config

    with pytest.raises(ValueError, match="parameters"):
        load_config_parameters(client, {"ExtractionPipelineExtId": "ep_file_annotation"})
