"""A Launch batch either starts a detect job and records it, or marks its files failed."""

from unittest.mock import MagicMock

from cognite.client.data_classes.data_modeling import Node, NodeApply
from cognite.client.exceptions import CogniteAPIError
from services.launch_service import GeneralLaunchService
from test_file_annotation import _config_with_debug_file
from utils.data_structures import BatchOfPairedNodes


def _node(space: str, external_id: str, view: str, properties: dict[str, object]) -> Node:
    return Node.load(
        {
            "instanceType": "node",
            "space": space,
            "externalId": external_id,
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {
                "cdf_cdm" if view == "CogniteFile" else "dm_sol_file_annotation": {f"{view}/v1": properties}
            },
        }
    )


def _service(annotation_service: MagicMock, data_model_service: MagicMock) -> GeneralLaunchService:
    config = _config_with_debug_file(None)
    config.launch_function.pattern_mode = True
    return GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=MagicMock(log_level="INFO"),
        tracker=MagicMock(),
        data_model_service=data_model_service,
        cache_service=MagicMock(),
        annotation_service=annotation_service,
        function_call_info={"function_id": 1, "call_id": 2},
        rate_limit_policy=MagicMock(),
    )


def _batch(service: GeneralLaunchService) -> BatchOfPairedNodes:
    file_node = _node("files", "file-1", "CogniteFile", {"tags": ["ToAnnotate", "AnnotationInProcess"]})
    state_node = _node("files", "state-1", "FileAnnotationState", {"annotationStatus": "New"})
    batch = BatchOfPairedNodes(file_to_state_map={file_node.as_id(): state_node})
    reference = batch.create_file_reference(file_node.as_id(), 50, service.annotation_state_view.as_view_id())
    batch.add_pair(file_node, reference)
    return batch


def _recording_data_model_service() -> tuple[MagicMock, dict[str, dict[str, object]]]:
    """The batch clears its apply list after writing, so record each write when it is made."""
    applied: dict[str, dict[str, object]] = {}

    def record(node_applies: list[NodeApply]) -> None:
        for node_apply in node_applies:
            applied[node_apply.external_id] = dict(node_apply.sources[0].properties)

    data_model_service = MagicMock()
    data_model_service.update_annotation_state.side_effect = record
    return data_model_service, applied


def test_batch_without_entities_or_patterns_marks_its_files_failed() -> None:
    data_model_service, applied = _recording_data_model_service()
    service = _service(MagicMock(), data_model_service)
    service.in_memory_cache = []
    service.in_memory_patterns = []

    assert service._process_batch(_batch(service)) is False

    assert applied["state-1"]["annotationStatus"] == "Failed"
    assert applied["file-1"]["tags"] == ["ToAnnotate", "AnnotationFailed"]


def test_regular_job_is_recorded_when_pattern_detect_fails() -> None:
    """Pattern failure after a regular job started must still count as launched (no release/retry race)."""
    annotation_service = MagicMock()
    annotation_service.run_diagram_detect.return_value = (11, "token")
    annotation_service.run_pattern_mode_detect.side_effect = CogniteAPIError("too many jobs", code=429)
    data_model_service, applied = _recording_data_model_service()
    service = _service(annotation_service, data_model_service)
    service.in_memory_cache = [{"external_id": "asset-1", "search_property": ["P-101"]}]
    service.in_memory_patterns = [{"sample": ["[A]-000"], "resource_type": "asset"}]

    assert service._process_batch(_batch(service)) is True

    state = applied["state-1"]
    assert state["diagramDetectJobId"] == 11
    assert state["annotationStatus"] == "Processing"
    assert "patternModeJobId" not in state
