"""Finalize moves past a still-running job and pages through pattern-only jobs."""

from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import NodeId
from services.retrieve_service import DiagramDetectJobPoll, JobPollStatus
from test_file_annotation import _finalize_service_for_one_file


def _completed_poll_with_item(item: dict[str, object]) -> DiagramDetectJobPoll:
    return DiagramDetectJobPoll(status=JobPollStatus.COMPLETED, results={"items": [item], "statusCount": {"failed": 1}})


def _set_state_attempt_count(service: object, attempt_count: int) -> None:
    from cognite.client.data_classes.data_modeling import ViewId

    state = service.retrieve_service.get_job_id.return_value[2][NodeId("files", "doc-1")]
    view_id = ViewId("dm_sol_file_annotation", "FileAnnotationState", "v1")
    state.properties[view_id]["attemptCount"] = attempt_count


def test_detect_item_error_retries_without_applying_annotations() -> None:
    """Completed jobs can still carry per-file errorMessage (e.g. OOM); do not clean or apply."""
    apply_service = MagicMock()
    service = _finalize_service_for_one_file(
        apply_service,
        _completed_poll_with_item(
            {
                "fileInstanceId": {"space": "files", "externalId": "doc-1"},
                "errorMessage": "Timeout or out of memory error",
                "pageRange": {"begin": 1, "end": 50},
            }
        ),
    )

    service.run()

    apply_service.process_and_apply_annotations_for_file.assert_not_called()
    applies: list = []
    for call in apply_service.update_instances.call_args_list:
        applies.extend(call.kwargs["list_node_apply"])
    state_apply = next(apply for apply in applies if apply.external_id == "state-1")
    properties = state_apply.sources[0].properties
    assert properties["annotationStatus"] == "Retry"
    assert properties["annotationMessage"] == "Timeout or out of memory error"
    assert properties["diagramDetectJobId"] is None
    assert properties["patternModeJobId"] is None
    assert not any(apply.external_id == "doc-1" for apply in applies)


def test_detect_item_error_marks_failed_after_max_retries() -> None:
    apply_service = MagicMock()
    service = _finalize_service_for_one_file(
        apply_service,
        _completed_poll_with_item(
            {
                "fileInstanceId": {"space": "files", "externalId": "doc-1"},
                "errorMessage": "Timeout or out of memory error",
            }
        ),
    )
    _set_state_attempt_count(service, attempt_count=2)
    service.max_retries = 3

    service.run()

    apply_service.process_and_apply_annotations_for_file.assert_not_called()
    applies: list = []
    for call in apply_service.update_instances.call_args_list:
        applies.extend(call.kwargs["list_node_apply"])
    state_apply = next(apply for apply in applies if apply.external_id == "state-1")
    assert state_apply.sources[0].properties["annotationStatus"] == "Failed"
    file_apply = next(apply for apply in applies if apply.external_id == "doc-1")
    assert "AnnotationFailed" in file_apply.sources[0].properties["tags"]


def test_pattern_item_error_fails_the_file_even_when_regular_completed() -> None:
    apply_service = MagicMock()
    service = _finalize_service_for_one_file(apply_service)
    file_id = NodeId("files", "doc-1")
    state = service.retrieve_service.get_job_id.return_value[2][file_id]
    service.retrieve_service.get_job_id.return_value = ((1, "token"), (2, "pattern-token"), {file_id: state})
    completed_regular = DiagramDetectJobPoll(
        status=JobPollStatus.COMPLETED,
        results={
            "items": [
                {
                    "fileInstanceId": {"space": "files", "externalId": "doc-1"},
                    "pageCount": 1,
                    "annotations": [{"text": "P-101"}],
                }
            ]
        },
    )
    failed_pattern = _completed_poll_with_item(
        {
            "fileInstanceId": {"space": "files", "externalId": "doc-1"},
            "errorMessage": "Timeout or out of memory error",
        }
    )
    service.retrieve_service.get_diagram_detect_job_result.side_effect = [completed_regular, failed_pattern]

    service.run()

    apply_service.process_and_apply_annotations_for_file.assert_not_called()
    applies: list = []
    for call in apply_service.update_instances.call_args_list:
        applies.extend(call.kwargs["list_node_apply"])
    state_apply = next(apply for apply in applies if apply.external_id == "state-1")
    assert state_apply.sources[0].properties["annotationStatus"] == "Retry"
    assert state_apply.sources[0].properties["patternModeMessage"] == "Timeout or out of memory error"


def test_running_job_is_skipped_so_finished_jobs_behind_it_are_finalized(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep = MagicMock()
    monkeypatch.setattr("services.finalize_service.time.sleep", sleep)
    service = _finalize_service_for_one_file(MagicMock(), DiagramDetectJobPoll(status=JobPollStatus.RUNNING))

    service.run()
    service.run()

    sleep.assert_not_called()
    assert service.retrieve_service.get_job_id.call_args.args == ({1},)


def test_finalize_waits_when_only_running_jobs_are_left(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep = MagicMock()
    monkeypatch.setattr("services.finalize_service.time.sleep", sleep)
    service = _finalize_service_for_one_file(MagicMock(), DiagramDetectJobPoll(status=JobPollStatus.RUNNING))
    service.run()
    service.retrieve_service.get_job_id.return_value = (None, None, None)

    assert service.run() is None
    sleep.assert_called_once_with(30)
    service.run()
    assert service.retrieve_service.get_job_id.call_args.args == (set(),)


def test_pattern_only_job_uses_the_pattern_page_count() -> None:
    apply_service = MagicMock()
    apply_service.process_and_apply_annotations_for_file.return_value = ("regular", "pattern")
    pattern_result = DiagramDetectJobPoll(
        status=JobPollStatus.COMPLETED,
        results={
            "items": [{"fileInstanceId": {"space": "files", "externalId": "doc-1"}, "pageCount": 80, "annotations": []}]
        },
    )
    service = _finalize_service_for_one_file(apply_service, pattern_result)
    regular, _, mapping = service.retrieve_service.get_job_id.return_value
    service.retrieve_service.get_job_id.return_value = (None, regular, mapping)

    service.run()

    (state,) = [
        node
        for node in apply_service.update_instances.call_args.kwargs["list_node_apply"]
        if node.external_id == "state-1"
    ]
    properties = state.sources[0].properties
    assert properties["annotationStatus"] == "New"
    assert properties["pageCount"] == 80
    assert mapping[NodeId("files", "doc-1")] is not None


def test_finalize_skips_detect_results_without_an_annotation_state() -> None:
    """Detect results can include files whose state was skipped (e.g. missing linkedFile)."""
    apply_service = MagicMock()
    apply_service.process_and_apply_annotations_for_file.return_value = ("regular", "pattern")
    service = _finalize_service_for_one_file(
        apply_service,
        DiagramDetectJobPoll(
            status=JobPollStatus.COMPLETED,
            results={
                "items": [
                    {
                        "fileInstanceId": {"space": "files", "externalId": "doc-1"},
                        "pageCount": 1,
                        "annotations": [],
                    },
                    {
                        "fileInstanceId": {"space": "files", "externalId": "orphan"},
                        "pageCount": 1,
                        "annotations": [],
                    },
                ]
            },
        ),
    )

    service.run()

    apply_service.process_and_apply_annotations_for_file.assert_called_once()
    assert apply_service.process_and_apply_annotations_for_file.call_args.args[0].external_id == "doc-1"
