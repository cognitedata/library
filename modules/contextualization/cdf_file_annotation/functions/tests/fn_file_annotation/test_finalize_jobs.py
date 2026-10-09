"""Finalize moves past a still-running job and pages through pattern-only jobs."""

from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import NodeId
from services.retrieve_service import DiagramDetectJobPoll, JobPollStatus
from test_file_annotation import _finalize_service_for_one_file


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
