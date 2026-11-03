"""Podcast IDs and latest-summary lookup after the data space merge."""

from datetime import datetime, timezone
from types import SimpleNamespace

from config import runtime as summary_runtime
from podcast_config import runtime
from save_results import _find_latest_summary, podcast_external_ids, save_podcast_to_cdf


def test_podcast_ids_match_the_app_for_a_known_summary():
    script_id, audio_id = podcast_external_ids(
        source_summary_id="ops_summary_x",
        start_date="",
        end_date="",
        prefix="ops_podcast_",
    )
    assert script_id == "ops_podcast_ops_summary_x_script"
    assert audio_id == "ops_podcast_ops_summary_x_audio"


def test_timestamp_fallback_keeps_the_podcast_prefix():
    moment = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
    script_id, audio_id = podcast_external_ids(
        source_summary_id=None,
        start_date="2026-10-01",
        end_date="2026-10-07",
        prefix="ops_podcast_",
        now=moment,
    )
    assert script_id == "ops_podcast_ops_summary_2026-10-01_2026-10-07_20261028_120000_script"
    assert audio_id == "ops_podcast_ops_summary_2026-10-01_2026-10-07_20261028_120000_audio"


def test_save_writes_prefixed_ids_into_the_summary_space(monkeypatch):
    runtime.summary_space = "ops_summaries"
    runtime.podcast_prefix = "ops_podcast_"
    applied: list[dict] = []

    class FakeApply:
        def __init__(self, **kwargs):
            applied.append(kwargs)

    monkeypatch.setattr(
        "cognite.client.data_classes.data_modeling.cdm.v1.CogniteFileApply",
        FakeApply,
    )
    monkeypatch.setattr("save_results.ensure_space_exists", lambda _client, _space: None)
    monkeypatch.setattr("save_results._upload_with_retry", lambda *_args, **_kwargs: None)

    client = SimpleNamespace(config=SimpleNamespace(project="demo"))
    client.data_modeling = SimpleNamespace(instances=SimpleNamespace(apply=lambda *_a, **_k: None))

    result = save_podcast_to_cdf(
        client,
        script="hello",
        source_summary_id="ops_summary_x",
    )

    assert result["script_external_id"] == "ops_podcast_ops_summary_x_script"
    assert applied[0]["space"] == "ops_summaries"
    assert applied[0]["external_id"] == "ops_podcast_ops_summary_x_script"


def test_find_latest_summary_ignores_non_summaries():
    summary_runtime.operation_summary_prefix = "ops_summary_"
    client = _ListingClient(
        [
            SimpleNamespace(external_id="ops_podcast_ops_summary_x_audio", created_time=3),
            SimpleNamespace(external_id="test_report_1", created_time=2),
        ]
    )

    assert _find_latest_summary(client, "ops_summaries", file_view="view") is None
    assert client.list_kwargs["limit"] == 1
    assert client.list_kwargs["sort"] == [
        {"property": ["node", "createdTime"], "direction": "descending"}
    ]


def test_find_latest_summary_returns_a_prefixed_node():
    summary_runtime.operation_summary_prefix = "ops_summary_"
    client = _ListingClient(
        [SimpleNamespace(external_id="ops_summary_latest", created_time=9)]
    )
    assert _find_latest_summary(client, "ops_summaries", file_view="view") == "ops_summary_latest"


class _ListingClient:
    def __init__(self, nodes):
        self._nodes = nodes
        self.list_kwargs = None
        self.data_modeling = self
        self.instances = self

    def list(self, **kwargs):
        self.list_kwargs = kwargs
        summary_filter = kwargs["filter"]
        dumped = summary_filter.dump() if hasattr(summary_filter, "dump") else {}
        text = str(dumped) + str(summary_filter)
        if "ops_summary_" not in text:
            return self._nodes
        return [node for node in self._nodes if (node.external_id or "").startswith("ops_summary_")]
