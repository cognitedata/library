"""Tests for reading targets through the sync endpoint with a cached copy in CDF.

The read is the slowest step of a submit run and the one that times out on a large data
model, so what matters here is that a run with unchanged targets touches the data model
once and reads the content from the cache, that changes are merged into that cache, and
that a timed-out page comes back with a smaller page size rather than failing the run.
"""

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import Node
from cognite.client.data_classes.data_modeling.instances import NodeListWithCursor, Properties
from cognite.client.data_classes.data_modeling.query import Query, QueryResult
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError

sys.path.append(str(Path(__file__).parent))

from em_config import Config, ConfigData, Parameters, ViewPropertyConfig  # isort: skip
from em_constants import (  # isort: skip
    TARGET_SYNC_BATCH_SIZE,
    TARGET_SYNC_COL_BATCH_SIZE,
    TARGET_SYNC_COL_CURSOR,
    TARGET_SYNC_COL_FILE,
    TARGET_SYNC_QUERY_NAME,
)
from em_logger import CogniteFunctionLogger  # isort: skip
from em_targets import (  # isort: skip
    cache_file_external_id,
    get_all_targets,
    load_targets,
    target_cache_key,
    target_state_row_key,
)

VIEW_ID = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")
INSTANCE_SPACE = "inst_location"


def target_node(external_id: str, name: str, deleted: bool = False) -> Node:
    return Node(
        space=INSTANCE_SPACE,
        external_id=external_id,
        version=1,
        last_updated_time=1,
        created_time=1,
        deleted_time=1 if deleted else None,
        type=None,
        properties=Properties({VIEW_ID: {"name": name, "alias": [name]}}),
    )


class FakeRowsAPI:
    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], dict[str, dict]] = {}
        self.insert_error: Exception | None = None

    def _table(self, db: str, table: str) -> dict[str, dict]:
        return self.tables.setdefault((db, table), {})

    def insert(self, db_name: str, table_name: str, row: Row) -> None:
        if self.insert_error:
            raise self.insert_error
        self._table(db_name, table_name)[row.key] = dict(row.columns or {})

    def retrieve(self, db_name: str, table_name: str, key: str) -> Row | None:
        columns = self._table(db_name, table_name).get(key)
        return Row(key, dict(columns)) if columns is not None else None


class FakeCreateAPI:
    def create(self, *args: Any, **kwargs: Any) -> None:
        return None


class FakeRawAPI:
    def __init__(self) -> None:
        self.rows = FakeRowsAPI()
        self.databases = FakeCreateAPI()
        self.tables = FakeCreateAPI()


class FakeFilesAPI:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.uploads: list[str] = []
        self.upload_errors: list[Exception] = []
        self.download_errors: list[Exception] = []
        self.download_attempts = 0

    def upload_bytes(self, content: bytes, name: str, external_id: str, **kwargs: Any) -> None:
        self.uploads.append(external_id)
        if self.upload_errors:
            raise self.upload_errors.pop(0)
        self.files[external_id] = content

    def download_bytes(self, external_id: str) -> bytes:
        self.download_attempts += 1
        if self.download_errors:
            raise self.download_errors.pop(0)
        if external_id not in self.files:
            raise CogniteAPIError("Files not found", code=400)
        return self.files[external_id]

    def store(self, external_id: str, nodes: list[Node]) -> None:
        self.files[external_id] = json.dumps([node.dump(camel_case=True) for node in nodes]).encode("utf-8")

    def nodes(self, external_id: str) -> list[Node]:
        return [Node.load(item) for item in json.loads(self.files[external_id])]


class FakeInstancesAPI:
    """Serves scripted sync responses, recording the page size each call asked for."""

    def __init__(self, pages: list[Any]) -> None:
        self.pages = list(pages)
        self.cursors: list[str | None] = []
        self.page_sizes: list[int] = []
        self.filters: list[dict[str, Any]] = []

    def sync(self, query: Query) -> QueryResult:
        expression = query.with_[TARGET_SYNC_QUERY_NAME]
        self.cursors.append((query.cursors or {}).get(TARGET_SYNC_QUERY_NAME))
        self.page_sizes.append(expression.limit or 0)
        self.filters.append(expression.filter.dump() if expression.filter is not None else {})

        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        nodes, cursor = page
        return QueryResult({TARGET_SYNC_QUERY_NAME: NodeListWithCursor(nodes, cursor=cursor)})


class FakeClient:
    def __init__(self, pages: list[Any] | None = None) -> None:
        self.raw = FakeRawAPI()
        self.files = FakeFilesAPI()
        self.data_modeling = type("FakeDataModeling", (), {})()
        self.data_modeling.instances = FakeInstancesAPI(pages or [])


def build_config(version: str = "v1", filter_values: list[str] | None = None) -> Config:
    entity_view = ViewPropertyConfig(
        schemaSpace="cdf_cdm",
        instanceSpace=INSTANCE_SPACE,
        externalId="CogniteTimeSeries",
        version="v1",
    )
    target_view = ViewPropertyConfig(
        schemaSpace=VIEW_ID.space,
        instanceSpace=INSTANCE_SPACE,
        externalId=VIEW_ID.external_id,
        version=version,
        searchProperty="alias",
        filterProperty="tags" if filter_values else None,
        filterValues=filter_values,
    )
    return Config(
        parameters=Parameters(
            dmUpdate=False,
            runAll=False,
            removeOldLinks=False,
            rawDb="db",
            rawTableState="state",
            rawTableCtxGood="good",
            rawTableCtxBad="bad",
            autoApprovalThreshold=0.85,
        ),
        data=ConfigData(entityView=entity_view, targetView=target_view),
    )


@pytest.fixture
def logger() -> CogniteFunctionLogger:
    return CogniteFunctionLogger("ERROR")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("em_targets.time.sleep", lambda _: None)


def state_row(client: FakeClient, config: Config) -> dict[str, Any]:
    key = target_state_row_key(target_cache_key(config.data.target_view))
    return client.raw.rows.tables[("db", "state")][key]


def seed_cache(client: FakeClient, config: Config, targets: list[Node], cursor: str = "cursor-1") -> str:
    """Leave behind what an earlier run would have: a cache file and the cursor for it."""
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, targets)
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {TARGET_SYNC_COL_CURSOR: cursor, TARGET_SYNC_COL_FILE: file_external_id},
        ),
    )
    return file_external_id


def test_first_run_syncs_every_target_and_caches_the_content(logger: CogniteFunctionLogger) -> None:
    nodes = [target_node("A-1", "Pump 1"), target_node("A-2", "Pump 2")]
    client = FakeClient(pages=[(nodes, "cursor-1")])
    config = build_config()

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1", "A-2"]
    assert client.data_modeling.instances.cursors == [None]
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    assert client.files.uploads == [file_external_id]
    assert [node.external_id for node in client.files.nodes(file_external_id)] == ["A-1", "A-2"]
    assert state_row(client, config)[TARGET_SYNC_COL_CURSOR] == "cursor-1"
    assert state_row(client, config)[TARGET_SYNC_COL_FILE] == file_external_id


def test_unchanged_targets_are_read_from_the_cached_file(logger: CogniteFunctionLogger) -> None:
    nodes = [target_node("A-1", "Pump 1"), target_node("A-2", "Pump 2")]
    client = FakeClient(pages=[([], "cursor-2")])
    config = build_config()
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, nodes)
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {TARGET_SYNC_COL_CURSOR: "cursor-1", TARGET_SYNC_COL_FILE: file_external_id},
        ),
    )

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1", "A-2"]
    assert client.data_modeling.instances.cursors == ["cursor-1"]
    assert client.files.uploads == [], "an unchanged cache is not written again"
    assert state_row(client, config)[TARGET_SYNC_COL_CURSOR] == "cursor-2"


def test_changed_and_deleted_targets_are_merged_into_the_cache(logger: CogniteFunctionLogger) -> None:
    cached = [target_node("A-1", "Pump 1"), target_node("A-2", "Pump 2")]
    changes = [target_node("A-1", "Pump 1 renamed"), target_node("A-2", "Pump 2", deleted=True)]
    client = FakeClient(pages=[(changes, "cursor-2")])
    config = build_config()
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, cached)
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {TARGET_SYNC_COL_CURSOR: "cursor-1", TARGET_SYNC_COL_FILE: file_external_id},
        ),
    )

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]
    assert targets[0].properties[VIEW_ID]["name"] == "Pump 1 renamed"
    assert [node.external_id for node in client.files.nodes(file_external_id)] == ["A-1"]


def test_a_timed_out_page_is_retried_20_percent_smaller(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(
        pages=[
            CogniteAPIError("Request timed out", code=408),
            CogniteAPIError("Request timed out", code=408),
            ([target_node("A-1", "Pump 1")], "cursor-1"),
        ]
    )

    load_targets(client, build_config(), logger)  # type: ignore[arg-type]

    assert client.data_modeling.instances.page_sizes == [TARGET_SYNC_BATCH_SIZE, 800, 640]
    assert state_row(client, build_config())[TARGET_SYNC_COL_BATCH_SIZE] == 640


def test_the_page_size_that_worked_is_used_on_the_next_run(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([target_node("A-1", "Pump 1")], "cursor-2")])
    config = build_config()
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, [target_node("A-1", "Pump 1")])
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {
                TARGET_SYNC_COL_CURSOR: "cursor-1",
                TARGET_SYNC_COL_FILE: file_external_id,
                TARGET_SYNC_COL_BATCH_SIZE: 640,
            },
        ),
    )

    load_targets(client, config, logger)  # type: ignore[arg-type]

    assert client.data_modeling.instances.page_sizes == [640]


def test_a_missing_cache_file_is_read_again_from_the_data_model(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([target_node("A-1", "Pump 1")], "cursor-2")])
    config = build_config()
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {
                TARGET_SYNC_COL_CURSOR: "cursor-1",
                TARGET_SYNC_COL_FILE: cache_file_external_id(target_cache_key(config.data.target_view)),
            },
        ),
    )

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]
    assert client.data_modeling.instances.cursors == [None], "a cache miss reads from the start"


def test_a_rejected_cursor_is_read_again_from_the_data_model(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(
        pages=[
            CogniteAPIError("Cursor has expired", code=400),
            ([target_node("A-1", "Pump 1")], "cursor-2"),
        ]
    )
    config = build_config()
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, [target_node("A-9", "Pump 9")])
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {TARGET_SYNC_COL_CURSOR: "cursor-1", TARGET_SYNC_COL_FILE: file_external_id},
        ),
    )

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]
    assert client.data_modeling.instances.cursors == ["cursor-1", None]


def test_each_target_configuration_keeps_its_own_cursor_and_file() -> None:
    first = target_cache_key(build_config().data.target_view)
    other_version = target_cache_key(build_config(version="v2").data.target_view)
    other_filter = target_cache_key(build_config(filter_values=["pump"]).data.target_view)

    assert len({first, other_version, other_filter}) == 3
    assert len({cache_file_external_id(key) for key in (first, other_version, other_filter)}) == 3


def test_only_instances_with_data_in_the_target_view_are_synced(logger: CogniteFunctionLogger) -> None:
    """An `instances.list` read is scoped by `sources`; a sync read has nothing but its filter.

    Without `hasData`, sync returns every node in the configured spaces - state nodes,
    annotation nodes and all - and each one is then skipped for having no name.
    """
    client = FakeClient(pages=[([], "cursor-1")])
    config = build_config()

    load_targets(client, config, logger)  # type: ignore[arg-type]

    applied = json.dumps(client.data_modeling.instances.filters[0])
    assert "hasData" in applied
    assert VIEW_ID.external_id in applied
    assert INSTANCE_SPACE in applied


def test_a_transient_download_failure_is_retried(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([], "cursor-2")])
    config = build_config()
    file_external_id = seed_cache(client, config, [target_node("A-1", "Pump 1")])
    client.files.download_errors = [CogniteAPIError("Service unavailable", code=503)]

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]
    assert client.files.download_attempts == 2
    assert client.data_modeling.instances.cursors == ["cursor-1"], "the data model was not read again"
    assert file_external_id in client.files.files


def test_a_cache_that_cannot_be_downloaded_falls_back_to_the_data_model(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([target_node("A-1", "Pump 1")], "cursor-2")])
    config = build_config()
    seed_cache(client, config, [target_node("A-1", "Pump 1")])
    client.files.download_errors = [CogniteConnectionError("Connection reset") for _ in range(5)]

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]
    assert client.data_modeling.instances.cursors == [None], "an unreadable cache reads from the start"


def test_a_transient_upload_failure_is_retried(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([target_node("A-1", "Pump 1")], "cursor-1")])
    config = build_config()
    client.files.upload_errors = [CogniteAPIError("Too many requests", code=429)]

    load_targets(client, config, logger)  # type: ignore[arg-type]

    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    assert len(client.files.uploads) == 2
    assert [node.external_id for node in client.files.nodes(file_external_id)] == ["A-1"]
    assert state_row(client, config)[TARGET_SYNC_COL_CURSOR] == "cursor-1"


def test_a_cache_that_cannot_be_stored_leaves_the_cursor_alone(logger: CogniteFunctionLogger) -> None:
    """The stored cursor has to describe targets in cache file, or next run merges onto wrong content."""
    client = FakeClient(pages=[([target_node("A-2", "Pump 2")], "cursor-2")])
    config = build_config()
    file_external_id = seed_cache(client, config, [target_node("A-1", "Pump 1")])
    client.files.upload_errors = [CogniteAPIError("Forbidden", code=403)]

    targets = load_targets(client, config, logger)  # type: ignore[arg-type]

    assert sorted(node.external_id for node in targets) == ["A-1", "A-2"], "the run still has its targets"
    assert state_row(client, config)[TARGET_SYNC_COL_CURSOR] == "cursor-1"
    assert [node.external_id for node in client.files.nodes(file_external_id)] == ["A-1"]


def test_a_cursor_that_cannot_be_stored_does_not_fail_the_run(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(pages=[([target_node("A-1", "Pump 1")], "cursor-1")])
    client.raw.rows.insert_error = CogniteAPIError("Service unavailable", code=503)

    targets = load_targets(client, build_config(), logger)  # type: ignore[arg-type]

    assert [node.external_id for node in targets] == ["A-1"]


def test_a_timed_out_page_fails_when_max_retries_exceeded(logger: CogniteFunctionLogger) -> None:
    client = FakeClient(
        pages=[
            CogniteAPIError("Request timed out", code=408),
            CogniteAPIError("Request timed out", code=408),
            CogniteAPIError("Request timed out", code=408),
            CogniteAPIError("Request timed out", code=408),
            CogniteAPIError("Request timed out", code=408),
        ]
    )

    with pytest.raises(CogniteAPIError):
        load_targets(client, build_config(), logger)  # type: ignore[arg-type]


def test_targets_are_built_from_the_cached_content(logger: CogniteFunctionLogger) -> None:
    """A cached read produces the same match entities as a read from the data model."""
    client = FakeClient(pages=[([], "cursor-2")])
    config = build_config()
    file_external_id = cache_file_external_id(target_cache_key(config.data.target_view))
    client.files.store(file_external_id, [target_node("A-1", "Pump 1")])
    client.raw.rows.insert(
        "db",
        "state",
        Row(
            target_state_row_key(target_cache_key(config.data.target_view)),
            {TARGET_SYNC_COL_CURSOR: "cursor-1", TARGET_SYNC_COL_FILE: file_external_id},
        ),
    )

    targets = get_all_targets(client, logger, config)  # type: ignore[arg-type]

    assert targets == [
        {
            "asset_ext_id": "A-1",
            "asset_space": INSTANCE_SPACE,
            "org_name": "Pump 1",
            "name": "Pump 1",
            "rule_keys": None,
        }
    ]
