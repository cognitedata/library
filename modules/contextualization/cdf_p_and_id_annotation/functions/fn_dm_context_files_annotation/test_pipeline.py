"""Unit tests for `pipeline.py`.

The tests focus on the bug-fix surface that motivated the recent cleanup PRs:

* C2 (per-batch counting)             — exercised indirectly via push_result_to_annotations.
* C3 (push_result_to_annotations
       always returns int, never tuple).
* C4 (file-entity search_property key,
       cross-view canonical key).
* C7 (cursor preservation on a 400
       in get_new_files).
* C8 (typed coercion in
       read_state_cursor / read_state_batch_num).
* H8 (entity de-duplication in
       get_all_entities).
* `_truncate` helper.
* `create_annotation_id` length-boundary fallbacks.

Tests do not require a live CDF connection. The CogniteClient is fully mocked
via unittest.mock.MagicMock; pydantic models are exercised through model_validate
with camelCase aliases.

Run from the function directory:

    pytest -q test_pipeline.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError

# Same path-prepend pattern used by handler.py so flat imports work in-test.
sys.path.append(str(Path(__file__).parent))

from config import Config
from logger import CogniteFunctionLogger
from pipeline import (
    EXTERNAL_ID_LIMIT,
    STAT_STORE_CURSOR,
    STAT_STORE_NUM_IN_BATCH,
    STAT_STORE_VALUE,
    _truncate,
    create_annotation_id,
    get_all_entities,
    get_new_files,
    push_result_to_annotations,
    read_state_batch_num,
    read_state_cursor,
)

# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #

@pytest.fixture
def logger() -> CogniteFunctionLogger:
    return CogniteFunctionLogger("DEBUG")


@pytest.fixture
def file_view_id() -> dm.ViewId:
    return dm.ViewId(space="schema", external_id="FileView", version="v1")


@pytest.fixture
def asset_view_id() -> dm.ViewId:
    return dm.ViewId(space="schema", external_id="AssetView", version="v1")


@pytest.fixture
def base_config_dict() -> dict:
    """Minimal pipeline config in pydantic-alias (camelCase) form."""
    return {
        "parameters": {
            "debug": False,
            "runAll": False,
            "cleanOldAnnotations": False,
            "rawDb": "raw-db",
            "rawTableState": "state",
            "rawTableDocTag": "doc_tag",
            "rawTableDocDoc": "doc_doc",
            "autoApprovalThreshold": 0.9,
            "autoSuggestThreshold": 0.5,
        },
        "data": {
            "annotationView": {
                "schemaSpace": "schema",
                "externalId": "AnnotationView",
                "version": "v1",
            },
            "annotationJob": {
                "fileView": {
                    "schemaSpace": "schema",
                    "instanceSpace": "instance",
                    "externalId": "FileView",
                    "version": "v1",
                    "searchProperty": "alias",
                    "type": "diagrams.FileLink",
                },
                "entityViews": [],
            },
        },
    }


@pytest.fixture
def config(base_config_dict) -> Config:
    return Config.model_validate(base_config_dict)


def _file_node(space: str, external_id: str, view_id: dm.ViewId, name: str, alias: str):
    """Lightweight stand-in for a Cognite Node with the attrs the pipeline reads."""
    return SimpleNamespace(
        external_id=external_id,
        space=space,
        properties={view_id: {"name": name, "alias": alias}},
    )


# --------------------------------------------------------------------------- #
# _truncate                                                                   #
# --------------------------------------------------------------------------- #

class TestTruncate:
    def test_short_message_unchanged(self):
        assert _truncate("hello", 100) == "hello"

    def test_exactly_at_limit_unchanged(self):
        assert _truncate("a" * 10, 10) == "a" * 10

    def test_overflow_is_ellipsis_truncated(self):
        result = _truncate("a" * 100, 10)
        assert len(result) == 10
        assert result.endswith("...")
        assert result.startswith("a" * 7)

    def test_empty_message(self):
        assert _truncate("", 5) == ""

    def test_max_len_equal_to_ellipsis_returns_only_ellipsis(self):
        # Pathological corner case: max_len == 3. Should not crash.
        assert _truncate("abcdef", 3) == "..."


# --------------------------------------------------------------------------- #
# create_annotation_id                                                        #
# --------------------------------------------------------------------------- #

class TestCreateAnnotationId:
    @staticmethod
    def _inputs(raw=None):
        file_id = dm.NodeId(space="instance", external_id="file-001")
        entity = {"space": "instance", "external_id": "asset-001"}
        text = "TAG-001"
        if raw is None:
            raw = {"region": {"page": 1}, "confidence": 0.92}
        return file_id, entity, text, raw

    def test_short_id_uses_naive_form(self):
        file_id, entity, text, raw = self._inputs()
        out = create_annotation_id(file_id, entity, text, raw)
        assert out.startswith("instance:file-001:instance:asset-001:TAG-001:")
        assert len(out) < EXTERNAL_ID_LIMIT

    def test_deterministic_for_same_raw_annotation(self):
        file_id, entity, text, raw = self._inputs()
        a = create_annotation_id(file_id, entity, text, raw)
        b = create_annotation_id(file_id, entity, text, raw)
        assert a == b

    def test_distinct_raw_annotations_produce_distinct_ids(self):
        file_id, entity, text, _ = self._inputs()
        a = create_annotation_id(file_id, entity, text, {"region": {"page": 1}})
        b = create_annotation_id(file_id, entity, text, {"region": {"page": 2}})
        assert a != b

    def test_long_inputs_fall_back_to_short_form(self):
        # We want inputs where the naive form (which includes both spaces) is
        # >= EXTERNAL_ID_LIMIT but the short form (without spaces) is below.
        # Naive layout: "{space}:{fid}:{ent_space}:{ent_ext}:{text}:{hash10}"
        # Short layout: "{fid}:{ent_ext}:{text}:{hash10}"
        # Choose sizes so naive >= 256 and short < 256.
        file_id = dm.NodeId(space="instance", external_id="x" * 120)
        entity = {"space": "instance", "external_id": "y" * 70}
        text = "z" * 40
        out = create_annotation_id(file_id, entity, text, {"any": "data"})
        assert len(out) < EXTERNAL_ID_LIMIT
        # short_id form starts with the file's external_id, not the space-prefixed naive form.
        assert out.startswith("x" * 120 + ":" + "y" * 70 + ":" + "z" * 40 + ":")

    def test_extremely_long_inputs_fall_back_to_truncated_prefix(self):
        file_id = dm.NodeId(space="instance", external_id="x" * 300)
        entity = {"space": "instance", "external_id": "y" * 100}
        text = "z" * 100
        out = create_annotation_id(file_id, entity, text, {"any": "data"})
        assert len(out) <= EXTERNAL_ID_LIMIT


# --------------------------------------------------------------------------- #
# read_state_cursor / read_state_batch_num (C8)                                #
# --------------------------------------------------------------------------- #

class TestReadStateValues:
    @staticmethod
    def _client_with_rows(rows):
        client = MagicMock()
        client.raw.rows.list.return_value = rows
        return client

    @staticmethod
    def _row(key, value):
        return SimpleNamespace(key=key, columns={STAT_STORE_VALUE: value})

    def test_cursor_returns_str_when_present(self, logger):
        client = self._client_with_rows([self._row(STAT_STORE_CURSOR, "abc-cursor")])
        assert read_state_cursor(client, logger, "db", "tbl") == "abc-cursor"

    def test_cursor_returns_none_when_missing(self, logger):
        client = self._client_with_rows([])
        assert read_state_cursor(client, logger, "db", "tbl") is None

    def test_cursor_coerces_non_str_to_str(self, logger):
        # RAW columns are JSON-typed: numbers come back as numbers. The
        # cursor reader should still return a str so callers like
        # sync_query.cursors["files"] = file_cursor get a string.
        client = self._client_with_rows([self._row(STAT_STORE_CURSOR, 12345)])
        assert read_state_cursor(client, logger, "db", "tbl") == "12345"

    def test_batch_num_returns_int_when_present(self, logger):
        client = self._client_with_rows([self._row(STAT_STORE_NUM_IN_BATCH, 42)])
        assert read_state_batch_num(client, logger, "db", "tbl") == 42

    def test_batch_num_returns_zero_when_missing(self, logger):
        client = self._client_with_rows([])
        assert read_state_batch_num(client, logger, "db", "tbl") == 0

    def test_batch_num_coerces_string_int(self, logger):
        client = self._client_with_rows([self._row(STAT_STORE_NUM_IN_BATCH, "10")])
        assert read_state_batch_num(client, logger, "db", "tbl") == 10

    def test_batch_num_returns_zero_for_unparseable_string(self, logger):
        # The pre-C8 implementation would have returned a non-int and made
        # the caller's `range(file_num, ...)` blow up. Now we coerce and warn.
        client = self._client_with_rows(
            [self._row(STAT_STORE_NUM_IN_BATCH, "not-an-int")]
        )
        assert read_state_batch_num(client, logger, "db", "tbl") == 0


# --------------------------------------------------------------------------- #
# get_all_entities (C4 + H8)                                                  #
# --------------------------------------------------------------------------- #

class TestGetAllEntities:
    def test_file_entities_use_file_search_property_as_key(
        self, logger, base_config_dict, file_view_id
    ):
        """C4: file entities must store their searchable value under
        file_view.search_property (here: 'alias'), not the literal key
        'search_property'. Otherwise diagram-detect's search_field never
        matches them."""
        config = Config.model_validate(base_config_dict)
        all_files = [
            _file_node("instance", "f1", file_view_id, "F1", "F1-alias"),
            _file_node("instance", "f2", file_view_id, "F2", "F2-alias"),
        ]

        with patch("pipeline.get_all_files", return_value=all_files):
            entities = get_all_entities(MagicMock(), logger, config)

        assert len(entities) == 2
        assert all(e["alias"] == f"F{i + 1}-alias" for i, e in enumerate(entities))
        assert all("search_property" not in e for e in entities)
        assert all(e["annotation_type_external_id"] == "diagrams.FileLink" for e in entities)

    def test_entity_view_normalises_to_canonical_search_property(
        self, logger, base_config_dict, asset_view_id
    ):
        """C4 second-order: entity views that declare a different
        search_property than the file view must still store under the file
        view's search_property (which is the diagram-detect search_field)."""
        base_config_dict["data"]["annotationJob"]["entityViews"] = [
            {
                "schemaSpace": "schema",
                "instanceSpace": "instance",
                "externalId": "AssetView",
                "version": "v1",
                "searchProperty": "tag",  # differs from file view's "alias"
                "type": "diagrams.AssetLink",
            },
        ]
        config = Config.model_validate(base_config_dict)

        client = MagicMock()
        client.data_modeling.instances.list.return_value = [
            SimpleNamespace(
                external_id="asset-1",
                space="instance",
                properties={asset_view_id: {"name": "Asset 1", "tag": "TAG-1"}},
            ),
        ]

        with patch("pipeline.get_all_files", return_value=[]):
            entities = get_all_entities(client, logger, config)

        assert len(entities) == 1
        assert entities[0]["alias"] == "TAG-1"  # stored under canonical key
        assert "tag" not in entities[0]
        assert entities[0]["annotation_type_external_id"] == "diagrams.AssetLink"

    def test_dedup_drops_duplicate_space_external_id(
        self, logger, base_config_dict, file_view_id, asset_view_id
    ):
        """H8: a node listed by two views (e.g. file_view + an entity_view
        both pointing at the same instance) must yield a single entity
        after dedup, not two."""
        base_config_dict["data"]["annotationJob"]["entityViews"] = [
            {
                "schemaSpace": "schema",
                "instanceSpace": "instance",
                "externalId": "AssetView",
                "version": "v1",
                "searchProperty": "alias",
                "type": "diagrams.AssetLink",
            },
        ]
        config = Config.model_validate(base_config_dict)

        all_files = [_file_node("instance", "n1", file_view_id, "Node 1", "N1")]
        client = MagicMock()
        client.data_modeling.instances.list.return_value = [
            SimpleNamespace(
                external_id="n1",  # same external_id as the file above
                space="instance",
                properties={asset_view_id: {"name": "Node 1", "alias": "N1"}},
            ),
        ]

        with patch("pipeline.get_all_files", return_value=all_files):
            entities = get_all_entities(client, logger, config)

        assert len(entities) == 1


# --------------------------------------------------------------------------- #
# get_new_files (C7)                                                          #
# --------------------------------------------------------------------------- #

class TestGetNewFiles:
    @staticmethod
    def _sync_result(files=None, cursor_value="next-cursor"):
        files_list = list(files or [])
        result = MagicMock()
        result.cursors = {"files": cursor_value}
        result.__getitem__.side_effect = lambda key: {"files": files_list}[key]
        return result

    def test_successful_sync_persists_new_cursor(self, logger, config):
        client = MagicMock()
        client.data_modeling.instances.sync.return_value = self._sync_result()
        files_view_id = config.data.annotation_job.file_view.as_view_id()

        with patch("pipeline.update_state_store") as mock_update:
            result = get_new_files(client, logger, "old-cursor", files_view_id, config)

        assert result is client.data_modeling.instances.sync.return_value
        mock_update.assert_called_once()
        # update_state_store(client, logger, new_cursor_value, ...) — positional 2.
        assert mock_update.call_args.args[2] == "next-cursor"

    def test_400_retries_with_same_cursor(self, logger, config):
        """C7: a transient 400 must NOT reset the cursor to None. The retry
        must re-issue the sync with the original cursor value."""
        client = MagicMock()
        captured_cursors: list[str | None] = []

        def fake_sync(query):
            captured_cursors.append(query.cursors.get("files"))
            if len(captured_cursors) == 1:
                raise CogniteAPIError("temporary 400", code=400)
            return self._sync_result()

        client.data_modeling.instances.sync.side_effect = fake_sync
        files_view_id = config.data.annotation_job.file_view.as_view_id()

        with patch("pipeline.update_state_store"), patch("pipeline.time.sleep"):
            get_new_files(client, logger, "old-cursor", files_view_id, config)

        assert captured_cursors == ["old-cursor", "old-cursor"]

    def test_400_after_max_retries_raises_without_persisting_state(self, logger, config):
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = CogniteAPIError(
            "persistent 400", code=400
        )
        files_view_id = config.data.annotation_job.file_view.as_view_id()

        with patch("pipeline.update_state_store") as mock_update, patch("pipeline.time.sleep"):
            with pytest.raises(Exception):
                get_new_files(client, logger, "old-cursor", files_view_id, config)

        mock_update.assert_not_called()

    def test_non_400_error_raises_immediately(self, logger, config):
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = CogniteAPIError(
            "server error", code=500
        )
        files_view_id = config.data.annotation_job.file_view.as_view_id()

        with patch("pipeline.update_state_store") as mock_update, \
                patch("pipeline.time.sleep") as mock_sleep, pytest.raises(Exception):
            get_new_files(client, logger, "old-cursor", files_view_id, config)

        mock_update.assert_not_called()
        mock_sleep.assert_not_called()
        assert client.data_modeling.instances.sync.call_count == 1


# --------------------------------------------------------------------------- #
# push_result_to_annotations (C3)                                             #
# --------------------------------------------------------------------------- #

class TestPushResultToAnnotations:
    def test_missing_file_instance_id_does_not_abort_batch(
        self, logger, config, file_view_id
    ):
        """C3: a result item missing 'fileInstanceId' must NOT cause the
        function to bail with a tuple. The remaining valid items must still
        be processed and the function must return an int.
        """
        client = MagicMock()

        # `new_files` is duck-typed: code reads it like a dict and iterates
        # `new_files["files"]`. A plain dict + SimpleNamespace nodes is enough.
        new_files = {
            "files": [_file_node("instance", "f1", file_view_id, "F1", "F1-alias")],
        }

        result = {
            "items": [
                {"fileId": 7, "annotations": []},  # bad: no fileInstanceId
                {
                    "fileInstanceId": {"space": "instance", "externalId": "f1"},
                    "annotations": [],
                },
            ],
        }
        annotation_view_id = config.data.annotation_view.as_view_id()

        # The mock must echo the running error_count back so the +1 from the
        # malformed item isn't clobbered when the second (good) item is
        # processed. _result_item_to_edge_applies takes error_count as the
        # 7th positional arg and returns it as the 2nd tuple element.
        def echo_error_count(*args, **kwargs):
            return ([], args[6])

        with patch("pipeline._result_item_to_edge_applies", side_effect=echo_error_count):
            count = push_result_to_annotations(
                client,
                config,
                logger,
                annotation_view_id,
                file_view_id,
                result,
                new_files,
                error_count=0,
                doc_doc=[],
                doc_tag=[],
            )

        assert isinstance(count, int)
        assert count == 1  # exactly one error: the missing-fileInstanceId item
        # apply was called once (after the loop), proving we did not abort
        # the batch on the first malformed item.
        client.data_modeling.instances.apply.assert_called_once()

    def test_clean_old_annotations_batches_into_single_call(
        self, logger, base_config_dict, file_view_id
    ):
        """M6: with clean_old_annotations=True, the per-page cleanup must go
        through delete_annotations_for_files in a single batched call, not
        per-file."""
        base_config_dict["parameters"]["cleanOldAnnotations"] = True
        config = Config.model_validate(base_config_dict)

        client = MagicMock()
        new_files = {
            "files": [
                _file_node("instance", "f1", file_view_id, "F1", "F1-alias"),
                _file_node("instance", "f2", file_view_id, "F2", "F2-alias"),
            ],
        }
        result = {
            "items": [
                {"fileInstanceId": {"space": "instance", "externalId": "f1"}, "annotations": []},
                {"fileInstanceId": {"space": "instance", "externalId": "f2"}, "annotations": []},
            ],
        }
        annotation_view_id = config.data.annotation_view.as_view_id()

        with patch("pipeline._result_item_to_edge_applies", return_value=([], 0)), \
                patch("pipeline.delete_annotations_for_files") as mock_delete:
            count = push_result_to_annotations(
                client,
                config,
                logger,
                annotation_view_id,
                file_view_id,
                result,
                new_files,
                error_count=0,
                doc_doc=[],
                doc_tag=[],
            )

        assert count == 0
        mock_delete.assert_called_once()
        # Third positional arg is the list of NodeIds to clean.
        passed_nodes = mock_delete.call_args.args[3]
        assert len(passed_nodes) == 2
        assert {n.external_id for n in passed_nodes} == {"f1", "f2"}
