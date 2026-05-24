"""Unit tests for cdf_browse helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ui.server import cdf_browse


def test_truncate_string():
    long = "x" * 600
    out = cdf_browse._truncate(long, max_len=100)
    assert isinstance(out, str)
    assert len(out) <= 101
    assert out.endswith("…")


def test_run_sql_preview_maps_results():
    col = MagicMock()
    col.name = "id"
    col.dump.return_value = {"name": "id", "type": {"type": "INT64"}, "sql_type": "BIGINT", "nullable": True}
    schema = MagicMock()
    schema.__iter__ = lambda self: iter([col])

    preview = MagicMock()
    preview.schema = schema
    preview.results = [{"id": "1", "name": "A"}]

    client = MagicMock()
    client.transformations.preview.return_value = preview

    out = cdf_browse.run_sql_preview(
        client,
        query="SELECT * FROM assets",
        limit=10,
        source_limit=None,
        convert_to_string=True,
    )
    client.transformations.preview.assert_called_once_with(
        query="SELECT * FROM assets",
        convert_to_string=True,
        limit=10,
        source_limit=None,
    )
    assert out["columns"] == ["id", "name"]
    assert out["row_count"] == 1
    assert out["items"][0]["id"] == "1"
    assert out["schema"][0]["name"] == "id"
    assert out["schema"][0]["type"] == {"type": "INT64"}


def test_run_sql_preview_requires_query():
    with pytest.raises(ValueError, match="query is required"):
        cdf_browse.run_sql_preview(MagicMock(), query="   ")


def test_dm_list_spaces_always_includes_cdf_cdm():
    client = MagicMock()
    sp = MagicMock()
    sp.space = "custom_dm"
    client.data_modeling.spaces.list.return_value = [sp]

    names = cdf_browse.dm_list_spaces(client)
    assert cdf_browse.NATIVE_CDF_CDM_SPACE in names


def test_dm_list_data_models_always_includes_native_cdf_cdm():
    client = MagicMock()
    client.data_modeling.data_models.list.return_value = []
    client.data_modeling.data_models.retrieve.side_effect = Exception("not found")

    rows = cdf_browse.dm_list_data_models(client)
    assert any(
        r["space"] == cdf_browse.NATIVE_CDF_CDM_SPACE
        and r["external_id"] == cdf_browse.NATIVE_CDF_CDM_DATA_MODEL_EXTERNAL_ID
        for r in rows
    )


def test_dm_list_data_models_falls_back_to_per_space_when_global_list_empty():
    client = MagicMock()
    client.data_modeling.data_models.list.return_value = []
    with (
        patch(
            "ui.server.cdf_browse.dm_list_spaces",
            return_value=["custom_dm"],
        ),
        patch(
            "ui.server.cdf_browse.native_cdf_cdm_data_model_row",
            return_value={
                "space": "cdf_cdm",
                "external_id": "CogniteCore",
                "version": "v1",
                "name": "Cognite Core Data Model",
            },
        ),
    ):
        other = MagicMock(
            space="custom_dm",
            external_id="MyModel",
            version="1",
            name="Mine",
        )
        other.name.strip.return_value = "Mine"
        client.data_modeling.data_models.list.side_effect = [
            [],
            [other],
        ]
        rows = cdf_browse.dm_list_data_models(client, space=None, include_global=True)
    assert len(rows) >= 1
    assert any(r["external_id"] == "MyModel" for r in rows)


def test_dm_list_data_models_skips_native_when_other_space_filtered():
    client = MagicMock()
    other = MagicMock()
    other.space = "dm_custom"
    other.external_id = "MyModel"
    other.version = "v1"
    other.name = "Custom"
    client.data_modeling.data_models.list.return_value = [other]

    rows = cdf_browse.dm_list_data_models(client, space="dm_custom")
    assert not any(r["space"] == cdf_browse.NATIVE_CDF_CDM_SPACE for r in rows)


def test_dm_data_model_graph_builds_views_and_edges():
    from cognite.client.data_classes.data_modeling.ids import ViewId
    from cognite.client.data_classes.data_modeling.views import MappedProperty, View

    asset_view = MagicMock(spec=View)
    asset_view.space = "cdf_cdm"
    asset_view.external_id = "CogniteAsset"
    asset_view.version = "v1"
    asset_view.name = "Asset"
    asset_view.properties = {}

    equip_view = MagicMock(spec=View)
    equip_view.space = "cdf_cdm"
    equip_view.external_id = "CogniteEquipment"
    equip_view.version = "v1"
    equip_view.name = "Equipment"
    rel = MagicMock(spec=MappedProperty)
    rel.source = ViewId("cdf_cdm", "CogniteAsset", "v1")
    equip_view.properties = {"asset": rel}

    model = MagicMock()
    model.name = "Core"
    model.views = [asset_view, equip_view]

    client = MagicMock()
    client.data_modeling.data_models.retrieve.return_value = [model]

    out = cdf_browse.dm_data_model_graph(
        client, space="cdf_cdm", external_id="CogniteCore", version="v1"
    )
    assert out["data_model"]["external_id"] == "CogniteCore"
    assert len(out["views"]) == 2
    assert len(out["edges"]) == 1
    assert out["edges"][0]["label"] == "asset"
    assert out["edges"][0]["to"]["external_id"] == "CogniteAsset"


def test_dm_list_views_for_data_model_from_inline_views():
    from cognite.client.data_classes.data_modeling.views import View

    view = MagicMock(spec=View)
    view.space = "cdf_cdm"
    view.external_id = "CogniteAsset"
    view.version = "v1"
    view.name = "Asset"

    model = MagicMock()
    model.views = [view]

    client = MagicMock()
    client.data_modeling.data_models.retrieve.return_value = [model]

    rows = cdf_browse.dm_list_views_for_data_model(
        client, space="cdf_cdm", external_id="CogniteCore", version="v1"
    )
    assert rows == [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "version": "v1",
            "name": "Asset",
            "instance_kind": "node",
        }
    ]


def test_view_instance_kind_from_used_for():
    edge_view = MagicMock()
    edge_view.usedFor = "edge"
    assert cdf_browse._view_instance_kind(edge_view) == "edge"

    node_view = MagicMock()
    node_view.usedFor = "node"
    assert cdf_browse._view_instance_kind(node_view) == "node"

    default_view = MagicMock(spec=[])
    assert cdf_browse._view_instance_kind(default_view) == "node"


def test_dm_instances_open_target_edge():
    assert cdf_browse.dm_instances_open_target(
        view_space="cdf_cdm",
        view_external_id="CogniteAnnotation",
        view_version="v1",
        instance_kind="edge",
    ) == {
        "type": "dm_instances",
        "view_space": "cdf_cdm",
        "view_external_id": "CogniteAnnotation",
        "view_version": "v1",
        "instance_kind": "edge",
    }


def test_list_transformations_maps_and_sorts():
    tx = MagicMock()
    tx.id = 10
    tx.external_id = "ext_tx"
    tx.name = "Transform B"
    tx.created_time = None
    tx.data_set_id = None
    client = MagicMock()
    client.transformations.list.return_value = [tx]
    rows = cdf_browse.list_transformations(client, limit=100)
    assert rows[0]["label"] == "Transform B"
    assert rows[0]["id"] == 10


def test_list_functions_maps_and_sorts():
    fn = MagicMock()
    fn.id = "fn-1"
    fn.external_id = "my_fn"
    fn.name = "Handler"
    fn.status = "Ready"
    fn.file_id = 100
    fn.owner = "user@example.com"
    fn.created_time = None
    client = MagicMock()
    client.functions.list.return_value = [fn]
    rows = cdf_browse.list_functions(client, limit=100)
    assert rows[0]["label"] == "Handler"
    assert rows[0]["id"] == "fn-1"
    assert rows[0]["status"] == "Ready"


def test_get_function_detail_retrieves_by_numeric_id():
    fn = MagicMock()
    fn.id = 42
    fn.external_id = "my_fn"
    fn.name = "Handler"
    fn.description = "Does work"
    fn.status = "Ready"
    fn.file_id = 100
    fn.owner = "user@example.com"
    fn.created_time = None
    fn.dump.return_value = {"id": 42, "name": "Handler"}
    client = MagicMock()
    client.functions.retrieve.return_value = fn
    out = cdf_browse.get_function_detail(client, function_id="42")
    client.functions.retrieve.assert_called_once_with(id=42)
    assert out["name"] == "Handler"
    assert out["description"] == "Does work"
    assert out["definition"]["id"] == 42


def test_get_function_detail_retrieves_by_external_id():
    fn = MagicMock()
    fn.id = "fn-2"
    fn.external_id = "my_fn"
    fn.name = "Handler"
    fn.description = None
    fn.status = None
    fn.file_id = None
    fn.owner = None
    fn.created_time = None
    fn.dump.return_value = {"externalId": "my_fn"}
    client = MagicMock()
    client.functions.retrieve.return_value = fn
    out = cdf_browse.get_function_detail(client, function_id="my_fn")
    client.functions.retrieve.assert_called_once_with(external_id="my_fn")
    assert out["external_id"] == "my_fn"


def test_workflow_graph_builds_tasks_and_depends_on_edges():
    task_a = MagicMock()
    task_a.external_id = "kea__vq"
    task_a.name = "View query"
    task_a.description = "Query view"
    task_a.type = "function"
    task_a.retries = 3
    task_a.timeout = 7200
    task_a.on_failure = "abortWorkflow"
    task_a.depends_on = None
    task_a.parameters = MagicMock()
    task_a.parameters.dump.return_value = {"task_type": "function"}

    task_b = MagicMock()
    task_b.external_id = "kea__tr"
    task_b.name = "Transform"
    task_b.description = ""
    task_b.type = "function"
    task_b.retries = 3
    task_b.timeout = 7200
    task_b.on_failure = "abortWorkflow"
    task_b.depends_on = ["kea__vq"]
    task_b.parameters = MagicMock()
    task_b.parameters.dump.return_value = {"task_type": "function"}

    wdef = MagicMock()
    wdef.tasks = [task_a, task_b]
    wdef.description = "Discovery pipeline"

    wv_v1 = MagicMock()
    wv_v1.version = "v1"
    wv_v1.workflow_external_id = "key_extraction_aliasing"
    wv_v1.workflow_definition = wdef

    wv_v5 = MagicMock()
    wv_v5.version = "v5"
    wv_v5.workflow_external_id = "key_extraction_aliasing"
    wv_v5.workflow_definition = wdef

    client = MagicMock()
    client.workflows.versions.list.return_value = [wv_v1, wv_v5]

    out = cdf_browse.workflow_graph(client, workflow_external_id="key_extraction_aliasing")
    assert out["workflow"]["version"] == "v5"
    assert len(out["tasks"]) == 2
    assert len(out["edges"]) == 1
    assert out["edges"][0]["from"] == "kea__vq"
    assert out["edges"][0]["to"] == "kea__tr"


def test_workflow_graph_retrieves_explicit_version():
    task = MagicMock()
    task.external_id = "t1"
    task.name = "Only"
    task.description = ""
    task.type = "transformation"
    task.retries = None
    task.timeout = None
    task.on_failure = None
    task.depends_on = None
    task.parameters = MagicMock()
    task.parameters.dump.return_value = {}

    wdef = MagicMock()
    wdef.tasks = [task]
    wdef.description = None

    wv = MagicMock()
    wv.version = "v2"
    wv.workflow_external_id = "my_wf"
    wv.workflow_definition = wdef

    client = MagicMock()
    client.workflows.versions.retrieve.return_value = wv

    out = cdf_browse.workflow_graph(client, workflow_external_id="my_wf", version="v2")
    client.workflows.versions.retrieve.assert_called_once()
    assert out["workflow"]["version"] == "v2"
    assert out["tasks"][0]["external_id"] == "t1"


def test_list_workflows_maps_external_id():
    wf = MagicMock()
    wf.external_id = "wf_scope"
    wf.name = "Scope workflow"
    wf.created_time = None
    wf.data_set_id = None
    client = MagicMock()
    client.workflows.list.return_value = [wf]
    rows = cdf_browse.list_workflows(client)
    assert rows[0]["label"] == "Scope workflow"
    assert rows[0]["external_id"] == "wf_scope"


def test_get_transformation_detail_retrieves_query():
    tx = MagicMock()
    tx.id = 99
    tx.external_id = "my_tx"
    tx.name = "My transform"
    tx.query = "SELECT 1"
    tx.created_time = None
    tx.last_updated_time = None
    tx.data_set_id = 5
    tx.is_public = False
    tx.conflict_mode = "abort"
    tx.destination = None
    tx.schedule = None
    tx.dump.return_value = {"id": 99, "query": "SELECT 1", "name": "My transform"}
    client = MagicMock()
    client.transformations.retrieve.return_value = tx
    out = cdf_browse.get_transformation_detail(client, transformation_id=99)
    client.transformations.retrieve.assert_called_once_with(id=99)
    assert out["query"] == "SELECT 1"
    assert out["name"] == "My transform"
    assert out["definition"]["id"] == 99


def test_list_security_groups_caps_and_labels():
    grp = MagicMock()
    grp.id = 7
    grp.name = "Data readers"
    grp.source_id = "src"
    grp.member_ids = [1, 2, 3]
    client = MagicMock()
    client.groups.list.return_value = [grp]
    rows = cdf_browse.list_security_groups(client, limit=10)
    assert rows[0]["label"] == "Data readers"
    assert rows[0]["member_count"] == 3


def test_list_security_groups_uses_iam_groups_when_top_level_missing():
    grp = MagicMock()
    grp.id = 9
    grp.name = "IAM group"
    grp.source_id = None
    grp.member_ids = []
    client = MagicMock(spec=["iam"])
    client.groups = None
    client.iam.groups.list.return_value = [grp]
    rows = cdf_browse.list_security_groups(client, limit=10)
    client.iam.groups.list.assert_called_once()
    assert rows[0]["id"] == 9
    assert rows[0]["label"] == "IAM group"


def test_fusion_list_containers_in_space_without_version():
    container = MagicMock()
    container.space = "cdf_cdm"
    container.external_id = "CogniteAsset"
    container.name = "Asset"
    client = MagicMock()
    client.data_modeling.containers.return_value = [[container]]
    rows = cdf_browse.fusion_list_containers_in_space(
        client, "cdf_cdm", include_global=True
    )
    assert rows == [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "name": "Asset",
            "label": "Asset",
        }
    ]


def test_fusion_view_by_container_lookup_prefers_highest_version():
    views = [
        {"space": "s", "external_id": "V", "version": "v1", "label": "V v1"},
        {"space": "s", "external_id": "V", "version": "v2", "label": "V v2"},
    ]
    lookup = cdf_browse.fusion_view_by_container_lookup(views)
    assert lookup[("s", "V")]["version"] == "v2"
    open_target = cdf_browse.fusion_open_target_for_container(
        {"space": "s", "external_id": "V", "label": "V"},
        lookup,
    )
    assert open_target == {
        "type": "dm_instances",
        "view_space": "s",
        "view_external_id": "V",
        "view_version": "v2",
        "instance_kind": "node",
    }


def test_dm_container_to_dict_serializes_schema():
    prop = MagicMock()
    prop.type = "text"
    prop.list = False
    prop.nullable = True
    prop.autoIncrement = False

    idx = MagicMock()
    idx.properties = ["name"]
    idx.cursorable = True
    idx.indexType = " btree"

    con = MagicMock()
    con.constraintType = "require"
    con.require = {"space": "s", "externalId": "C"}
    con.properties = ["ref"]

    container = MagicMock()
    container.space = "cdf_cdm"
    container.external_id = "CogniteAsset"
    container.name = "Asset"
    container.description = "Asset container"
    container.usedFor = "node"
    container.properties = {"name": prop}
    container.indexes = {"byName": idx}
    container.constraints = {"refReq": con}
    container.createdTime = 1
    container.lastUpdatedTime = 2

    out = cdf_browse.dm_container_to_dict(container)
    assert out["space"] == "cdf_cdm"
    assert out["external_id"] == "CogniteAsset"
    assert out["properties"]["name"]["type"] == "text"
    assert out["indexes"][0]["name"] == "byName"
    assert out["constraints"][0]["name"] == "refReq"


def test_dm_node_to_dict_serializes_instance():
    bag = MagicMock()
    bag.dump.return_value = {"name": "Pump A"}

    node = MagicMock()
    node.space = "inst"
    node.external_id = "node-1"
    node.version = 3
    node.properties = {"('s','V','v1')": bag}
    node.sources = []
    node.created_time = 100
    node.last_updated_time = 200

    out = cdf_browse.dm_node_to_dict(node)
    assert out["space"] == "inst"
    assert out["external_id"] == "node-1"
    assert out["properties"]["('s','V','v1')"]["name"] == "Pump A"
    assert out["created_time"] == 100


def test_dm_edge_to_dict_serializes_instance():
    start = MagicMock(spec=["space", "external_id"])
    start.space = "inst"
    start.external_id = "n1"
    end = MagicMock(spec=["space", "external_id"])
    end.space = "inst"
    end.external_id = "n2"

    edge = MagicMock()
    edge.space = "inst"
    edge.external_id = "edge-1"
    edge.type = {"space": "inst", "externalId": "rel"}
    edge.start_node = start
    edge.end_node = end
    edge.properties = {"weight": 1}
    edge.created_time = 10
    edge.last_updated_time = 20

    out = cdf_browse.dm_edge_to_dict(edge)
    assert out["external_id"] == "edge-1"
    assert out["start_node"]["external_id"] == "n1"
    assert out["end_node"]["external_id"] == "n2"
    assert out["properties"]["weight"] == 1
