"""
Upload Asset hierarchy (DM nodes) from a CSV file in CDF Files to CDF Data Modeling.
Used by the upload_asset_hierarchy extraction pipeline.
CSV columns: externalId (required), name, description, parentExternalId, tags, aliases, assetType.
"""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId
from cognite.client.exceptions import CogniteAPIError

sys.path.append(str(Path(__file__).parent))

BATCH_SIZE = 1000


def _resolve_asset_view(config: dict) -> ViewId:
    """
    View for AssetExtension (or project-specific asset view) from:
    1) extraction pipeline config parameters (assetViewSpace / assetViewExternalId / assetViewVersion)
    2) env vars set at deploy (ASSET_VIEW_SPACE, ASSET_VIEW_EXT_ID, ASSET_VIEW_VERSION from Function.yaml)
    3) no hardcoded fallback: must be explicitly provided
    """
    space = (
        config.get("assetViewSpace")
        or config.get("asset_view_space")
        or os.environ.get("ASSET_VIEW_SPACE")
        or os.environ.get("DATA_MODEL_SPACE")
    )
    ext_id = (
        config.get("assetViewExternalId")
        or config.get("assetViewExtId")
        or config.get("asset_view_ext_id")
        or os.environ.get("ASSET_VIEW_EXT_ID")
    )
    version = (
        config.get("assetViewVersion")
        or config.get("asset_view_version")
        or os.environ.get("ASSET_VIEW_VERSION")
    )
    if not space or not ext_id or not version:
        raise ValueError(
            "Missing asset view configuration. Provide assetViewSpace/assetViewExternalId/assetViewVersion "
            "in extraction pipeline config or ASSET_VIEW_SPACE/ASSET_VIEW_EXT_ID/ASSET_VIEW_VERSION env vars."
        )
    return ViewId(str(space), str(ext_id), str(version))


def _resolve_asset_instance_space(config: dict, data: dict) -> str:
    """
    Instance space for asset nodes from:
    1) extraction pipeline config parameters (assetInstanceSpace)
    2) function run input (assetInstanceSpace)
    3) env var injected by Function.yaml (ASSET_INSTANCE_SPACE)
    """
    space = (
        config.get("assetInstanceSpace")
        or config.get("asset_instance_space")
        or data.get("assetInstanceSpace")
        or data.get("asset_instance_space")
        or os.environ.get("ASSET_INSTANCE_SPACE")
    )
    if not space:
        raise ValueError(
            "Missing asset instance space. Provide assetInstanceSpace in extraction pipeline config or run input, "
            "or set ASSET_INSTANCE_SPACE env var."
        )
    return str(space)


def _get_config(client: CogniteClient, pipeline_ext_id: str) -> dict:
    """Load pipeline config from CDF."""
    raw = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
    if not raw or not raw.config:
        return {}
    data = yaml.safe_load(raw.config) or {}
    params = (data.get("config") or {}).get("data", {}).get("parameters", {})
    if not params:
        params = data.get("data", {}).get("parameters", {})
    return params


def _split_list(value: str) -> list[str]:
    return [v.strip() for v in value.split(";") if v.strip()] if value else []


def _build_nodes(rows: list[dict], space: str, asset_view: ViewId) -> list[NodeApply]:
    nodes: list[NodeApply] = []
    for row in rows:
        ext_id = (row.get("externalId") or "").strip()
        if not ext_id:
            continue
        name = (row.get("name") or "").strip() or ext_id
        description = (row.get("description") or "").strip() or None
        parent_id = (row.get("parentExternalId") or "").strip() or None
        tags_raw = (row.get("tags") or "").strip()
        aliases_raw = (row.get("aliases") or "").strip()
        asset_type = (row.get("assetType") or "").strip() or None

        props: dict = {"name": name}
        if description:
            props["description"] = description
        if parent_id:
            props["parent"] = {"space": space, "externalId": parent_id}
        if tags_raw:
            props["tags"] = _split_list(tags_raw)
        if aliases_raw:
            props["aliases"] = _split_list(aliases_raw)
        if asset_type:
            props["assetType"] = asset_type

        nodes.append(
            NodeApply(
                space=space,
                external_id=ext_id,
                sources=[NodeOrEdgeData(source=asset_view, properties=props)],
            )
        )
    return nodes


def handle(data: dict, client: CogniteClient) -> dict:
    """
    Read asset CSV from CDF File (fileExternalId in config or data), parse it,
    and upsert Asset instances into the configured instance space.
    """
    pipeline_ext_id = data.get("ExtractionPipelineExtId") or os.environ.get(
        "EXTRACTION_PIPELINE_EXT_ID", "ep_ctx_3d_clov_navisworks_upload_asset_hierarchy"
    )
    config = _get_config(client, pipeline_ext_id)
    space = _resolve_asset_instance_space(config, data)
    asset_view = _resolve_asset_view(config)
    file_ext_id = config.get("fileExternalId") or data.get("fileExternalId")
    data_set_ext_id = config.get("dataSetExternalId") or data.get("dataSetExternalId")

    if not file_ext_id:
        return {
            "status": "skipped",
            "message": "No fileExternalId in config or run input. Upload asset CSV to CDF Files and set fileExternalId.",
            "assetInstanceSpace": space,
        }
    try:
        if data_set_ext_id:
            files = client.files.list(data_set_external_ids=[data_set_ext_id], external_id=file_ext_id, limit=1)
        else:
            files = client.files.list(external_id=file_ext_id, limit=1)
        file_list = list(files)
        if not file_list:
            return {"status": "error", "message": f"File not found: externalId={file_ext_id!r}"}
        content = client.files.download_bytes(file_list[0].id)
    except CogniteAPIError as e:
        return {"status": "error", "message": str(e)}

    text = content.decode("utf-8", errors="replace")
    rows = list(csv.DictReader(io.StringIO(text)))
    nodes = _build_nodes(rows, space, asset_view)
    if not nodes:
        return {"status": "skipped", "message": "CSV has no valid rows (need externalId)", "assetInstanceSpace": space}

    total = len(nodes)
    for i in range(0, total, BATCH_SIZE):
        batch = nodes[i : i + BATCH_SIZE]
        client.data_modeling.instances.apply(nodes=batch)

    return {"status": "succeeded", "nodesUpserted": total, "assetInstanceSpace": space}


