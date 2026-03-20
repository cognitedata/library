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

ASSET_VIEW = ViewId("upstream-value-chain", "AssetExtension", "v1")
BATCH_SIZE = 1000


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


def _build_nodes(rows: list[dict], space: str) -> list[NodeApply]:
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
                sources=[NodeOrEdgeData(source=ASSET_VIEW, properties=props)],
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
    space = config.get("assetInstanceSpace") or data.get("assetInstanceSpace") or "instance_upstream_value_chain"
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
    nodes = _build_nodes(rows, space)
    if not nodes:
        return {"status": "skipped", "message": "CSV has no valid rows (need externalId)", "assetInstanceSpace": space}

    total = len(nodes)
    for i in range(0, total, BATCH_SIZE):
        batch = nodes[i : i + BATCH_SIZE]
        client.data_modeling.instances.apply(nodes=batch)

    return {"status": "succeeded", "nodesUpserted": total, "assetInstanceSpace": space}


if __name__ == "__main__":
    from cognite.client import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials

    os.environ.setdefault("EXTRACTION_PIPELINE_EXT_ID", "ep_ctx_3d_clov_navisworks_upload_asset_hierarchy")
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    except ImportError:
        pass
    project = os.environ["CDF_PROJECT"]
    cluster = os.environ.get("CDF_CLUSTER", "api")
    base_url = f"https://{cluster}.cognitedata.com"
    client = CogniteClient(
        ClientConfig(
            client_name=project,
            base_url=base_url,
            project=project,
            credentials=OAuthClientCredentials(
                token_url=os.environ["IDP_TOKEN_URL"],
                client_id=os.environ["IDP_CLIENT_ID"],
                client_secret=os.environ["IDP_CLIENT_SECRET"],
                scopes=[f"{base_url}/.default"],
            ),
        )
    )
    data = {"ExtractionPipelineExtId": os.environ["EXTRACTION_PIPELINE_EXT_ID"]}
    if os.environ.get("UPLOAD_FILE_EXTERNAL_ID"):
        data["fileExternalId"] = os.environ["UPLOAD_FILE_EXTERNAL_ID"]
    if os.environ.get("UPLOAD_DATA_SET_EXTERNAL_ID"):
        data["dataSetExternalId"] = os.environ["UPLOAD_DATA_SET_EXTERNAL_ID"]
    result = handle(data, client)
    print(result)
