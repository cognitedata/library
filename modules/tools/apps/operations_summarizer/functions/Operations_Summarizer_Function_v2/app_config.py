"""Load the Operations Summarizer config node.

Fixed identifiers match ``src/config/configLocation.ts``.
The Cognite SDK retries HTTP 429 responses.
"""

from __future__ import annotations

from typing import Any

SCHEMA_SPACE = "dm_sol_ops_summarizer_app"
INSTANCE_SPACE = "inst_ops_summarizer_app_config"
DATA_MODEL_EXTERNAL_ID = "OpsSummarizerConfig_SOL"
VIEW_EXTERNAL_ID = "OpsSummarizerConfig"
NODE_EXTERNAL_ID = "ops_summarizer_config"
NODE_PATH = f"{INSTANCE_SPACE}/{NODE_EXTERNAL_ID}"
SUPPORTED_SCHEMA_VERSION = 1


class AppConfigError(Exception):
    """The config node is missing, unreadable, or has an unsupported schemaVersion."""


def load_app_config(client: Any) -> dict[str, Any]:
    """Read and validate the config node. Raises ``AppConfigError`` when it cannot be used."""
    if client is None:
        raise AppConfigError(
            f"config node {NODE_PATH}: a CogniteClient is required to read the node."
        )
    view_version = _read_view_version(client)
    properties = _read_node_properties(client, view_version)
    schema_version = properties.get("schemaVersion")
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise AppConfigError(
            f"config node {NODE_PATH}: unsupported schemaVersion {schema_version}. "
            f"This release supports schemaVersion {SUPPORTED_SCHEMA_VERSION}."
        )
    return properties


def _read_view_version(client: Any) -> str:
    try:
        models = client.data_modeling.data_models.list(
            space=SCHEMA_SPACE,
            all_versions=False,
            limit=None,
        )
    except Exception as exc:
        raise _request_failed(exc) from exc

    for model in models:
        if getattr(model, "external_id", None) != DATA_MODEL_EXTERNAL_ID:
            continue
        for view in getattr(model, "views", []) or []:
            if getattr(view, "external_id", None) != VIEW_EXTERNAL_ID:
                continue
            version = getattr(view, "version", None)
            if version:
                return str(version)
    raise AppConfigError(
        f"config node {NODE_PATH}: data model {DATA_MODEL_EXTERNAL_ID} was not found "
        f"in {SCHEMA_SPACE}. Grant dataModels:read on that space."
    )


def _read_node_properties(client: Any, view_version: str) -> dict[str, Any]:
    from cognite.client.data_classes.data_modeling import NodeId, ViewId

    view_id = ViewId(SCHEMA_SPACE, VIEW_EXTERNAL_ID, view_version)
    try:
        result = client.data_modeling.instances.retrieve(
            nodes=NodeId(INSTANCE_SPACE, NODE_EXTERNAL_ID),
            sources=view_id,
        )
    except Exception as exc:
        raise _request_failed(exc) from exc

    nodes = list(getattr(result, "nodes", None) or [])
    if not nodes:
        raise AppConfigError(
            f"config node {NODE_PATH}: node was not found in {INSTANCE_SPACE}. "
            "Grant dataModelInstances:read on that space."
        )
    properties = _properties_for_view(nodes[0], view_version)
    if not properties:
        raise AppConfigError(
            f"config node {NODE_PATH}: node properties for {VIEW_EXTERNAL_ID}/{view_version} were empty."
        )
    return properties


def _properties_for_view(node: Any, view_version: str) -> dict[str, Any] | None:
    raw = getattr(node, "properties", None) or {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        external = getattr(key, "external_id", None)
        version = getattr(key, "version", None)
        if external == VIEW_EXTERNAL_ID and str(version) == view_version:
            return value
    return None


def _request_failed(exc: Exception) -> AppConfigError:
    status = getattr(exc, "code", None)
    if not isinstance(status, int):
        status = getattr(exc, "status_code", None)
    status_text = f"status {status}" if isinstance(status, int) else "a request error"
    return AppConfigError(
        f"config node {NODE_PATH}: CDF request failed with {status_text}. "
        f"Grant dataModels:read on {SCHEMA_SPACE} and dataModelInstances:read on {INSTANCE_SPACE}."
    )
