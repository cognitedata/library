"""Push Workflow / WorkflowVersion / WorkflowTrigger YAML to CDF via the Cognite SDK."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, TextIO, cast

_DISCOVERY_ROOT = Path(__file__).resolve().parent.parent.parent
_FUNCTIONS_ROOT = _DISCOVERY_ROOT / "functions"
_STALE_FUNCTIONS = (_DISCOVERY_ROOT / "transform" / "functions").resolve()


def _drop_stale_functions_paths() -> None:
    kept: list[str] = []
    for entry in sys.path:
        if not entry:
            kept.append(entry)
            continue
        try:
            if Path(entry).resolve() == _STALE_FUNCTIONS:
                continue
        except OSError:
            if entry.replace("\\", "/").endswith("transform/functions"):
                continue
        kept.append(entry)
    sys.path[:] = kept


_drop_stale_functions_paths()
_fn_root = str(_FUNCTIONS_ROOT)
while _fn_root in sys.path:
    sys.path.remove(_fn_root)
sys.path.insert(0, _fn_root)
for _key in list(sys.modules):
    if _key == "functions" or _key.startswith("functions."):
        del sys.modules[_key]

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowList,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionList,
    WorkflowVersionUpsert,
    WorkflowTriggerUpsert,
)

from cdf_deploy_workflow_guard import scope_suffix_from_trigger
from cdf_workflow_io import shallow_has_toolkit_placeholder


def _yaml_mapping(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping at root: {path}")
    return data


def _workflow_version_body_for_cdf_api(wv_raw: dict[str, Any]) -> dict[str, Any]:
    from functions.cdf_fn_common.workflow_compile.codegen import (
        escape_workflow_version_document_for_cdf,
    )

    return escape_workflow_version_document_for_cdf(wv_raw)


def _resolve_data_set_id(client: CogniteClient, data_set_external_id: str | None) -> int | None:
    if data_set_external_id is None:
        return None
    ext = str(data_set_external_id).strip().strip("'\"")
    if not ext:
        return None
    ds = client.data_sets.retrieve(external_id=ext)
    if ds is None:
        raise RuntimeError(
            f"Data set not found for external_id={ext!r}. Create it in CDF or fix dataSetExternalId in Workflow YAML."
        )
    return int(ds.id)


def _workflow_trigger_authentication_from_env() -> dict[str, str] | None:
    client_id = (
        (os.environ.get("KEA_WORKFLOW_TRIGGER_CLIENT_ID") or "").strip()
        or (os.environ.get("IDP_CLIENT_ID") or "").strip()
        or (os.environ.get("COGNITE_CLIENT_ID") or "").strip()
        or (os.environ.get("CLIENT_ID") or "").strip()
    )
    client_secret = (
        (os.environ.get("KEA_WORKFLOW_TRIGGER_CLIENT_SECRET") or "").strip()
        or (os.environ.get("IDP_CLIENT_SECRET") or "").strip()
        or (os.environ.get("COGNITE_CLIENT_SECRET") or "").strip()
        or (os.environ.get("CLIENT_SECRET") or "").strip()
    )
    if client_id and client_secret:
        return {"clientId": client_id, "clientSecret": client_secret}
    return None


def _trigger_dict_for_sdk(raw: dict[str, Any], *, require_deploy_credentials: bool) -> dict[str, Any]:
    out = dict(raw)
    out.pop("authentication", None)
    env_auth = _workflow_trigger_authentication_from_env()
    if env_auth:
        out["authentication"] = env_auth
    elif require_deploy_credentials:
        raise ValueError(
            "WorkflowTrigger credentials are not stored in YAML. For SDK deploy, set "
            "KEA_WORKFLOW_TRIGGER_CLIENT_ID and KEA_WORKFLOW_TRIGGER_CLIENT_SECRET, or "
            "IDP_CLIENT_ID and IDP_CLIENT_SECRET, or COGNITE_CLIENT_ID / COGNITE_CLIENT_SECRET."
        )
    return out


def deploy_workflows(
    client: CogniteClient | None,
    *,
    workflow_yaml: Path,
    workflow_version_yaml: Path,
    workflow_trigger_yaml: Path,
    dry_run: bool = False,
    allow_unresolved_placeholders: bool = False,
    log: TextIO | None = None,
) -> None:
    sink = log or sys.stderr
    scope_suffix_from_trigger(workflow_trigger_yaml)
    if workflow_yaml.parent != workflow_trigger_yaml.parent:
        raise ValueError(
            "Workflow, WorkflowVersion, and WorkflowTrigger YAML must live in the same workflows/ folder"
        )

    wf_raw = _yaml_mapping(workflow_yaml)
    wv_raw = _yaml_mapping(workflow_version_yaml)
    trig_raw = _yaml_mapping(workflow_trigger_yaml)

    if not allow_unresolved_placeholders:
        for label, blob in (
            ("Workflow", wf_raw),
            ("WorkflowVersion", wv_raw),
            ("WorkflowTrigger", trig_raw),
        ):
            if shallow_has_toolkit_placeholder(blob):
                raise ValueError(
                    f"{label} YAML still contains `{{{{ ... }}}}` placeholders. "
                    "Pass --allow-unresolved-placeholders to deploy anyway."
                )

    ext_id = wf_raw.get("externalId")
    if not ext_id:
        raise ValueError(f"Workflow YAML missing externalId: {workflow_yaml}")

    ds_id: int | None = None
    if not dry_run:
        if client is None:
            raise ValueError("CogniteClient is required when dry_run is False")
        ds_id = _resolve_data_set_id(client, wf_raw.get("dataSetExternalId"))

    wf_upsert = WorkflowUpsert(
        external_id=str(ext_id),
        description=wf_raw.get("description"),
        data_set_id=ds_id,
    )

    if dry_run:
        print(f"[dry-run] Would upsert Workflow externalId={ext_id!r} dataSetId={ds_id}", file=sink)
        print(
            f"[dry-run] Would upsert WorkflowVersion "
            f"{wv_raw.get('workflowExternalId')!r} version={wv_raw.get('version')!r}",
            file=sink,
        )
        print(f"[dry-run] Would upsert WorkflowTrigger externalId={trig_raw.get('externalId')!r}", file=sink)
        return

    cc = cast(CogniteClient, client)
    cc.workflows.upsert(wf_upsert)
    print(f"Upserted Workflow {ext_id!r}", file=sink)

    wv_api = cc.workflows.versions
    wv_body = _workflow_version_body_for_cdf_api(wv_raw)
    wv_upsert = WorkflowVersionUpsert._load(wv_body)
    if hasattr(wv_api, "upsert"):
        wv_api.upsert(wv_upsert)
    elif hasattr(wv_api, "_create_multiple"):
        # Backward compatibility for older SDK releases.
        wv_api._create_multiple(
            [wv_body],
            list_cls=WorkflowVersionList,
            resource_cls=WorkflowVersion,
            input_resource_cls=WorkflowVersionUpsert,
        )
    else:
        raise RuntimeError("CDF SDK does not support workflow version upsert on this installation.")
    print(
        f"Upserted WorkflowVersion {wv_raw.get('workflowExternalId')!r} {wv_raw.get('version')!r}",
        file=sink,
    )

    trig_obj = WorkflowTriggerUpsert._load(
        _trigger_dict_for_sdk(trig_raw, require_deploy_credentials=True)
    )
    cc.workflows.triggers.upsert(trig_obj)
    print(f"Upserted WorkflowTrigger {trig_raw.get('externalId')!r}", file=sink)
