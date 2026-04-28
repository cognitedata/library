"""Push scoped Workflow / WorkflowVersion / WorkflowTrigger YAML to CDF via the Cognite SDK.

Used by ``deploy_scope_cdf.py`` and the operator UI instead of Cognite Toolkit
(``cdf build`` / ``cdf deploy``). Cognite Functions are deployed separately by
``deploy_kea_functions_cdf_api`` when using ``deploy_scope_cdf.py`` (see ``--deploy-functions``).
This module upserts only the three workflow artifacts under ``workflows/<suffix>/``.

**WorkflowTrigger authentication** must not live in git-tracked YAML. Any ``authentication``
key from disk is stripped; the upsert uses ``KEA_WORKFLOW_TRIGGER_CLIENT_*`` or
``IDP_CLIENT_*`` / ``COGNITE_CLIENT_*`` from the process environment (see
``_workflow_trigger_authentication_from_env``).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, TextIO, cast

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

from cdf_deploy_scope_guard import assert_scope_suffix_deployable
from cdf_workflow_io import shallow_has_toolkit_placeholder


def _yaml_mapping(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping at root: {path}")
    return data


def _workflow_version_body_for_cdf_api(wv_raw: dict[str, Any]) -> dict[str, Any]:
    """CDF rejects ``input`` under ``workflowDefinition`` (typed SDK dump omits it)."""
    wd = wv_raw.get("workflowDefinition")
    if not isinstance(wd, dict) or "input" not in wd:
        return wv_raw
    trimmed = {k: v for k, v in wd.items() if k != "input"}
    return {**wv_raw, "workflowDefinition": trimmed}


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
    """OAuth client credentials for the trigger upsert — never taken from git-tracked YAML.

    Resolution order (first non-empty pair wins)::

        KEA_WORKFLOW_TRIGGER_CLIENT_ID / KEA_WORKFLOW_TRIGGER_CLIENT_SECRET
        IDP_CLIENT_ID / IDP_CLIENT_SECRET  (same as Cognite Functions in default.config.yaml)
        COGNITE_CLIENT_ID / COGNITE_CLIENT_SECRET
        CLIENT_ID / CLIENT_SECRET
    """
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
    """Strip any ``authentication`` from disk (no secrets in source control), then set from env when deploying."""
    out = dict(raw)
    out.pop("authentication", None)
    env_auth = _workflow_trigger_authentication_from_env()
    if env_auth:
        out["authentication"] = env_auth
    elif require_deploy_credentials:
        raise ValueError(
            "WorkflowTrigger credentials are not stored in YAML. For SDK deploy, set "
            "KEA_WORKFLOW_TRIGGER_CLIENT_ID and KEA_WORKFLOW_TRIGGER_CLIENT_SECRET, or "
            "IDP_CLIENT_ID and IDP_CLIENT_SECRET (same variables as Cognite Functions in default.config.yaml), "
            "or COGNITE_CLIENT_ID / COGNITE_CLIENT_SECRET in the environment."
        )
    return out


def deploy_scope_workflows(
    client: CogniteClient | None,
    *,
    workflow_yaml: Path,
    workflow_version_yaml: Path,
    workflow_trigger_yaml: Path,
    dry_run: bool = False,
    allow_unresolved_placeholders: bool = False,
    log: TextIO | None = None,
) -> None:
    """Upsert workflow shell (definition + version + trigger) from generated YAML files."""
    sink = log or sys.stderr

    scope_leaf = workflow_trigger_yaml.parent.name
    assert_scope_suffix_deployable(scope_leaf)
    if workflow_yaml.parent != workflow_trigger_yaml.parent:
        raise ValueError(
            "Workflow, WorkflowVersion, and WorkflowTrigger YAML must live in the same workflows/<suffix>/ folder"
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
                    f"{label} YAML still contains `{{{{ ... }}}}` placeholders (Toolkit-style). "
                    "Replace them with literal values for your CDF project, or pass "
                    "--allow-unresolved-placeholders to deploy anyway (CDF may reject the request)."
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

    # Full version document (including workflowDefinition.input) is sent as JSON; the public
    # ``upsert`` helper only accepts WorkflowVersionUpsert, which omits ``input`` on dump.
    wv_api = cc.workflows.versions
    wv_body = _workflow_version_body_for_cdf_api(wv_raw)
    wv_api._create_multiple(  # noqa: SLF001 — public body matches API; SDK omits input on typed dump
        [wv_body],
        list_cls=WorkflowVersionList,
        resource_cls=WorkflowVersion,
        input_resource_cls=WorkflowVersionUpsert,
    )
    print(
        f"Upserted WorkflowVersion {wv_raw.get('workflowExternalId')!r} {wv_raw.get('version')!r}",
        file=sink,
    )

    trig_obj = WorkflowTriggerUpsert._load(
        _trigger_dict_for_sdk(trig_raw, require_deploy_credentials=True)
    )
    cc.workflows.triggers.upsert(trig_obj)
    print(f"Upserted WorkflowTrigger {trig_raw.get('externalId')!r}", file=sink)
