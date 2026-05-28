#!/usr/bin/env python3
"""Start a CDF workflow execution for one built workflow and poll until terminal status."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_TERMINAL = frozenset({"completed", "failed", "terminated", "timed_out"})


def _transform_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _bootstrap_paths() -> Path:
    tr = _transform_root()
    for p in (str(tr), str(tr / "functions"), str(tr / "scripts")):
        if p not in sys.path:
            sys.path.insert(0, p)
    return tr


def _status_str(status: object) -> str:
    if isinstance(status, str):
        return status
    return getattr(status, "value", str(status))


def main(argv: list[str] | None = None) -> int:
    transform_root = _bootstrap_paths()
    from cdf_workflow_io import (
        assert_expected_workflow_input_keys,
        shallow_has_toolkit_placeholder,
        substitute_instance_space_placeholder,
        workflow_external_id_from_yaml,
        workflow_input_from_json,
        workflow_input_from_trigger_yaml,
        workflow_version_from_yaml,
    )
    from workflow_deploy_paths import resolve_workflow_artifacts

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workflow", required=True, help="Workflow instance id.")
    p.add_argument("--scope-suffix", default="", help="Optional workflows/<suffix>/ subfolder.")
    p.add_argument("--module-root", type=Path, default=transform_root, help="transform/ directory")
    p.add_argument("--workflow-external-id", default=None, help="Override Workflow externalId.")
    p.add_argument("--input-json", type=Path, default=None, help="JSON workflow input instead of trigger YAML.")
    p.add_argument("--instance-space", default=None, help="Replace {{instance_space}} in trigger input.")
    p.add_argument("--timeout-seconds", type=float, default=7200.0)
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--workflow-client-id",
        default=os.environ.get("KEA_WORKFLOW_CLIENT_ID"),
    )
    p.add_argument(
        "--workflow-client-secret",
        default=os.environ.get("KEA_WORKFLOW_CLIENT_SECRET"),
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    paths = resolve_workflow_artifacts(args.module_root.resolve(), args.workflow.strip(), args.scope_suffix)
    for key in ("workflow", "workflow_version", "trigger"):
        if not paths[key].is_file():
            raise SystemExit(f"Missing {paths[key]} — run build first")

    wf_ext = (args.workflow_external_id or "").strip() or workflow_external_id_from_yaml(paths["workflow"])
    version = workflow_version_from_yaml(paths["workflow_version"])
    if args.input_json is not None:
        wf_input = workflow_input_from_json(args.input_json.expanduser().resolve())
    else:
        wf_input = workflow_input_from_trigger_yaml(paths["trigger"])
    assert_expected_workflow_input_keys(wf_input)

    inst_space = (
        (args.instance_space or "").strip()
        or (os.environ.get("KEA_INSTANCE_SPACE") or "").strip()
        or (os.environ.get("CDF_INSTANCE_SPACE") or "").strip()
    )
    if inst_space:
        wf_input = substitute_instance_space_placeholder(wf_input, inst_space)
        logger.info("Substituted {{instance_space}} in workflow input.")

    if shallow_has_toolkit_placeholder(wf_input):
        logger.warning("Workflow input still contains {{ … }} placeholders.")

    logger.info("workflow_external_id=%s version=%s", wf_ext, version)

    if args.dry_run:
        logger.info("dry-run: would run executions.run with input keys %s", sorted(wf_input.keys()))
        return 0

    from cognite.client.data_classes.iam import ClientCredentials

    from local_runner.client import create_cognite_client
    from local_runner.env import load_env

    load_env()
    try:
        client = create_cognite_client()
    except Exception as e:
        logger.error("Failed to create CogniteClient: %s", e)
        return 1

    client_credentials = None
    cid = (args.workflow_client_id or "").strip()
    csec = (args.workflow_client_secret or "").strip()
    if cid and csec:
        client_credentials = ClientCredentials(cid, csec)
    elif cid or csec:
        logger.error("Both --workflow-client-id and --workflow-client-secret are required when either is set.")
        return 1

    try:
        started = client.workflows.executions.run(
            wf_ext,
            version,
            input=wf_input,
            client_credentials=client_credentials,
        )
    except Exception as e:
        logger.error("executions.run failed: %s", e)
        return 1

    ex_id = started.id
    logger.info("Started workflow execution id=%s status=%s", ex_id, _status_str(started.status))

    deadline = time.monotonic() + float(args.timeout_seconds)
    last_status = _status_str(started.status)
    detail: object | None = started
    while last_status not in _TERMINAL:
        if time.monotonic() > deadline:
            logger.error("Timeout after %s s (last status=%s)", args.timeout_seconds, last_status)
            return 1
        time.sleep(max(0.5, float(args.poll_interval)))
        try:
            detail = client.workflows.executions.retrieve_detailed(ex_id)
        except Exception as e:
            logger.warning("retrieve_detailed failed: %s", e)
            continue
        if detail is None:
            continue
        last_status = _status_str(detail.status)
        logger.info("execution id=%s status=%s", ex_id, last_status)

    if last_status == "completed":
        logger.info("Workflow execution completed successfully.")
        return 0
    reason = getattr(detail, "reason_for_incompletion", None) if detail is not None else None
    logger.error("Workflow execution finished with status=%s reason=%s", last_status, reason)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
