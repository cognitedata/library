#!/usr/bin/env python3
"""Start a CDF workflow execution for one built workflow and poll until terminal status."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from runtime_paths import ensure_import_paths, transform_root_from_path

logger = logging.getLogger(__name__)

_TERMINAL = frozenset({"completed", "failed", "terminated", "timed_out"})
_DEBUG_LOG_PATH = Path(
    "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-d31d35.log"
)
_DEBUG_SESSION_ID = "d31d35"


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": _DEBUG_SESSION_ID,
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        return


def _status_str(status: object) -> str:
    if isinstance(status, str):
        return status
    return getattr(status, "value", str(status))


def _ensure_workflow_input_run_id(wf_input: dict[str, Any]) -> None:
    configuration = wf_input.get("configuration")
    if not isinstance(configuration, dict):
        return
    parameters = configuration.get("parameters")
    params = dict(parameters) if isinstance(parameters, dict) else {}
    if not str(params.get("run_id") or "").strip():
        params["run_id"] = str(uuid.uuid4())
    configuration["parameters"] = params
    wf_input["configuration"] = configuration


def main(argv: list[str] | None = None) -> int:
    ensure_import_paths(__file__)
    transform_root = transform_root_from_path(__file__)
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
    # region agent log
    _debug_log(
        run_id="pre-run",
        hypothesis_id="H1",
        location="cdf_workflow_run.py:90",
        message="Loaded workflow input before validation",
        data={
            "workflow": args.workflow.strip(),
            "scope_suffix": str(args.scope_suffix or "").strip(),
            "input_keys": sorted(list(wf_input.keys())) if isinstance(wf_input, dict) else [],
            "input_run_id": str(wf_input.get("run_id") or "") if isinstance(wf_input, dict) else "",
            "from_input_json": bool(args.input_json is not None),
        },
    )
    # endregion
    assert_expected_workflow_input_keys(wf_input)

    inst_space = (
        (args.instance_space or "").strip()
        or (os.environ.get("KEA_INSTANCE_SPACE") or "").strip()
        or (os.environ.get("CDF_INSTANCE_SPACE") or "").strip()
    )
    if inst_space:
        wf_input = substitute_instance_space_placeholder(wf_input, inst_space)
        logger.info("Substituted {{instance_space}} in workflow input.")
    if isinstance(wf_input, dict):
        _ensure_workflow_input_run_id(wf_input)
    # region agent log
    _debug_log(
        run_id="pre-run",
        hypothesis_id="H3",
        location="cdf_workflow_run.py:111",
        message="Workflow input after placeholder substitution",
        data={
            "instance_space_arg": bool((args.instance_space or "").strip()),
            "instance_space_env": bool((os.environ.get("KEA_INSTANCE_SPACE") or "").strip()),
            "input_run_id": str(wf_input.get("run_id") or "") if isinstance(wf_input, dict) else "",
        },
    )
    # endregion

    if shallow_has_toolkit_placeholder(wf_input):
        logger.warning("Workflow input still contains {{ … }} placeholders.")

    logger.info("workflow_external_id=%s version=%s", wf_ext, version)

    if args.dry_run:
        logger.info("dry-run: would run executions.run with input keys %s", sorted(wf_input.keys()))
        # region agent log
        _debug_log(
            run_id="dry-run",
            hypothesis_id="H2",
            location="cdf_workflow_run.py:123",
            message="Dry run selected, skipping executions.run",
            data={"input_run_id": str(wf_input.get("run_id") or "") if isinstance(wf_input, dict) else ""},
        )
        # endregion
        return 0

    from cognite.client.data_classes.iam import ClientCredentials

    from local_runner.client import create_cognite_client
    from local_runner.env import load_env

    load_env()
    try:
        client = create_cognite_client()
    except Exception as e:
        logger.error("Failed to create CogniteClient: %s", e)
        # region agent log
        _debug_log(
            run_id="client-init",
            hypothesis_id="H5",
            location="cdf_workflow_run.py:176",
            message="Failed to create CogniteClient",
            data={"error": str(e)},
        )
        # endregion
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
        # region agent log
        _debug_log(
            run_id="pre-execution",
            hypothesis_id="H1",
            location="cdf_workflow_run.py:148",
            message="Calling executions.run",
            data={
                "workflow_external_id": wf_ext,
                "version": version,
                "input_run_id": str(wf_input.get("run_id") or "") if isinstance(wf_input, dict) else "",
                "input_incremental_change_processing": (
                    wf_input.get("incremental_change_processing") if isinstance(wf_input, dict) else None
                ),
            },
        )
        # endregion
        started = client.workflows.executions.run(
            wf_ext,
            version,
            input=wf_input,
            client_credentials=client_credentials,
        )
    except Exception as e:
        logger.error("executions.run failed: %s", e)
        # region agent log
        _debug_log(
            run_id="run-exception",
            hypothesis_id="H5",
            location="cdf_workflow_run.py:219",
            message="executions.run failed",
            data={
                "workflow_external_id": wf_ext,
                "version": version,
                "error": str(e),
                "input_keys": sorted(list(wf_input.keys())) if isinstance(wf_input, dict) else [],
            },
        )
        # endregion
        return 1

    ex_id = started.id
    logger.info("Started workflow execution id=%s status=%s", ex_id, _status_str(started.status))
    # region agent log
    _debug_log(
        run_id=str(ex_id),
        hypothesis_id="H4",
        location="cdf_workflow_run.py:170",
        message="Workflow execution started",
        data={"status": _status_str(started.status), "submitted_input_run_id": str(wf_input.get("run_id") or "")},
    )
    # endregion

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
            # region agent log
            _debug_log(
                run_id=str(ex_id),
                hypothesis_id="H5",
                location="cdf_workflow_run.py:252",
                message="retrieve_detailed failed",
                data={"error": str(e), "last_status": last_status},
            )
            # endregion
            continue
        if detail is None:
            continue
        last_status = _status_str(detail.status)
        logger.info("execution id=%s status=%s", ex_id, last_status)

    if last_status == "completed":
        logger.info("Workflow execution completed successfully.")
        # region agent log
        _debug_log(
            run_id=str(ex_id),
            hypothesis_id="H5",
            location="cdf_workflow_run.py:269",
            message="workflow completed",
            data={"final_status": last_status},
        )
        # endregion
        return 0
    reason = getattr(detail, "reason_for_incompletion", None) if detail is not None else None
    logger.error("Workflow execution finished with status=%s reason=%s", last_status, reason)
    # region agent log
    _debug_log(
        run_id=str(ex_id),
        hypothesis_id="H5",
        location="cdf_workflow_run.py:280",
        message="workflow finished non-terminal-success",
        data={"final_status": last_status, "reason_for_incompletion": str(reason or "")},
    )
    # endregion
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
