#!/usr/bin/env python3
"""Start a CDF data workflow execution for one scope and poll until a terminal status.

Reads ``workflow`` id from ``default.config.yaml``, ``version`` from the generated
``WorkflowVersion.yaml``, and ``input`` from the leaf ``WorkflowTrigger.yaml`` (same
payload the schedule trigger sends). Override input with ``--input-json``.

Credentials: uses ``local_runner`` env loading and ``create_cognite_client()`` (API key
or OAuth). Optional ``--workflow-client-id`` / ``--workflow-client-secret`` (or env
``KEA_WORKFLOW_CLIENT_ID`` / ``KEA_WORKFLOW_CLIENT_SECRET``) are passed as
``client_credentials`` to ``executions.run`` when both are set.

Examples::

  cd modules/accelerators/contextualization/cdf_key_extraction_aliasing
  export COGNITE_PROJECT=...
  PYTHONPATH=functions:scripts:. python scripts/cdf_workflow_run.py \\
    --scope-suffix site_01 --dry-run

  PYTHONPATH=functions:scripts:. python scripts/cdf_workflow_run.py \\
    --scope-suffix site_01 --poll-interval 5 --timeout-seconds 7200
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_TERMINAL = frozenset({"completed", "failed", "terminated", "timed_out"})


def _module_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _bootstrap_paths() -> Path:
    root = _module_root()
    scripts = root / "scripts"
    for p in (str(root), str(scripts)):
        if p not in sys.path:
            sys.path.insert(0, p)
    return root


def _status_str(status: object) -> str:
    if isinstance(status, str):
        return status
    return getattr(status, "value", str(status))


def main(argv: list[str] | None = None) -> int:
    module_root = _bootstrap_paths()
    from scope_build.hierarchy import load_hierarchy_doc
    from scope_build.mode import scoped_workflow_external_id
    from scope_build.orchestrate import DEFAULT_HIERARCHY, workflow_external_id_from_hierarchy

    from cdf_workflow_io import (
        assert_expected_workflow_input_keys,
        shallow_has_toolkit_placeholder,
        workflow_input_from_json,
        workflow_input_from_trigger_yaml,
        workflow_version_from_yaml,
    )

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--scope-suffix",
        required=True,
        help="Leaf folder under workflows/ (e.g. site_01).",
    )
    p.add_argument(
        "--hierarchy",
        type=Path,
        default=None,
        help=f"Hierarchy YAML (default: <module>/{DEFAULT_HIERARCHY})",
    )
    p.add_argument(
        "--workflow-external-id",
        default=None,
        help="Override workflow external id (default: {workflow from hierarchy}.{suffix}).",
    )
    p.add_argument(
        "--input-json",
        type=Path,
        default=None,
        help="JSON file to use as workflow input instead of parsing the WorkflowTrigger.yaml.",
    )
    p.add_argument(
        "--timeout-seconds",
        type=float,
        default=7200.0,
        help="Max seconds to wait for a terminal execution status (default: 7200).",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds between status polls (default: 5).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved ids, version, and input keys only; do not call CDF.",
    )
    p.add_argument(
        "--workflow-client-id",
        default=os.environ.get("KEA_WORKFLOW_CLIENT_ID"),
        help="OAuth client id for executions.run client_credentials (optional).",
    )
    p.add_argument(
        "--workflow-client-secret",
        default=os.environ.get("KEA_WORKFLOW_CLIENT_SECRET"),
        help="OAuth client secret for executions.run client_credentials (optional).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    hierarchy = args.hierarchy or (module_root / DEFAULT_HIERARCHY)
    doc = load_hierarchy_doc(hierarchy)
    wf_base = workflow_external_id_from_hierarchy(doc)
    suffix = args.scope_suffix.strip()
    from cdf_deploy_scope_guard import assert_scope_suffix_deployable

    assert_scope_suffix_deployable(suffix)
    wf_ext = (args.workflow_external_id or "").strip() or scoped_workflow_external_id(wf_base, suffix)

    wv_path = module_root / "workflows" / suffix / f"{wf_base}.{suffix}.WorkflowVersion.yaml"
    trig_path = module_root / "workflows" / suffix / f"{wf_base}.{suffix}.WorkflowTrigger.yaml"

    version = workflow_version_from_yaml(wv_path)
    if args.input_json is not None:
        wf_input = workflow_input_from_json(args.input_json.expanduser().resolve())
    else:
        wf_input = workflow_input_from_trigger_yaml(trig_path)
    assert_expected_workflow_input_keys(wf_input)

    if shallow_has_toolkit_placeholder(wf_input):
        logger.warning(
            "Workflow input still contains {{ ... }} placeholders. "
            "Resolve templates via cdf build in your Toolkit project, or use --input-json."
        )

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
