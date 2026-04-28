#!/usr/bin/env python3
"""Deploy one scoped workflow to CDF using the Cognite Data Fusion APIs (SDK).

Validates ``workflows/<suffix>/*.Workflow.yaml`` (+ Version + Trigger), optionally runs
``module.py build --scope-suffix``, then upserts **Workflow**, **WorkflowVersion**, and
**WorkflowTrigger** via ``client.workflows`` (no Cognite Toolkit ``cdf build`` / ``cdf deploy``).

Other module resources (functions, RAW, schedules resolved from placeholders) must already
exist or be deployed separately.

Examples::

  cd modules/accelerators/contextualization/cdf_key_extraction_aliasing
  PYTHONPATH=functions:scripts:. python scripts/deploy_scope_cdf.py --scope-suffix site_01

  # Print actions only (no CDF calls, no credentials required for placeholder check only
  # when combined with --allow-unresolved-placeholders if YAML still has ``{{`` tokens)::
  PYTHONPATH=functions:scripts:. python scripts/deploy_scope_cdf.py --scope-suffix site_01 --dry-run

Environment (when not using ``--dry-run``): same as ``module.py run`` / ``cdf_workflow_run.py``
(``COGNITE_*`` or OAuth variables; see ``local_runner``). **WorkflowTrigger** OAuth credentials
are not read from YAML: set ``KEA_WORKFLOW_TRIGGER_CLIENT_ID`` / ``KEA_WORKFLOW_TRIGGER_CLIENT_SECRET``
or ``IDP_CLIENT_ID`` / ``IDP_CLIENT_SECRET`` (or ``COGNITE_CLIENT_ID`` / ``COGNITE_CLIENT_SECRET``)
so the SDK upsert can attach ``authentication`` at deploy time.
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys
from pathlib import Path


def _module_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _ensure_scripts_on_path() -> Path:
    root = _module_root()
    sd = root / "scripts"
    if str(sd) not in sys.path:
        sys.path.insert(0, str(sd))
    return root


def _expected_workflow_paths(module_root: Path, workflow_base: str, suffix: str) -> dict[str, Path]:
    scope_dir = module_root / "workflows" / suffix
    return {
        "dir": scope_dir,
        "workflow": scope_dir / f"{workflow_base}.{suffix}.Workflow.yaml",
        "workflow_version": scope_dir / f"{workflow_base}.{suffix}.WorkflowVersion.yaml",
        "trigger": scope_dir / f"{workflow_base}.{suffix}.WorkflowTrigger.yaml",
    }


def _validate_artifacts(paths: dict[str, Path]) -> None:
    missing = [str(p) for k, p in paths.items() if k != "dir" and not p.is_file()]
    if not paths["dir"].is_dir():
        raise SystemExit(f"Scope directory missing: {paths['dir']}")
    if missing:
        raise SystemExit(
            "Missing workflow artifacts for this scope (run ``python module.py build "
            f"--scope-suffix ...`` first):\n  " + "\n  ".join(missing)
        )


def _run_subprocess(argv: list[str], *, cwd: Path, dry_run: bool) -> int:
    print("+", " ".join(argv), file=sys.stderr)
    if dry_run:
        return 0
    proc = subprocess.run(argv, cwd=str(cwd))
    return int(proc.returncode)


def main(argv: list[str] | None = None) -> int:
    module_root = _ensure_scripts_on_path()
    from scope_build.hierarchy import load_hierarchy_doc
    from scope_build.orchestrate import DEFAULT_HIERARCHY, workflow_external_id_from_hierarchy

    from deploy_scope_workflows_cdf_api import deploy_scope_workflows

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--scope-suffix",
        required=True,
        metavar="SUFFIX",
        help="Leaf folder name under workflows/ (same as module.py build --scope-suffix).",
    )
    p.add_argument(
        "--hierarchy",
        type=Path,
        default=None,
        help=f"Hierarchy YAML (default: <module>/{DEFAULT_HIERARCHY})",
    )
    p.add_argument(
        "--skip-build",
        action="store_true",
        help="Do not run module.py build --scope-suffix before deploying.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned upserts only; do not call CDF (no credentials required).",
    )
    p.add_argument(
        "--allow-unresolved-placeholders",
        action="store_true",
        help="Allow ``{{ ... }}`` strings in YAML (Toolkit placeholders); CDF may still reject invalid payloads.",
    )
    args = p.parse_args(argv)

    from cdf_deploy_scope_guard import assert_scope_suffix_deployable

    assert_scope_suffix_deployable(args.scope_suffix)

    hierarchy = args.hierarchy or (module_root / DEFAULT_HIERARCHY)
    doc = load_hierarchy_doc(hierarchy)
    wf_base = workflow_external_id_from_hierarchy(doc)
    paths = _expected_workflow_paths(module_root, wf_base, args.scope_suffix.strip())
    _validate_artifacts(paths)

    print(
        "Deploying workflow shell via CDF APIs (Workflow, WorkflowVersion, WorkflowTrigger).\n"
        "Ensure functions, data sets, and IAM are already in place.\n"
        f"Artifacts: {paths['workflow'].name}, {paths['workflow_version'].name}, {paths['trigger'].name}",
        file=sys.stderr,
    )

    module_py = module_root / "module.py"
    if not args.skip_build:
        build_cmd = [
            sys.executable,
            str(module_py),
            "build",
            "--scope-suffix",
            args.scope_suffix.strip(),
        ]
        rc = _run_subprocess(build_cmd, cwd=module_root, dry_run=args.dry_run)
        if rc != 0:
            return rc
        _validate_artifacts(paths)

    log_buf = io.StringIO()

    if args.dry_run:
        try:
            deploy_scope_workflows(
                None,
                workflow_yaml=paths["workflow"],
                workflow_version_yaml=paths["workflow_version"],
                workflow_trigger_yaml=paths["trigger"],
                dry_run=True,
                allow_unresolved_placeholders=args.allow_unresolved_placeholders,
                log=log_buf,
            )
        except Exception as e:
            print(str(e), file=sys.stderr)
            return 1
        sys.stdout.write(log_buf.getvalue())
        return 0

    from local_runner.client import create_cognite_client
    from local_runner.env import load_env

    load_env()
    try:
        client = create_cognite_client()
    except Exception as e:
        print(f"Failed to create CogniteClient: {e}", file=sys.stderr)
        return 1

    try:
        deploy_scope_workflows(
            client,
            workflow_yaml=paths["workflow"],
            workflow_version_yaml=paths["workflow_version"],
            workflow_trigger_yaml=paths["trigger"],
            dry_run=False,
            allow_unresolved_placeholders=args.allow_unresolved_placeholders,
            log=log_buf,
        )
    except Exception as e:
        sys.stderr.write(log_buf.getvalue())
        print(str(e), file=sys.stderr)
        return 1

    sys.stdout.write(log_buf.getvalue())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
