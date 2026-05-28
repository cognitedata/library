#!/usr/bin/env python3
"""Deploy one built workflow to CDF (Cognite Functions + Workflow artifacts via SDK).

Examples::

  cd modules/accelerators/contextualization/cdf_discovery
  PYTHONPATH=functions:transform:transform/scripts:. \\
    python transform/scripts/deploy_workflow_cdf.py --workflow aliasing_workflow

  PYTHONPATH=functions:transform:transform/scripts:. \\
    python transform/scripts/deploy_workflow_cdf.py --workflow aliasing_workflow --dry-run
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys
from pathlib import Path
from typing import cast


def _transform_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _discovery_root(transform_root: Path) -> Path:
    if transform_root.name == "transform" and (transform_root.parent / "transform").resolve() == transform_root:
        return transform_root.parent
    return transform_root


def _ensure_scripts_on_path() -> Path:
    tr = _transform_root()
    sd = tr / "scripts"
    dr = _discovery_root(tr)
    stale_functions = (tr / "functions").resolve()

    def _keep(entry: str) -> bool:
        if not entry:
            return True
        try:
            return Path(entry).resolve() != stale_functions
        except OSError:
            return not entry.replace("\\", "/").endswith("transform/functions")

    sys.path[:] = [p for p in sys.path if _keep(p)]
    # Insert in reverse so ``functions`` ends up first on ``sys.path``.
    for p in (str(sd), str(tr), str(dr / "functions")):
        while p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    return tr


def _validate_artifacts(paths: dict[str, Path]) -> None:
    missing = [str(p) for k, p in paths.items() if k != "dir" and not p.is_file()]
    if not paths["dir"].is_dir():
        raise SystemExit(f"Workflow output directory missing: {paths['dir']} (run build first)")
    if missing:
        raise SystemExit(
            "Missing workflow artifacts (run ``python module.py transform build --workflow …`` first):\n  "
            + "\n  ".join(missing)
        )


def _run_subprocess(argv: list[str], *, cwd: Path, dry_run: bool) -> int:
    print("+", " ".join(argv), file=sys.stderr)
    if dry_run:
        return 0
    proc = subprocess.run(argv, cwd=str(cwd))
    return int(proc.returncode)


def main(argv: list[str] | None = None) -> int:
    transform_root = _ensure_scripts_on_path()
    discovery_root = _discovery_root(transform_root)

    from deploy_kea_functions_cdf_api import DeployFunctionsMode, deploy_kea_functions
    from deploy_workflows_cdf_api import deploy_workflows
    from workflow_deploy_paths import resolve_workflow_artifacts

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--workflow",
        required=True,
        metavar="ID",
        help="Workflow instance id (same as ``module.py transform build --workflow``).",
    )
    p.add_argument(
        "--scope-suffix",
        default="",
        metavar="SUFFIX",
        help="Optional workflows/<suffix>/ subfolder (empty = flat workflows/).",
    )
    p.add_argument("--module-root", type=Path, default=transform_root, help="transform/ directory")
    p.add_argument("--skip-build", action="store_true", help="Do not run module.py transform build first.")
    p.add_argument("--dry-run", action="store_true", help="Print planned upserts only.")
    p.add_argument(
        "--allow-unresolved-placeholders",
        action="store_true",
        help="Allow ``{{ … }}`` in YAML (CDF may still reject).",
    )
    p.add_argument(
        "--deploy-functions",
        choices=("never", "if-missing", "if-stale", "always"),
        default="if-stale",
        metavar="MODE",
    )
    args = p.parse_args(argv)

    tr = args.module_root.resolve()
    paths = resolve_workflow_artifacts(tr, args.workflow.strip(), args.scope_suffix)
    _validate_artifacts(paths)

    fn_mode = cast(DeployFunctionsMode, args.deploy_functions)
    print(
        "Deploying via CDF APIs: Cognite Functions, then Workflow / WorkflowVersion / WorkflowTrigger.\n"
        f"Artifacts: {paths['workflow'].name}, {paths['workflow_version'].name}, {paths['trigger'].name}\n"
        f"Function deploy mode: {fn_mode}",
        file=sys.stderr,
    )

    module_py = discovery_root / "module.py"
    if not args.skip_build:
        build_cmd = [
            sys.executable,
            str(module_py),
            "transform",
            "build",
            "--workflow",
            args.workflow.strip(),
        ]
        suffix = str(args.scope_suffix or "").strip()
        if suffix:
            build_cmd.extend(["--scope-suffix", suffix])
        rc = _run_subprocess(build_cmd, cwd=discovery_root, dry_run=args.dry_run)
        if rc != 0:
            return rc
        _validate_artifacts(paths)

    log_buf = io.StringIO()

    if args.dry_run:
        try:
            deploy_kea_functions(None, discovery_root, mode=fn_mode, dry_run=True, log=log_buf)
            deploy_workflows(
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
        deploy_kea_functions(client, discovery_root, mode=fn_mode, dry_run=False, log=log_buf)
        deploy_workflows(
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
