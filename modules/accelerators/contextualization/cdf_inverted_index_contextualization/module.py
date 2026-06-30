#!/usr/bin/env python3
"""
Local CLI for inverted index contextualization (RAW backend default).

Requires CDF credentials in .env (repo root or module directory).

  python module.py build-metadata [--dry-run]
  python module.py build-annotations [--file FILE_EXT_ID] [--dry-run]
  python module.py migrate [--dry-run] [--skip-purge] [--scope-key global]
  python module.py partition-health
  python module.py reshard-scope --scope-key 'site:Rotterdam|unit:U100'
  python module.py query --terms P-101A --scope-key 'site:Rotterdam|unit:U100'
  python module.py query --terms P-101A --scope-key 'site:A|unit:1' --scope-key 'site:A|unit:2'
  python module.py query --terms P-101A --all-scopes
  python module.py tag-reuse-audit --all-scopes
  python module.py target-driven [--instance-id ASSET_P101] [--scope-key global] [--scope-override] [--dry-run]
  python module.py score --file-id FILE_PID_12 [--scope-key 'site:Rotterdam|unit:U100']
  python module.py list-by-file --file-id FILE_PID_12 [--scope-key global]
  python module.py deltas --file-id FILE_PID_12
  python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
  python module.py demo [--json]   # offline in-memory demo
  python module.py ui [--api-port PORT] [--vite-port PORT] [--no-browser]
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import List

_PACKAGE_ROOT = Path(__file__).resolve().parent
_UI_DIR = _PACKAGE_ROOT / "ui"
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))


def _load_env() -> None:
    from local_runner.env import load_env

    load_env(_PACKAGE_ROOT)


def _wait_for_http(url: str, *, timeout_sec: float = 45.0, poll_interval: float = 0.25) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 400:
                    return True
        except (urllib.error.URLError, OSError, TimeoutError):
            pass
        time.sleep(poll_interval)
    return False


def _run_ui(argv: List[str]) -> int:
    p = argparse.ArgumentParser(
        prog="module.py ui",
        description="Host the inverted index operator UI (FastAPI + Vite).",
    )
    p.add_argument("--api-host", default="127.0.0.1", help="Bind address for FastAPI")
    p.add_argument("--api-port", type=int, default=8787, help="Port for FastAPI (default 8787)")
    p.add_argument("--vite-port", type=int, default=5195, help="Port for Vite dev server (default 5195)")
    p.add_argument("--no-browser", action="store_true", help="Do not open a browser tab")
    p.add_argument("--no-reload", action="store_true", help="Disable uvicorn --reload")
    args = p.parse_args(argv)

    if not shutil.which("npm"):
        print("npm not found on PATH; install Node.js.", file=sys.stderr)
        return 1
    if not (_UI_DIR / "package.json").is_file():
        print(f"Missing {_UI_DIR / 'package.json'}", file=sys.stderr)
        return 1
    if not (_UI_DIR / "node_modules").is_dir():
        print("Installing UI dependencies (npm install)…")
        r = subprocess.run(["npm", "install"], cwd=str(_UI_DIR), check=False)
        if r.returncode != 0:
            return r.returncode

    env = {
        **os.environ,
        "PYTHONPATH": str(_PACKAGE_ROOT),
        "CDF_INVERTED_INDEX_ROOT": str(_PACKAGE_ROOT),
    }
    api_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "ui.server.main:app",
        "--host",
        args.api_host,
        "--port",
        str(args.api_port),
        "--log-level",
        "debug",
    ]
    if not args.no_reload:
        api_cmd.append("--reload")

    procs: List[subprocess.Popen] = []

    def _terminate_all() -> None:
        for pr in reversed(procs):
            if pr.poll() is None:
                pr.terminate()
        for pr in reversed(procs):
            try:
                pr.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pr.kill()

    atexit.register(_terminate_all)

    def _handle_sigint(_signum: int, _frame: object) -> None:
        _terminate_all()
        sys.exit(130)

    signal.signal(signal.SIGINT, _handle_sigint)

    print(f"Starting API on http://{args.api_host}:{args.api_port} …")
    procs.append(subprocess.Popen(api_cmd, cwd=str(_PACKAGE_ROOT), env=env))
    time.sleep(0.8)

    vite_env = {
        **os.environ,
        "VITE_API_PROXY": f"http://{args.api_host}:{args.api_port}",
    }
    vite_cmd = ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(args.vite_port)]
    print(f"Starting Vite on http://127.0.0.1:{args.vite_port} …")
    procs.append(subprocess.Popen(vite_cmd, cwd=str(_UI_DIR), env=vite_env))

    ui_url = f"http://127.0.0.1:{args.vite_port}/"
    if _wait_for_http(ui_url):
        print(f"UI ready at {ui_url}")
        if not args.no_browser:
            webbrowser.open(ui_url)
    else:
        print(f"Timed out waiting for {ui_url}", file=sys.stderr)

    for pr in procs:
        pr.wait()
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from local_runner.demo import run_demo

    output = _PACKAGE_ROOT / "local_run_results" if not args.no_write else None
    report = run_demo(output_dir=output)
    if args.json or not output:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(f"Demo complete — report written to {report.get('output_file')}")
        print(f"Index entries: {report['index_entry_count']}")
        print(f"Target-driven references found: {report['target_driven']['references_found']}")
        print(f"Overall score: {report['contextualization_score']['overall_score']}")
    return 0


def cmd_build_metadata(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_build_metadata, print_json

    print_json(cmd_build_metadata(dry_run=args.dry_run))
    return 0


def cmd_build_annotations(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_build_annotations, print_json

    print_json(
        cmd_build_annotations(
            file_external_id=args.file_id,
            detection_mode=args.detection_mode,
            dry_run=args.dry_run,
        )
    )
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_query, print_json

    terms = [t.strip() for t in args.terms.split(",") if t.strip()]
    source_types = (
        [s.strip() for s in args.source_types.split(",") if s.strip()]
        if args.source_types
        else None
    )
    print_json(
        cmd_query(
            terms,
            all_scopes=args.all_scopes,
            match_scope_keys=args.scope_key,
            source_types=source_types,
            min_confidence=args.min_confidence,
            reuse_only=args.reuse_only,
            hits_only=args.hits_only,
        )
    )
    return 0


def cmd_tag_reuse_audit(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_tag_reuse_audit, print_json

    print_json(
        cmd_tag_reuse_audit(
            all_scopes=args.all_scopes,
            match_scope_keys=args.scope_key,
            min_scope_count=args.min_scope_count,
            limit=args.limit,
        )
    )
    return 0


def cmd_virtual_tags(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_virtual_tags, parse_scope_key_args, print_json

    print_json(
        cmd_virtual_tags(
            all_scopes=args.all_scopes,
            match_scope_keys=parse_scope_key_args(args.scope_key),
            dry_run=args.dry_run,
            limit=args.limit,
            term_selection_mode=args.term_selection_mode,
            progress_interval=args.progress_interval,
        )
    )
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_migrate, parse_scope_key_args, print_json

    print_json(
        cmd_migrate(
            dry_run=args.dry_run,
            purge=not args.skip_purge,
            match_scope_keys=parse_scope_key_args(args.scope_key),
        )
    )
    return 0


def cmd_partition_health(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_partition_health, print_json

    print_json(cmd_partition_health())
    return 0


def cmd_reshard_scope(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_reshard_scope, print_json

    print_json(cmd_reshard_scope(args.scope_key, dry_run=args.dry_run))
    return 0


def cmd_target_driven(args: argparse.Namespace) -> int:
    from local_runner.commands import (
        cmd_target_driven,
        parse_instance_id_args,
        parse_scope_key_args,
        print_json,
    )

    print_json(
        cmd_target_driven(
            instance_external_ids=parse_instance_id_args(None, args.instance_id) or None,
            incoming_view_key=args.view_key,
            view_external_id=args.view_external_id,
            instance_space=args.space,
            dry_run=args.dry_run,
            min_confidence=args.min_confidence,
            match_scope_keys=parse_scope_key_args(args.scope_key),
            scope_lookup_override=args.scope_override,
            max_assets=args.max_assets,
            progress_interval=args.progress_interval,
            query_property=args.query_property,
            force=args.force,
        )
    )
    return 0


def cmd_score(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_score, print_json

    print_json(
        cmd_score(
            args.file_id,
            match_scope_key=args.scope_key,
            file_space=args.space,
        )
    )
    return 0


def cmd_list_by_file(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_list_by_file, print_json

    source_types = (
        [s.strip() for s in args.source_types.split(",") if s.strip()]
        if args.source_types
        else None
    )
    print_json(
        cmd_list_by_file(
            args.file_id,
            match_scope_key=args.scope_key,
            file_space=args.space,
            source_types=source_types,
            limit=args.limit,
        )
    )
    return 0


def cmd_deltas(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_deltas, print_json

    print_json(
        cmd_deltas(
            args.file_id,
            match_scope_key=args.scope_key,
            file_space=args.space,
        )
    )
    return 0


def cmd_index_detections(args: argparse.Namespace) -> int:
    import json
    from pathlib import Path

    from local_runner.commands import cmd_index_detections, print_json

    detections = json.loads(Path(args.detections_file).read_text(encoding="utf-8"))
    if not isinstance(detections, list):
        raise SystemExit("detections file must contain a JSON array")
    print_json(
        cmd_index_detections(
            detections=detections,
            detection_mode=args.mode,
            write_mode=args.write_mode,
            file_external_id=args.file_id,
            file_space=args.space,
            dry_run=args.dry_run,
        )
    )
    return 0


def cmd_index_metadata_instance(args: argparse.Namespace) -> int:
    from local_runner.commands import cmd_index_metadata_instance, print_json

    print_json(
        cmd_index_metadata_instance(
            args.instance_id,
            view_external_id=args.view,
            incoming_view_key=args.view_key,
            instance_space=args.space,
            write_mode=args.write_mode,
            dry_run=args.dry_run,
        )
    )
    return 0


def cmd_whoami(_args: argparse.Namespace) -> int:
    from local_runner.commands import print_auth_banner

    print_auth_banner()
    return 0


def cmd_handle_subscription(args: argparse.Namespace) -> int:
    import json
    from pathlib import Path

    from local_runner.client import create_cognite_client
    from inverted_index.subscription import handle_aliases_subscription_event
    from inverted_index.config_loader import build_runtime_config

    event = json.loads(Path(args.event_file).read_text(encoding="utf-8"))
    client = create_cognite_client()
    result = handle_aliases_subscription_event(
        client,
        event,
        dry_run=args.dry_run,
        runtime_config=build_runtime_config(),
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_invoke_fn(args: argparse.Namespace) -> int:
    import importlib
    import json

    fn_id = args.function_id
    mapping = {
        "fn_idx_build_metadata": "fn_idx_build_metadata.handler",
        "fn_idx_build_annotations": "fn_idx_build_annotations.handler",
        "fn_idx_target_driven": "fn_idx_target_driven.handler",
        "fn_idx_handle_subscription": "fn_idx_handle_subscription.handler",
        "fn_idx_score": "fn_idx_score.handler",
        "fn_idx_deltas": "fn_idx_deltas.handler",
        "fn_idx_upsert_detections": "fn_idx_upsert_detections.handler",
        "fn_idx_index_metadata_instance": "fn_idx_index_metadata_instance.handler",
    }
    module_path = mapping.get(fn_id)
    if not module_path:
        print(f"Unknown function id: {fn_id}", file=sys.stderr)
        return 1
    functions_dir = _PACKAGE_ROOT / "functions"
    if str(functions_dir) not in sys.path:
        sys.path.insert(0, str(functions_dir))
    payload = json.loads(args.data) if args.data else {}
    mod = importlib.import_module(module_path)
    from local_runner.client import create_cognite_client

    client = create_cognite_client()
    result = mod.handle(payload, client)
    print(json.dumps(result, indent=2, default=str))
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "ui":
        return _run_ui(sys.argv[2:])
    _load_env()
    parser = argparse.ArgumentParser(
        description="CDF inverted index contextualization (RAW backend)"
    )
    sub = parser.add_subparsers(dest="command")

    demo = sub.add_parser("demo", help="Offline end-to-end demo (memory backend)")
    demo.add_argument("--json", action="store_true")
    demo.add_argument("--no-write", action="store_true")
    demo.set_defaults(func=cmd_demo)

    bm = sub.add_parser("build-metadata", help="Build metadata index to RAW")
    bm.add_argument("--dry-run", action="store_true")
    bm.set_defaults(func=cmd_build_metadata)

    ba = sub.add_parser("build-annotations", help="Build diagram annotation index to RAW")
    ba.add_argument("--file-id", default=None, help="Limit to one file external id")
    ba.add_argument(
        "--detection-mode",
        choices=["standard", "pattern", "all"],
        default="all",
    )
    ba.add_argument("--dry-run", action="store_true")
    ba.set_defaults(func=cmd_build_annotations)

    mg = sub.add_parser(
        "migrate",
        help="Purge RAW partitions and rebuild metadata + annotation indexes (file-as-reference migration)",
    )
    mg.add_argument(
        "--scope-key",
        action="append",
        default=None,
        help="Partitions to purge (default: all registered scopes, or fallback global)",
    )
    mg.add_argument(
        "--skip-purge",
        action="store_true",
        help="Rebuild without truncating partitions (merge only; legacy rows may remain)",
    )
    mg.add_argument("--dry-run", action="store_true")
    mg.set_defaults(func=cmd_migrate)

    ph = sub.add_parser(
        "partition-health",
        help="Report RAW partition row counts and reshard recommendations",
    )
    ph.set_defaults(func=cmd_partition_health)

    rs = sub.add_parser(
        "reshard-scope",
        help="Migrate a unified scope partition into term-bucket tables",
    )
    rs.add_argument(
        "--scope-key",
        required=True,
        help="match_scope_key to reshard (must have term_partition.enabled)",
    )
    rs.add_argument("--dry-run", action="store_true")
    rs.set_defaults(func=cmd_reshard_scope)

    q = sub.add_parser("query", help="Query RAW index by terms + scope(s)")
    q.add_argument("--terms", required=True, help="Comma-separated alias/terms")
    scope_group = q.add_mutually_exclusive_group(required=True)
    scope_group.add_argument(
        "--scope-key",
        action="append",
        default=None,
        help="match_scope_key (repeatable; comma-separated values allowed)",
    )
    scope_group.add_argument(
        "--all-scopes",
        action="store_true",
        help="Query every scope in the partition registry",
    )
    q.add_argument("--source-types", default=None, help="Optional comma-separated filter")
    q.add_argument("--min-confidence", type=float, default=0.0)
    q.add_argument(
        "--reuse-only",
        action="store_true",
        help="Omit by_term rows that are not cross-scope duplicates",
    )
    q.add_argument(
        "--hits-only",
        action="store_true",
        help="Print flat hit list only (backward-compatible scripts)",
    )
    q.set_defaults(func=cmd_query)

    tra = sub.add_parser(
        "tag-reuse-audit",
        help="Scan scope partitions for tags reused across scopes",
    )
    audit_scope_group = tra.add_mutually_exclusive_group(required=True)
    audit_scope_group.add_argument(
        "--scope-key",
        action="append",
        default=None,
        help="match_scope_key (repeatable; comma-separated values allowed)",
    )
    audit_scope_group.add_argument(
        "--all-scopes",
        action="store_true",
        help="Audit every scope in the partition registry",
    )
    tra.add_argument(
        "--min-scope-count",
        type=int,
        default=2,
        help="Minimum distinct scopes for a tag to be reported (default 2)",
    )
    tra.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Max duplicate terms to return (default 5000)",
    )
    tra.set_defaults(func=cmd_tag_reuse_audit)

    vt = sub.add_parser(
        "virtual-tags",
        help="Create virtual CogniteAsset tags from scoped index terms (UC4)",
    )
    vt_scope_group = vt.add_mutually_exclusive_group(required=True)
    vt_scope_group.add_argument(
        "--scope-key",
        action="append",
        default=None,
        help="match_scope_key (repeatable; comma-separated values allowed)",
    )
    vt_scope_group.add_argument(
        "--all-scopes",
        action="store_true",
        help="Process every scope in the partition registry",
    )
    vt.add_argument("--dry-run", action="store_true")
    vt.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max eligible terms to process (0 = no cap)",
    )
    vt.add_argument(
        "--term-selection-mode",
        choices=["all", "missing_tags_only"],
        default=None,
        help="Override virtual_tag_creation.term_selection_mode from config",
    )
    vt.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Emit stderr progress every N scanned terms (0 to disable)",
    )
    vt.set_defaults(func=cmd_virtual_tags)

    td = sub.add_parser(
        "target-driven",
        help="Run target-driven contextualization (all assets, or selected with --instance-id)",
    )
    td.add_argument(
        "--instance-id",
        action="append",
        default=None,
        metavar="ID",
        help="Asset/file/equipment/timeseries external id; repeatable, comma-separated allowed",
    )
    td.add_argument(
        "--view-key",
        default=None,
        help="Incoming view key from direct_relation_config.views (e.g. asset, file)",
    )
    td.add_argument(
        "--view-external-id",
        default=None,
        help="DM view external id when --view-key is omitted",
    )
    td.add_argument("--space", default="cdf_cdm")
    td.add_argument("--min-confidence", type=float, default=0.6)
    td.add_argument("--dry-run", action="store_true")
    td.add_argument(
        "--scope-key",
        action="append",
        default=None,
        help="Scope filter or lookup override; repeatable, comma-separated allowed",
    )
    td.add_argument(
        "--scope-override",
        action="store_true",
        help="Batch only: use --scope-key for index lookup on every asset",
    )
    td.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="Batch only: optional cap on number of assets to process",
    )
    td.add_argument(
        "--progress-interval",
        type=int,
        default=100,
        help="Batch only: emit stderr progress every N assets (0 to disable)",
    )
    td.add_argument(
        "--query-property",
        default=None,
        help="Instance property path for index query terms (default from config, usually aliases)",
    )
    td.add_argument(
        "--force",
        action="store_true",
        help="Bypass target-driven dedupe cooldown",
    )
    td.add_argument(
        "--backfill",
        action="store_true",
        help="Explicit fleet backfill (default when --instance-id is omitted)",
    )
    td.set_defaults(func=cmd_target_driven)

    sc = sub.add_parser("score", help="Contextualization score for a file")
    sc.add_argument("--file-id", required=True)
    sc.add_argument("--scope-key", default=None)
    sc.add_argument("--space", default="cdf_cdm")
    sc.set_defaults(func=cmd_score)

    lf = sub.add_parser("list-by-file", help="List index entries for a CogniteFile")
    lf.add_argument("--file-id", required=True)
    lf.add_argument("--scope-key", default=None)
    lf.add_argument("--space", default="cdf_cdm")
    lf.add_argument("--source-types", default=None, help="Optional comma-separated filter")
    lf.add_argument("--limit", type=int, default=5000)
    lf.set_defaults(func=cmd_list_by_file)

    dl = sub.add_parser("deltas", help="Detection mode deltas for a file")
    dl.add_argument("--file-id", required=True)
    dl.add_argument("--scope-key", default=None)
    dl.add_argument("--space", default="cdf_cdm")
    dl.set_defaults(func=cmd_deltas)

    idet = sub.add_parser(
        "index-detections",
        help="Incremental diagram detection index write (external detection results)",
    )
    idet.add_argument(
        "--mode",
        choices=["standard", "pattern"],
        default="pattern",
        help="Default detection_mode when not set per detection row",
    )
    idet.add_argument("--detections-file", required=True, help="JSON array of detection dicts")
    idet.add_argument(
        "--write-mode",
        choices=["upsert", "replace"],
        default="replace",
        help="replace removes existing file+mode postings before write",
    )
    idet.add_argument("--file-id", default=None, help="Parent file external id (required for replace)")
    idet.add_argument("--space", default="cdf_cdm", help="File instance space")
    idet.add_argument("--dry-run", action="store_true")
    idet.set_defaults(func=cmd_index_detections)

    imi = sub.add_parser(
        "index-metadata-instance",
        help="Incremental metadata index write for one DM instance",
    )
    imi.add_argument("--instance-id", required=True, help="Target instance external id")
    view_group = imi.add_mutually_exclusive_group(required=True)
    view_group.add_argument("--view", help="DM view external id (e.g. CogniteEquipment)")
    view_group.add_argument(
        "--view-key",
        dest="view_key",
        help="View key from direct_relation_config.views (e.g. equipment)",
    )
    imi.add_argument("--space", default="cdf_cdm", help="Instance space")
    imi.add_argument(
        "--write-mode",
        choices=["upsert", "replace"],
        default="replace",
        help="replace removes existing instance metadata postings before write",
    )
    imi.add_argument("--dry-run", action="store_true")
    imi.set_defaults(func=cmd_index_metadata_instance)

    sub.add_parser("whoami", help="Verify .env CDF connection").set_defaults(func=cmd_whoami)

    hs = sub.add_parser(
        "handle-subscription",
        help="Process a subscription event JSON file (aliases change)",
    )
    hs.add_argument("--event-file", required=True, help="Path to JSON event payload")
    hs.add_argument("--dry-run", action="store_true")
    hs.set_defaults(func=cmd_handle_subscription)

    inv = sub.add_parser("invoke-fn", help="Invoke a CDF function handler locally")
    inv.add_argument("function_id", help="Function externalId (e.g. fn_idx_build_metadata)")
    inv.add_argument("--data", default="{}", help="JSON payload for handle(data)")
    inv.set_defaults(func=cmd_invoke_fn)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
