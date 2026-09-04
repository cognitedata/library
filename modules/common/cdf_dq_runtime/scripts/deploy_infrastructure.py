#!/usr/bin/env python3
"""Deploy data quality validation infrastructure (Toolkit pack).

Mirrors ``data-quality-validation-deploy/scripts/deploy_infrastructure.py``:
function, instance workflows (via external DataProducts), time-series workflows,
``data_product_sync``, and historic queue manager.

Install the PyPI pin from ``default.config.yaml`` (``dq_pypi_version``), then::

    python scripts/deploy_infrastructure.py --toolkit-config config.dev.yaml --dry-run
    python scripts/deploy_infrastructure.py --toolkit-config config.dev.yaml
    python scripts/deploy_infrastructure.py --toolkit-config config.dev.yaml --enqueue-historic
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from _cli import (
    add_common_args,
    data_quality_space,
    load_settings_raw,
    resolve_cognite_client,
    resolve_function_secrets,
    resolve_materialized_pack,
)


def _print_summary(results: dict[str, object], *, dry_run: bool) -> None:
    print(f"\n{'=' * 60}")
    print("DEPLOYMENT SUMMARY")
    print(f"{'=' * 60}\n")

    function_results = results.get("functions", {})
    if isinstance(function_results, dict):
        func_result = function_results.get("function", {})
        if isinstance(func_result, dict):
            print(f"Function: {func_result.get('function', 'N/A')}")
            print(f"  Status: {str(func_result.get('status', 'unknown')).upper()}")

    for label, key in (
        ("Instance validation (external DataProducts)", "external_dataproduct_workflows"),
        ("Instance validation", "workflows"),
    ):
        workflows = results.get(key, [])
        if isinstance(workflows, list) and workflows:
            deployed = sum(1 for w in workflows if isinstance(w, dict) and w.get("status") == "deployed")
            skipped = sum(1 for w in workflows if isinstance(w, dict) and w.get("status") == "skipped")
            print(f"\n{label}: {len(workflows)} total")
            print(f"  Deployed: {deployed}")
            print(f"  Skipped (up to date): {skipped}")

    ts_workflows = results.get("timeseries_workflows", [])
    if isinstance(ts_workflows, list) and ts_workflows:
        deployed_ts = sum(1 for w in ts_workflows if isinstance(w, dict) and w.get("status") == "deployed")
        skipped_ts = sum(1 for w in ts_workflows if isinstance(w, dict) and w.get("status") == "skipped")
        print(f"\nTime series validation workflows: {len(ts_workflows)} total")
        print(f"  Deployed: {deployed_ts}")
        print(f"  Skipped (up to date): {skipped_ts}")

    dp_sync = results.get("data_product_sync", [])
    if isinstance(dp_sync, list) and dp_sync:
        print(f"\ndata_product_sync workflows: {len(dp_sync)}")

    historic_enqueue = results.get("historic_enqueue")
    if isinstance(historic_enqueue, dict):
        print(
            f"\nHistoric enqueue: {historic_enqueue.get('total_enqueued', 0)} enqueued, "
            f"{historic_enqueue.get('total_skipped', 0)} skipped"
        )

    if dry_run:
        print("\n[DRY RUN] No changes were made")


def main(argv: list[str] | None = None) -> int:
    """Deploy function, containers, external-DataProduct workflows, and timeseries workflows."""
    parser = argparse.ArgumentParser(
        description="Deploy data-quality function, containers, sync workflows, and timeseries validation",
    )
    add_common_args(parser)
    parser.add_argument(
        "--enqueue-historic",
        action="store_true",
        help="Enqueue historic jobs on HistoricJobQueue after deploy (same as deploy repo)",
    )
    parser.add_argument("--output", type=str, help="Write JSON deployment results to this file")
    args = parser.parse_args(argv)

    try:
        from cognite_data_quality import (
            deploy_validation_infrastructure,
            enqueue_historic_validation,
        )
    except ModuleNotFoundError:
        print(
            "cognite-data-quality is not installed. "
            "pip install 'cognite-data-quality==<dq_pypi_version from default.config.yaml>'",
            file=sys.stderr,
        )
        return 1

    try:
        installed_version = version("cognite-data-quality")
    except PackageNotFoundError:
        installed_version = "unknown"
    print(f"Using cognite-data-quality=={installed_version}")

    config_toml = Path(args.config_toml)
    client = None if args.dry_run else resolve_cognite_client(config_toml)
    if client is not None:
        print(f"Connected to project: {client.config.project}")

    function_secrets = resolve_function_secrets(config_toml)
    if function_secrets:
        print("Function secrets: configured (orchestrator can create triggers)")
    else:
        print("Warning: function secrets missing (orchestrator triggers may fail)")
        print(
            "  Set COGNITE_CLIENT_ID/COGNITE_CLIENT_SECRET or IDP_CLIENT_ID/IDP_CLIENT_SECRET "
            "(or client_id/client_secret in --config-toml for local use)"
        )

    with tempfile.TemporaryDirectory(prefix="dq-toolkit-pack-") as tmp:
        pack = resolve_materialized_pack(args, Path(tmp))
        if not pack.settings_path.is_file():
            raise FileNotFoundError(f"settings.yaml not found: {pack.settings_path}")

        settings_raw = load_settings_raw(pack.settings_path)
        print(f"\nsettings_path={pack.settings_path}")
        if pack.views_dir is not None:
            print(f"views_dir={pack.views_dir}")
        if pack.timeseries_dir is not None:
            print(f"timeseries_dir={pack.timeseries_dir}")

        deploy_kwargs: dict[str, object] = {
            "client": client,
            "settings_path": pack.settings_path,
            "views_dir": pack.views_dir,
            "timeseries_dir": pack.timeseries_dir,
            "function_secrets": function_secrets,
            "force": args.force,
            "force_workflows": args.force_workflows,
            "dry_run": args.dry_run,
        }
        if args.force_function:
            deploy_kwargs["force_function"] = True

        if settings_raw.get("config_source") == "dataproduct":
            dp_sync_cron = settings_raw.get("data_product_sync_cron", "13 * * * *")
            deploy_kwargs["deploy_data_product_sync"] = True
            deploy_kwargs["data_product_sync_cron"] = dp_sync_cron
            print(f"data_product_sync: enabled (cron: {dp_sync_cron})")

        print(f"\n{'=' * 60}")
        print("DEPLOYING DATA QUALITY VALIDATION INFRASTRUCTURE")
        print(f"{'=' * 60}\n")

        results = deploy_validation_infrastructure(**deploy_kwargs)

        if args.enqueue_historic and not args.dry_run and client is not None:
            dq_space = data_quality_space(settings_raw)
            print(f"\n{'=' * 60}")
            print("ENQUEUE HISTORIC VALIDATION JOBS")
            print(f"{'=' * 60}\n")
            print(f"  data_quality_space: {dq_space}")
            enqueue_result = enqueue_historic_validation(
                client,
                data_quality_space=dq_space,
                trigger_queue_manager=True,
            )
            results["historic_enqueue"] = enqueue_result.to_dict()
            print(
                f"  Enqueued: {enqueue_result.total_enqueued}, "
                f"skipped: {enqueue_result.total_skipped}, "
                f"warnings: {len(enqueue_result.warnings)}"
            )

    _print_summary(results, dry_run=args.dry_run)

    if args.output:
        with Path(args.output).open("w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)
        print(f"\nResults written to: {args.output}")

    print(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
