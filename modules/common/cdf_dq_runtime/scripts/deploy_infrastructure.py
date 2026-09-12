#!/usr/bin/env python3
"""Deploy data quality validation infrastructure (Toolkit pack).

Mirrors ``data-quality-validation-deploy/scripts/deploy_infrastructure.py``:
function, instance workflows (via external DataProducts), time-series workflows,
``data_product_sync``, and historic queue manager.

Install the PyPI pin from ``default.config.yaml`` (``dq_pypi_version``), then::

    python scripts/deploy_infrastructure.py --toolkit-config config.dev.yaml --dry-run
    python scripts/deploy_infrastructure.py --toolkit-config config.dev.yaml

Credentials are read from the Toolkit project ``.env`` (next to ``cdf.toml``) when present,
then from environment variables or optional ``--config-toml``.
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
    configure_deploy_cli,
    data_quality_space,
    load_project_env,
    load_settings_raw,
    print_deploy_context,
    print_infrastructure_summary,
    print_verbose_results,
    resolve_cognite_client,
    resolve_function_secrets,
    resolve_materialized_pack,
)


def main(argv: list[str] | None = None) -> int:
    """Deploy function, containers, external-DataProduct workflows, and timeseries workflows."""
    configure_deploy_cli()

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

    env_path = load_project_env(args.env_file)
    if args.env_file and env_path is None:
        print(f"Warning: --env-file not found: {args.env_file}", file=sys.stderr)

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

    config_toml = Path(args.config_toml)
    client = None if args.dry_run else resolve_cognite_client(config_toml)
    project = client.config.project if client is not None else None

    function_secrets = resolve_function_secrets(config_toml)
    if not function_secrets:
        print("Warning: function secrets missing (orchestrator triggers may fail)", file=sys.stderr)
        print(
            "  Set COGNITE_CLIENT_ID/COGNITE_CLIENT_SECRET or IDP_CLIENT_ID/IDP_CLIENT_SECRET "
            "(or client_id/client_secret in --config-toml for local use)",
            file=sys.stderr,
        )

    with tempfile.TemporaryDirectory(prefix="dq-toolkit-pack-") as tmp:
        pack = resolve_materialized_pack(args, Path(tmp))
        if not pack.settings_path.is_file():
            raise FileNotFoundError(f"settings.yaml not found: {pack.settings_path}")

        settings_raw = load_settings_raw(pack.settings_path)
        dp_sync_cron = None
        if settings_raw.get("config_source") == "dataproduct":
            dp_sync_cron = str(settings_raw.get("data_product_sync_cron", "13 * * * *"))

        credentials_source = str(env_path) if env_path is not None else None
        print_deploy_context(
            project=project,
            package_version=installed_version,
            credentials_source=credentials_source,
            toolkit_config=args.toolkit_config,
            data_product_sync_cron=dp_sync_cron,
        )

        if args.verbose:
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

        if dp_sync_cron is not None:
            deploy_kwargs["deploy_data_product_sync"] = True
            deploy_kwargs["data_product_sync_cron"] = dp_sync_cron

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

    print_infrastructure_summary(results, dry_run=args.dry_run)

    if args.output:
        with Path(args.output).open("w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2, default=str)
        print(f"\nResults written to: {args.output}")

    if args.verbose:
        print()
        print_verbose_results(results)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
