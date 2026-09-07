#!/usr/bin/env python3
"""Run historic validation after DQS infrastructure is deployed.

Same modes as ``data-quality-validation-deploy`` pipeline helpers::

    python scripts/deploy_pipeline.py --historic-mode enqueue
    python scripts/deploy_pipeline.py --view-external-id YourOrgAsset --historic-mode orchestrator
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from _cli import (
    add_common_args,
    configure_deploy_cli,
    load_project_env,
    print_verbose_results,
    resolve_cognite_client,
    resolve_materialized_pack,
)


def _print_pipeline_summary(result: object) -> None:
    if isinstance(result, dict):
        print("\nHistoric pipeline")
        for key in (
            "status",
            "historic_mode",
            "view_external_id",
            "data_product_external_id",
            "total_enqueued",
            "total_skipped",
            "execution_id",
        ):
            if key in result and result[key] is not None:
                print(f"  {key}: {result[key]}")
        return
    print(f"\nHistoric pipeline: {result}")


def main(argv: list[str] | None = None) -> int:
    """Enqueue or orchestrate historic validation for DataProduct views."""
    configure_deploy_cli()

    parser = argparse.ArgumentParser(description="Historic validation pipeline / enqueue")
    add_common_args(parser)
    parser.add_argument(
        "--view-external-id",
        default=None,
        help="View external ID to validate. Omit to run all views in the DataProduct.",
    )
    parser.add_argument("--view-space", default=None, help="View space when external ID is ambiguous")
    parser.add_argument(
        "--data-product-external-id",
        default=None,
        help="DataProduct external ID. Defaults to settings.external_dataproducts[0] when omitted.",
    )
    parser.add_argument(
        "--historic-mode",
        choices=("enqueue", "orchestrator"),
        default="enqueue",
        help="enqueue (sequential queue, default) or orchestrator (parallel partitions)",
    )
    parser.add_argument(
        "--trigger-queue-manager",
        action="store_true",
        help="Trigger historic_queue_manager after enqueue (enqueue mode only)",
    )
    parser.add_argument("--output", type=str, help="Write JSON pipeline results to this file")
    args = parser.parse_args(argv)

    load_project_env(args.env_file)

    try:
        from cognite_data_quality import deploy_validation_pipeline
        from cognite_data_quality.deploy import load_settings
    except ModuleNotFoundError:
        print(
            "cognite-data-quality is not installed. "
            "pip install 'cognite-data-quality==<dq_pypi_version from default.config.yaml>'",
            file=sys.stderr,
        )
        return 1

    client = resolve_cognite_client(args.config_toml)
    print(f"Connected to project: {client.config.project}")

    with tempfile.TemporaryDirectory(prefix="dq-toolkit-pack-") as tmp:
        pack = resolve_materialized_pack(args, Path(tmp))
        settings = load_settings(pack.settings_path)
        data_product_external_id = args.data_product_external_id
        if not data_product_external_id and settings.external_dataproducts:
            data_product_external_id = settings.external_dataproducts[0].external_id
        result = deploy_validation_pipeline(
            client,
            settings_path=str(pack.settings_path),
            view_external_id=args.view_external_id,
            view_space=args.view_space,
            data_product_external_id=data_product_external_id,
            data_quality_space=settings.effective_config_space,
            historic_mode=args.historic_mode,
            trigger_queue_manager=args.trigger_queue_manager,
            wait=args.historic_mode == "orchestrator",
        )

    if hasattr(result, "to_dict"):
        payload = result.to_dict()
    elif isinstance(result, dict):
        payload = result
    else:
        payload = {"result": str(result)}

    _print_pipeline_summary(payload)

    if args.output:
        with Path(args.output).open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)
        print(f"\nResults written to: {args.output}")

    if args.verbose:
        print()
        print_verbose_results(payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
