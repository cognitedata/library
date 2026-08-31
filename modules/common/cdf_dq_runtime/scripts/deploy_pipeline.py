#!/usr/bin/env python3
"""Run historic validation after DQS infrastructure is deployed.

Same modes as ``data-quality-validation-deploy`` pipeline helpers::

    python scripts/deploy_pipeline.py --historic-mode enqueue
    python scripts/deploy_pipeline.py --view-external-id YourOrgAsset --historic-mode orchestrator
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from _cli import add_common_args, resolve_cognite_client, resolve_materialized_pack


def main(argv: list[str] | None = None) -> int:
    """Enqueue or orchestrate historic validation for DataProduct views."""
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
    args = parser.parse_args(argv)

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
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
