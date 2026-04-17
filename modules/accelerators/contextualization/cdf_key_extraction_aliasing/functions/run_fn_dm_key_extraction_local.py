#!/usr/bin/env python3
"""
Run fn_dm_key_extraction locally using entities fetched from CDF.

What it does:
1) Loads extraction config YAML
2) Queries entities from CDF using source_views in the config
3) Caches queried entities to a local JSON file
4) Runs ``key_extraction`` in standalone mode (``cdf_config=None``; entities only; no RAW writes)
5) Writes extraction results to a local JSON file

Usage (from repo root):
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_key_extraction_local.py

Reuse cached entities (skip CDF fetch):
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_key_extraction_local.py --use-cache modules/accelerators/contextualization/cdf_key_extraction_aliasing/local_run_results/<cache_file>.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

# Repo root (library/) so `modules.*` and sibling imports resolve like other local runners.
REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT))

_FETCH_LOG = logging.getLogger(
    "cdf_key_extraction.run_fn_dm_key_extraction_local.fetch"
)

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling.ids import ViewId

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    load_config_from_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
    key_extraction,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.incremental_scope import (
    list_all_instances,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.source_view_filter_build import (
    build_source_view_query_filter,
)


def _build_client_from_env() -> CogniteClient:
    """Create a `CogniteClient` from standard CDF env vars."""
    load_dotenv()

    cdf_project = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    tenant_id = os.environ["IDP_TENANT_ID"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]

    creds = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
    )
    return CogniteClient(
        ClientConfig(
            client_name="fn_dm_key_extraction_local_runner",
            project=cdf_project,
            base_url=f"https://{cdf_cluster}.cognitedata.com",
            credentials=creds,
        )
    )


def _build_view_filter(view_cfg: Dict[str, Any], view_id: ViewId) -> Any:
    """Build a DM filter expression from a source_view config."""
    return build_source_view_query_filter(view_id, view_cfg.get("filters") or [])


def _extract_props(instance: Any, view_id: ViewId) -> Dict[str, Any]:
    """Extract view-scoped properties from an instance dump."""
    dumped = instance.dump()
    return (
        dumped.get("properties", {})
        .get(view_id.space, {})
        .get(f"{view_id.external_id}/{view_id.version}", {})
        or {}
    )


def _fetch_entities_from_cdf(
    client: CogniteClient,
    engine_config: Dict[str, Any],
    limit_override: int | None = None,
    total_samples: int | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Fetch entities from CDF using paginated ``instances.list`` (via ``list_all_instances``)."""
    entities: Dict[str, Dict[str, Any]] = {}
    source_views = engine_config.get("source_views", []) or []

    for view_cfg in source_views:
        if total_samples is not None and len(entities) >= total_samples:
            break

        view_space = view_cfg.get("view_space", "cdf_cdm")
        view_external_id = view_cfg.get("view_external_id")
        view_version = view_cfg.get("view_version", "v1")
        instance_space = view_cfg.get("instance_space")
        entity_type = view_cfg.get("entity_type", "asset")
        include_properties = view_cfg.get("include_properties", []) or []
        batch_size = int(view_cfg.get("batch_size", 1000) or 1000)

        if not view_external_id:
            continue

        view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
        filter_expr = _build_view_filter(view_cfg, view_id)
        per_view_cap = limit_override if limit_override is not None else batch_size
        if per_view_cap is not None and per_view_cap <= 0:
            per_view_cap = None

        n_this_view = 0
        limit_per_page = min(1000, batch_size) if batch_size > 0 else 1000

        for inst in list_all_instances(
            client,
            instance_type="node",
            space=instance_space if instance_space else None,
            sources=[view_id],
            filter=filter_expr,
            limit_per_page=limit_per_page,
            logger=_FETCH_LOG,
            progress_context=(
                f"view={view_space}/{view_external_id}/{view_version} "
                f"limit_per_page={limit_per_page}"
            ),
        ):
            if per_view_cap is not None and n_this_view >= per_view_cap:
                break
            if total_samples is not None and len(entities) >= total_samples:
                break

            ext_id = getattr(inst, "external_id", None)
            if not ext_id:
                continue
            props = _extract_props(inst, view_id)

            inst_sp = getattr(inst, "space", None) or instance_space
            entity_data = entities.setdefault(
                ext_id,
                {
                    "entity_type": entity_type,
                    "view_space": view_space,
                    "view_external_id": view_external_id,
                    "view_version": view_version,
                    "instance_space": inst_sp,
                },
            )
            for p in include_properties:
                if p in props:
                    entity_data[p] = props[p]

            n_this_view += 1

            if total_samples is not None and len(entities) >= total_samples:
                break

    return entities


def main() -> int:
    """CLI entrypoint for local key-extraction run."""
    module_dir = Path(__file__).resolve().parent.parent
    default_results_dir = module_dir / "local_run_results"

    parser = argparse.ArgumentParser(
        description="Fetch CDF entities, cache locally, run fn_dm_key_extraction locally."
    )
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--total-samples", type=int, default=10)
    parser.add_argument("--limit-per-view", type=int, default=0)
    parser.add_argument("--use-cache", default="")
    parser.add_argument("--cache-path", default="")
    parser.add_argument("--results-path", default="")
    args = parser.parse_args()


    config_path = Path(args.config_path)
    if not config_path.is_absolute():
        config_path = REPO_ROOT / config_path

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_results_dir.mkdir(parents=True, exist_ok=True)
    cache_path = Path(args.cache_path) if args.cache_path else default_results_dir / f"{ts}_cdf_entities_cache.json"
    results_path = Path(args.results_path) if args.results_path else default_results_dir / f"{ts}_local_key_extraction_results.json"

    print(f"Loading config: {config_path}")
    engine_config = load_config_from_yaml(str(config_path), validate=False)

    use_cache_path = Path(args.use_cache) if args.use_cache else None
    if use_cache_path:
        if not use_cache_path.is_absolute():
            use_cache_path = REPO_ROOT / use_cache_path
        print(f"Loading entities from cache: {use_cache_path}")
        cached = json.loads(use_cache_path.read_text(encoding="utf-8"))
        entities = cached.get("entities", {})
        if not isinstance(entities, dict):
            raise ValueError("Cache format invalid: 'entities' must be a dictionary")
        print(f"Loaded {len(entities)} entities from cache")
    else:
        client = _build_client_from_env()
        limit_override = args.limit_per_view if args.limit_per_view > 0 else None
        print("Fetching entities from CDF...")
        total_samples = args.total_samples if args.total_samples > 0 else None
        entities = _fetch_entities_from_cdf(
            client,
            engine_config,
            limit_override=limit_override,
            total_samples=total_samples,
        )
        print(f"Fetched {len(entities)} entities")

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "generated_at": ts,
                    "config_path": str(config_path),
                    "entity_count": len(entities),
                    "entities": entities,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print(f"Wrote cache: {cache_path}")

    # Standalone pipeline (cdf_config=None): dev sampling only; production uses workflow + incremental cohort.
    log = StdlibLoggerAdapter(logging.getLogger("fn_dm_key_extraction_local"))
    logging.basicConfig(level=logging.DEBUG)
    engine = KeyExtractionEngine(engine_config)
    data: Dict[str, Any] = {"entities": entities, "logLevel": "DEBUG"}
    print("Running key_extraction pipeline in standalone mode (entities from CDF fetch/cache)...")
    key_extraction(client=None, logger=log, data=data, engine=engine, cdf_config=None)
    result = {"status": "succeeded"}

    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(
        json.dumps(
            {
                "generated_at": ts,
                "status": result.get("status"),
                "message": result.get("message"),
                "keys_extracted": data.get("keys_extracted", 0),
                "entities_keys_extracted": data.get("entities_keys_extracted", {}),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"Wrote extraction results: {results_path}")
    print(f"Final status: {result.get('status')}")
    return 0 if result.get("status") == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
