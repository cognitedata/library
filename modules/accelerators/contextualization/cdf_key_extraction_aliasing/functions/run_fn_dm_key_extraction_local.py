#!/usr/bin/env python3
"""
Run fn_dm_key_extraction locally using entities fetched from CDF.

What it does:
1) Loads extraction config YAML (Geleen config by default)
2) Queries entities from CDF using source_views in the config
3) Caches queried entities to a local JSON file
4) Runs fn_dm_key_extraction.handler.handle() in standalone mode
   (config + entities passed directly; no RAW writes)
5) Writes extraction results to a local JSON file

Usage (from repo root):
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_key_extraction_local.py

Reuse cached entities (skip CDF fetch):
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_key_extraction_local.py --use-cache modules/accelerators/contextualization/cdf_key_extraction_aliasing/local_run_results/<cache_file>.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

# Ensure the function folder is importable as project root for local runs.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import ViewId

from fn_dm_key_extraction.cdf_adapter import load_config_from_yaml
from fn_dm_key_extraction.handler import handle


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
    filters_cfg = view_cfg.get("filters", []) or []
    filter_exprs: List[Any] = [dm.filters.HasData(views=[view_id])]

    for f in filters_cfg:
        op = str(f.get("operator", "")).upper()
        prop = f.get("target_property")
        values = f.get("values")
        if not prop:
            continue
        prop_ref = view_id.as_property_ref(prop)

        if op == "IN" and isinstance(values, list):
            filter_exprs.append(dm.filters.In(property=prop_ref, values=values))
        elif op == "EQUALS":
            if isinstance(values, list):
                if len(values) == 1:
                    filter_exprs.append(dm.filters.Equals(property=prop_ref, value=values[0]))
                elif len(values) > 1:
                    filter_exprs.append(
                        dm.filters.Or(
                            *[dm.filters.Equals(property=prop_ref, value=v) for v in values]
                        )
                    )
            else:
                filter_exprs.append(dm.filters.Equals(property=prop_ref, value=values))
        elif op == "CONTAINSALL" and isinstance(values, list):
            filter_exprs.append(dm.filters.ContainsAll(property=prop_ref, values=values))
        elif op == "SEARCH":
            if isinstance(values, list) and values:
                filter_exprs.append(dm.filters.Search(property=prop_ref, value=values[0]))
            elif isinstance(values, str) and values:
                filter_exprs.append(dm.filters.Search(property=prop_ref, value=values))
        elif op == "EXISTS":
            filter_exprs.append(dm.filters.Exists(property=prop_ref))

    if len(filter_exprs) == 1:
        return filter_exprs[0]
    return dm.filters.And(*filter_exprs)


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
    """Fetch entities from CDF instances.list based on engine config source_views."""
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
        batch_size = int(view_cfg.get("batch_size", 100))

        if not view_external_id or not instance_space:
            continue

        view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
        filter_expr = _build_view_filter(view_cfg, view_id)
        per_view_limit = limit_override if limit_override is not None else batch_size
        if total_samples is not None:
            remaining = total_samples - len(entities)
            if remaining <= 0:
                break
            limit = min(per_view_limit, remaining)
        else:
            limit = per_view_limit

        rows = client.data_modeling.instances.list(
            instance_type="node",
            space=instance_space,
            sources=[view_id],
            filter=filter_expr,
            limit=limit,
        )

        for inst in rows:
            ext_id = getattr(inst, "external_id", None)
            if not ext_id:
                continue
            props = _extract_props(inst, view_id)

            entity_data = entities.setdefault(
                ext_id,
                {
                    "entity_type": entity_type,
                    "view_space": view_space,
                    "view_external_id": view_external_id,
                    "view_version": view_version,
                    "instance_space": instance_space,
                },
            )
            for p in include_properties:
                if p in props:
                    entity_data[p] = props[p]

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

    # Run fn_dm_key_extraction handler in standalone mode with fetched entities.
    data = {
        "config": engine_config,
        "entities": entities,
        "logLevel": "DEBUG",
        "verbose": False,
    }
    print("Running fn_dm_key_extraction.handler.handle() locally...")
    result = handle(data, client=None)

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
