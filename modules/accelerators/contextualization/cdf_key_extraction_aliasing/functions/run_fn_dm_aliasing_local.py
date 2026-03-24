#!/usr/bin/env python3
"""
Run fn_dm_aliasing locally using the latest local key-extraction results.

What it does:
1) Loads aliasing config YAML from local file
2) Normalizes config to workflow-compatible shape ({externalId, config: {parameters, data}})
3) Loads the latest *_local_key_extraction_results.json from local_run_results
4) Passes entities_keys_extracted + config directly to fn_dm_aliasing.handler.handle()
5) Writes aliasing results to a local JSON file

Usage (from library_fresh):
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_aliasing_local.py

Custom input/output:
  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/run_fn_dm_aliasing_local.py \
    --key-results-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/local_run_results/<file>.json \
    --config-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/extraction_pipelines/ctx_aliasing_site_prod.config.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

# Ensure library_fresh is importable as project root.
REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.handler import (
    handle,
)


def _ensure_workflow_config_shape(raw_cfg: Dict[str, Any], cfg_path: Path) -> Dict[str, Any]:
    """
    Normalize config to workflow-compatible shape expected by handlers.

    Accepts:
    - {externalId, config: {parameters, data}}  -> unchanged
    - {parameters, data}                         -> wrapped
    - any other dict                             -> returned as-is
    """
    if not isinstance(raw_cfg, dict):
        raise ValueError("Config YAML must parse to a dictionary")

    if (
        "config" in raw_cfg
        and isinstance(raw_cfg["config"], dict)
        and "data" in raw_cfg["config"]
    ):
        return raw_cfg

    if "parameters" in raw_cfg and "data" in raw_cfg:
        return {
            "externalId": cfg_path.stem.replace(".config", ""),
            "config": raw_cfg,
        }

    return raw_cfg


def _resolve_path(path_value: str) -> Path:
    p = Path(path_value)
    return p if p.is_absolute() else REPO_ROOT / p


def _latest_key_results_file(results_dir: Path) -> Path:
    candidates = sorted(
        results_dir.glob("*_local_key_extraction_results.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No *_local_key_extraction_results.json found in {results_dir}"
        )
    return candidates[0]


def main() -> int:
    module_dir = Path(__file__).resolve().parent.parent
    default_results_dir = module_dir / "local_run_results"
    default_aliasing_config = (
        module_dir / "extraction_pipelines" / "ctx_aliasing_site_prod.config.yaml"
    )

    parser = argparse.ArgumentParser(
        description="Run fn_dm_aliasing locally using latest local key-extraction results."
    )
    parser.add_argument("--config-path", default=str(default_aliasing_config))
    parser.add_argument("--key-results-path", default="")
    parser.add_argument("--results-dir", default=str(default_results_dir))
    parser.add_argument("--results-path", default="")
    args = parser.parse_args()

    results_dir = _resolve_path(args.results_dir)
    config_path = _resolve_path(args.config_path)
    key_results_path = (
        _resolve_path(args.key_results_path)
        if args.key_results_path
        else _latest_key_results_file(results_dir)
    )

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    if not key_results_path.exists():
        raise FileNotFoundError(f"Key extraction result file not found: {key_results_path}")

    print(f"Loading aliasing config: {config_path}")
    raw_cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    workflow_cfg = _ensure_workflow_config_shape(raw_cfg, config_path)

    print(f"Loading key extraction results: {key_results_path}")
    key_payload = json.loads(key_results_path.read_text(encoding="utf-8"))
    entities_keys_extracted = key_payload.get("entities_keys_extracted", {})
    if not isinstance(entities_keys_extracted, dict):
        raise ValueError(
            "Invalid key extraction results format: 'entities_keys_extracted' must be a dictionary"
        )
    if not entities_keys_extracted:
        raise ValueError(
            "Input key extraction result has empty 'entities_keys_extracted'; nothing to alias"
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_output = results_dir / f"{ts}_local_aliasing_results.json"
    output_path = _resolve_path(args.results_path) if args.results_path else default_output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Same local pattern used for key extraction:
    # pass local config + local input payload directly to handler (no CDF pipeline fetch).
    data = {
        "logLevel": "DEBUG",
        "verbose": False,
        "config": workflow_cfg,
        "entities_keys_extracted": entities_keys_extracted,
    }

    print("Running fn_dm_aliasing.handler.handle() locally...")
    result = handle(data, client=None)

    output_payload = {
        "generated_at": ts,
        "status": result.get("status"),
        "message": result.get("message"),
        "input_key_results_path": str(key_results_path),
        "input_config_path": str(config_path),
        "total_tags_processed": data.get("total_tags_processed", 0),
        "total_aliases_generated": data.get("total_aliases_generated", 0),
        "aliasing_results": data.get("aliasing_results", []),
    }
    output_path.write_text(
        json.dumps(output_payload, indent=2, default=str),
        encoding="utf-8",
    )

    print(f"Wrote aliasing results: {output_path}")
    print(f"Final status: {result.get('status')}")
    return 0 if result.get("status") == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
