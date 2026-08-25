#!/usr/bin/env python3
"""
Run the 3D CAD contextualization function locally against the project in .env.

This invokes the *real* function handler
(functions/fn_context_3d_cad_asset_contextualization/handler.py) using the local
(now-fixed) code, while reading runtime parameters from the deployed CDF extraction
pipeline config. So the annotation extraction pipeline + its config must already be
deployed in the target project (cdf build && cdf deploy).

It is a REAL run: it reads RAW, runs entity matching, writes RAW good/bad, and (unless
debug=True in the pipeline config) creates the DM 3D chain via /3d/contextualization/cad
and writes an ExtractionPipelineRun. For a dry pass, set `debug: True` in the deployed
pipeline config first.

Requirements (install locally if needed):
    pip install -r functions/fn_context_3d_cad_asset_contextualization/requirements.txt

Usage:
    python scripts/run_cad_contextualization.py --ep ep_ctx_3d_<loc>_<src>_annotation
    python scripts/run_cad_contextualization.py        # build EP id from .env default_location/source_name
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Reuse the .env loader + client builder from the cleanup script (same scripts/ dir).
from cleanup_dm_contextualization import build_client, load_env

FUNC_DIR = Path(__file__).resolve().parent.parent / "functions" / "fn_context_3d_cad_asset_contextualization"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--ep",
        default=None,
        help="annotation extraction pipeline externalId (e.g. ep_ctx_3d_<loc>_<src>_annotation)",
    )
    args = parser.parse_args()

    env = load_env()
    client = build_client(env)

    ep = args.ep or env.get("EXTRACTION_PIPELINE_EXT_ID")
    if not ep:
        loc = env.get("default_location")
        src = env.get("source_name")
        if loc and src:
            ep = f"ep_ctx_3d_{loc}_{src}_annotation"
    if not ep:
        sys.exit(
            "ERROR: could not determine the annotation extraction pipeline externalId.\n"
            "       Pass --ep <externalId>, or set EXTRACTION_PIPELINE_EXT_ID, or "
            "default_location + source_name in .env."
        )

    # Make the function package importable, then load its handler (executes its top-level imports).
    sys.path.insert(0, str(FUNC_DIR))
    try:
        import handler  # noqa: E402
    except ModuleNotFoundError as e:
        sys.exit(
            f"ERROR: missing dependency for the function ({e}). Install with:\n"
            f"       pip install -r {FUNC_DIR / 'requirements.txt'}"
        )

    print("=" * 70)
    print(f"Project : {env.get('CDF_PROJECT')}  ({env.get('CDF_CLUSTER')})")
    print(f"Function: fn_context_3d_cad_asset_contextualization")
    print(f"EP      : {ep}")
    print("Mode    : REAL run (writes RAW + DM unless pipeline config has debug: True)")
    print("=" * 70)

    result = handler.handle({"ExtractionPipelineExtId": ep}, client)
    print("\nResult:", result)
    print(
        "\nNext: check the extraction pipeline run + RAW contextualization_good/bad, then "
        "run the quality-check function and the docs §8 chain verification."
    )


if __name__ == "__main__":
    main()
