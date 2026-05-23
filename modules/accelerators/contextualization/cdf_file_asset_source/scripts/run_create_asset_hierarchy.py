#!/usr/bin/env python3
"""Run create_asset_hierarchy locally."""

import json
import sys
from pathlib import Path

module_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(module_root.parent.parent.parent.parent))
sys.path.insert(0, str(module_root))

from local_runner.run import run_pipeline_step

if __name__ == "__main__":
    out = run_pipeline_step("create")
    print(json.dumps(out, indent=2, default=str))
    raise SystemExit(0 if out.get("succeeded") else 1)
