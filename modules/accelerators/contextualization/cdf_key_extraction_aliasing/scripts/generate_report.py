#!/usr/bin/env python3
"""Generate key discovery and aliasing report from latest JSON under tests/results/."""

import logging
import sys
from pathlib import Path

# Repo root + cdf_key_extraction_aliasing package root (for ``local_runner`` imports)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_PACKAGE_ROOT = Path(__file__).resolve().parent.parent
for _p in (_REPO_ROOT, _PACKAGE_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from local_runner.report import generate_report as _generate_report

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Find latest results and generate report."""
    # Results dir: key_extraction_aliasing/tests/results (relative to this script)
    key_extraction_dir = Path(__file__).resolve().parent.parent
    results_dir = key_extraction_dir / "tests" / "results"

    # Find latest extraction file
    extraction_files = sorted(
        results_dir.glob("*_cdf_extraction.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not extraction_files:
        raise FileNotFoundError(f"No extraction results files found in {results_dir}")

    extraction_path = extraction_files[0]

    # Find corresponding aliasing file
    timestamp = extraction_path.stem.replace("_cdf_extraction", "")
    aliasing_path = results_dir / f"{timestamp}_cdf_aliasing.json"

    if not aliasing_path.exists():
        raise FileNotFoundError(f"Aliasing results file not found: {aliasing_path}")

    logger.info(f"Using extraction results: {extraction_path}")
    logger.info(f"Using aliasing results: {aliasing_path}")

    _generate_report(extraction_path, aliasing_path, logger)
    logger.info("✓ Successfully generated key extraction aliasing report")


if __name__ == "__main__":
    main()
