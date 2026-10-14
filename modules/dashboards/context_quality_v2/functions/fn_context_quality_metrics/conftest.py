"""Make the function's own packages importable when its tests run from the repository root."""

import sys
from pathlib import Path

FUNCTION_DIR = str(Path(__file__).resolve().parent)
if FUNCTION_DIR not in sys.path:
    sys.path.insert(0, FUNCTION_DIR)
