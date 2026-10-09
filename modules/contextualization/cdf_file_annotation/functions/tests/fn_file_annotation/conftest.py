"""Put the function package and this test folder on sys.path.

The tests live outside fn_file_annotation so they are not uploaded with the CDF function.
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_FUNCTION_DIR = _TEST_DIR.parents[1] / "fn_file_annotation"
for path in (str(_FUNCTION_DIR), str(_TEST_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)
