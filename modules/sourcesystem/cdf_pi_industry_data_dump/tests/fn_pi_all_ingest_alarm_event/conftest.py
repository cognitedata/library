"""Put the function package on sys.path.

Tests live outside ``fn_pi_all_ingest_alarm_event`` so they are not uploaded with the CDF function.
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_FUNCTION_DIR = _TEST_DIR.parents[1] / "functions" / "fn_pi_all_ingest_alarm_event"
if str(_FUNCTION_DIR) not in sys.path:
    sys.path.insert(0, str(_FUNCTION_DIR))
