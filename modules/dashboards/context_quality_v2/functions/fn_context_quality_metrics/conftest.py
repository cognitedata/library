"""Make the function's own packages importable when its tests run from the repository root."""

import sys
from pathlib import Path

import pytest

FUNCTION_DIR = str(Path(__file__).resolve().parent)
if FUNCTION_DIR not in sys.path:
    sys.path.insert(0, FUNCTION_DIR)


@pytest.fixture(autouse=True)
def no_location_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    """The project has no location filters unless a test says so: the tests' clients only record Records writes."""
    from cq import locations

    monkeypatch.setattr(locations, "list_locations", lambda _client: [])
