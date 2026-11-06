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


@pytest.fixture(autouse=True)
def a_call_of_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test is a call of its own: what an earlier one noted (cq.resilience) is forgotten, and no call's time
    budget
    is running unless the test starts one."""
    from cq import resilience

    resilience.begin_call()
    monkeypatch.setattr(resilience, "_call_started", None)
