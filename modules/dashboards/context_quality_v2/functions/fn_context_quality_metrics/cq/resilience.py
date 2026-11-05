"""Every read of the data modeling service goes through one retry policy, so no query has to handle a busy project itself.

The service answers a busy project with 429 (more than 8 concurrent instance reads), 408 (a query that ran too long)
or a 5xx. `harden` wraps the instance reads of a client so that these are retried with a growing pause, instead of each
caller (and each new kind of query) failing the run on the first one. What a caller can do better than retrying, such
as reading a smaller piece (cq.reader, cq.lookups), it still does, with the error that is left after the retries.
"""

import threading
import time
from collections import Counter
from collections.abc import Callable
from functools import wraps
from typing import Any

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

# code -> (attempts, first pause in seconds, doubled each time). A query that timed out is tried once more; one the
# service refused because it was busy is tried again for longer.
POLICY: dict[int, tuple[int, float]] = {429: (6, 1.0), 408: (2, 3.0), 502: (4, 1.0), 503: (4, 1.0), 504: (4, 1.0)}
READS = ("aggregate", "list", "retrieve", "retrieve_nodes", "retrieve_edges", "search")

# A piece that still met a busy project after those retries is read again, from the start, after a longer pause; only
# while the first attempt left room for it in the ten minutes of a call.
PATIENCE_WAITS = (30.0, 60.0)
PATIENCE_BUDGET = 300.0

# A call of the function is stopped after ten minutes, whatever it is doing. A read gives up after these seconds of it
# (`begin_call`), so that the call fails with the error of the service while there is still time to say so, and the
# task can be tried again with ten minutes of its own.
CALL_BUDGET = 540.0

sleep: Callable[[float], None] = time.sleep
clock: Callable[[], float] = time.monotonic
_call_started: float | None = None


_events: Counter[str] = Counter()
_events_lock = threading.Lock()


def begin_call() -> None:
    """Marks the start of a call of the function: its budget of `CALL_BUDGET` seconds is counted from here, and the
    events noted by the call before it are forgotten."""
    global _call_started
    _call_started = clock()
    with _events_lock:
        _events.clear()


def note(event: str) -> None:
    """Counts something that made the call slower than a plain read: a failed request, a fallback, a restart. The
    reads run in threads, so this is safe to call from them."""
    with _events_lock:
        _events[event] += 1


def events() -> dict[str, int]:
    """What `note` counted since the call began, for the call's output."""
    with _events_lock:
        return dict(_events)


def elapsed() -> float:
    """Seconds since the call began (0 where no call has begun)."""
    return 0.0 if _call_started is None else clock() - _call_started


def time_left() -> float:
    """Seconds of the call's budget that are left. Unlimited where no call has begun (a script, a test)."""
    return float("inf") if _call_started is None else CALL_BUDGET - (clock() - _call_started)


def retrying[**P, R](call: Callable[P, R]) -> Callable[P, R]:
    @wraps(call)
    def run(*args: P.args, **kwargs: P.kwargs) -> R:
        attempt = 0
        while True:
            try:
                return call(*args, **kwargs)
            except CogniteAPIError as error:
                note(f"http {error.code}")
                attempts, pause = POLICY.get(error.code, (1, 0.0))
                attempt += 1
                wait = pause * 2 ** (attempt - 1)
                if attempt >= attempts or time_left() < wait:
                    raise
                sleep(wait)

    return run


class _Instances:
    """The instances API of a client, with its reads retried. Everything else is passed through."""

    def __init__(self, api: Any) -> None:
        self._api = api
        for name in READS:
            if hasattr(api, name):
                setattr(self, name, retrying(getattr(api, name)))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._api, name)


class _Client:
    """The client, which hardens its instances API the first time the data modeling API is asked for, so a call that
    never reads (a run that fails on its configuration) does not touch the client at all."""

    def __init__(self, client: CogniteClient) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._client, name)
        if name == "data_modeling" and not isinstance(getattr(value, "instances", None), _Instances):
            value.instances = _Instances(value.instances)
        return value


def harden(client: CogniteClient) -> CogniteClient:
    """`client`, with the reads of its data modeling instances API retried."""
    return client if isinstance(client, _Client) else _Client(client)  # type: ignore[return-value]


def patiently[R](work: Callable[[], R]) -> R:
    """`work()`, started again after a pause when it fails on a busy project (the errors of `POLICY`) even after the
    retries of each read. `work` must only read and compute: whatever it did is done again, so it must not write."""
    started = clock()
    for wait in PATIENCE_WAITS:
        try:
            return work()
        except CogniteAPIError as error:
            if error.code not in POLICY or clock() - started + wait > PATIENCE_BUDGET:
                raise
        note("restart after a pause")
        sleep(wait)
    return work()
