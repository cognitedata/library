"""Step timing and retried API calls for the entity-matching function."""

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from cognite.client.exceptions import CogniteAPIError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# isort: split
from em_constants import HTTP_STATUS_REQUEST_TIMEOUT
from em_logger import CogniteFunctionLogger


@contextmanager
def time_operation(operation_name: str, logger: CogniteFunctionLogger) -> Iterator[None]:
    """Context manager that logs how long the wrapped block ran.

    Timings are diagnostics rather than results, so they are logged at DEBUG and an INFO
    run reads as what the function did rather than how long each step took.
    """
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        logger.debug(f" Time: {operation_name} took {duration:.2f} seconds")


def is_retryable(error: Exception) -> bool:
    """Whether a failed page fetch stands a chance of succeeding on a retry.

    A client error - a missing view, a rejected filter, missing capabilities - means the
    request itself is wrong, so repeating it only delays the failure. Rate limiting,
    read timeouts and server-side errors are transient, as is anything the SDK re-raises
    unclassified from its transport layer, which is why the default is to retry. Bugs in
    this function are the exception: they fail the same way every time. ValueError is
    deliberately not one of them - it covers JSONDecodeError, which a half-read response
    raises and a second read can clear.
    """
    if isinstance(error, CogniteAPIError):
        return error.code in (HTTP_STATUS_REQUEST_TIMEOUT, 429) or (error.code is not None and error.code >= 500)
    return not isinstance(error, (TypeError, AttributeError, NameError, KeyError, IndexError))


class RobustAPIClient:
    """Wrap arbitrary CDF API calls with bounded exponential-backoff retry."""

    def __init__(self, logger: CogniteFunctionLogger) -> None:
        self.logger = logger

    @retry(
        retry=retry_if_exception(is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def robust_api_call[T](self, operation: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Retry the wrapped operation up to 3 times with exponential backoff.

        Only errors that `is_retryable` accepts are retried. Client errors and
        programming mistakes fail on the first attempt. Transient failures sleep
        according to the wait policy (4s capped at 10s, exponentially). If those
        attempts still fail, the last exception is re-raised unchanged.
        """
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            if is_retryable(e):
                self.logger.warning(f"API call failed, retrying: {e}")
            raise


__all__ = [
    "RobustAPIClient",
    "is_retryable",
    "time_operation",
]
