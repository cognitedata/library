"""Bounded retry of a stage run after a DMS query timeout."""

import time

from cognite.client.exceptions import CogniteAPIError
from fa_constants import QUERY_TIMEOUT_BACKOFF_SECONDS, QUERY_TIMEOUT_MAX_RETRIES
from services.LoggerService import CogniteFunctionLogger

HTTP_STATUS_REQUEST_TIMEOUT = 408


def is_query_timeout(error: CogniteAPIError) -> bool:
    return error.code == HTTP_STATUS_REQUEST_TIMEOUT


class QueryTimeoutRetry:
    """Waits before the next run of a stage whose query timed out, and gives up after a few in a row.

    A timeout that keeps coming back is a query DMS cannot serve rather than load that passes, so
    retrying it until the time budget is spent only delays the failure.
    """

    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.consecutive_timeouts = 0

    def wait(self, error: CogniteAPIError) -> None:
        """Sleeps with exponential backoff before the next run.

        Raises:
            CogniteAPIError: The timeout, once the retries are used up.
        """
        self.consecutive_timeouts += 1
        if self.consecutive_timeouts > QUERY_TIMEOUT_MAX_RETRIES:
            self.logger.error(
                message=f"Query timed out {self.consecutive_timeouts} times in a row - giving up", error=error
            )
            raise error
        sleep_seconds = QUERY_TIMEOUT_BACKOFF_SECONDS * 2 ** (self.consecutive_timeouts - 1)
        self.logger.warning(
            f"Query timed out ({self.consecutive_timeouts}/{QUERY_TIMEOUT_MAX_RETRIES}) - "
            f"retrying in {sleep_seconds}s: {error.message}"
        )
        time.sleep(sleep_seconds)

    def reset(self) -> None:
        """Called once a run got past its queries, so only timeouts in a row count."""
        self.consecutive_timeouts = 0
