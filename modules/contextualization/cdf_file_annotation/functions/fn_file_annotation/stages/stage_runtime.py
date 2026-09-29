"""Shared helpers for stage entrypoints."""

from cognite.client.exceptions import CogniteAPIError

# Logged, then re-raised so the CDF function call fails and the workflow stops.
STAGE_REPORTABLE_ERRORS: tuple[type[BaseException], ...] = (
    CogniteAPIError,
    ValueError,
    RuntimeError,
)
