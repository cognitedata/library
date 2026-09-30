"""Shared helpers for stage entrypoints."""

from cognite.client.exceptions import CogniteAPIError
from pydantic import ValidationError

# Logged, then re-raised so the CDF function call fails and the workflow stops.
# ValidationError is included because Pydantic v2 does not inherit it from ValueError.
STAGE_REPORTABLE_ERRORS: tuple[type[BaseException], ...] = (
    CogniteAPIError,
    ValueError,
    RuntimeError,
    ValidationError,
)
