"""Shared helpers for stage entrypoints (logging contract and failure responses)."""

from cognite.client.exceptions import CogniteAPIError

# Failures we surface as a structured function response instead of an unhandled crash.
STAGE_REPORTABLE_ERRORS: tuple[type[BaseException], ...] = (
    CogniteAPIError,
    ValueError,
    RuntimeError,
)


def failure_response(exc: BaseException) -> dict[str, str]:
    """Build the CDF function failure payload for a caught stage error."""
    return {"status": "failure", "message": str(exc)}
