"""Cooperative cancellation for long-running index operations."""

from __future__ import annotations

from collections.abc import Callable


class OperationCancelled(Exception):
    """Raised when an operation is cancelled by the caller."""


def raise_if_cancelled(should_cancel: Callable[[], bool] | None) -> None:
    if should_cancel is not None and should_cancel():
        raise OperationCancelled("Operation cancelled")
