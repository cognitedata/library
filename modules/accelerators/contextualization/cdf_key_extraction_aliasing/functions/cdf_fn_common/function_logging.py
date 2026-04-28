"""
CDF function logging helpers: one place to build `CogniteFunctionLogger` from task `data`,
optional injection with stdlib `logging.Logger` bridging, and compatibility with engines that call
`logger.verbose(level, message)`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from .logger import CogniteFunctionLogger


def cognite_function_logger(
    log_level: str,
    verbose: bool,
    *,
    on_invalid_level: str = "info",
    strict_level_names: bool = False,
) -> CogniteFunctionLogger:
    """
    Build `CogniteFunctionLogger` from level + verbose flag.

    ``on_invalid_level`` (when ``strict_level_names`` is false):
    - ``info`` (default): invalid ``log_level`` → ``INFO`` with the given ``verbose`` (most handlers).
    - ``empty``: invalid ``log_level`` → ``CogniteFunctionLogger()`` with defaults.

    ``strict_level_names`` (legacy ``fn_dm_key_extraction`` / ``dependencies.create_logger_service``):
    only the exact strings ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR`` are accepted (no lowercasing);
    anything else returns ``CogniteFunctionLogger()`` and drops ``verbose``, matching the old helper.
    """
    allowed = ("DEBUG", "INFO", "WARNING", "ERROR")
    if strict_level_names:
        ll_raw = str(log_level or "INFO")
        if ll_raw not in allowed:
            return CogniteFunctionLogger()
        return CogniteFunctionLogger(ll_raw, verbose=verbose)  # type: ignore[arg-type]

    ll = str(log_level or "INFO").upper()
    if ll not in allowed:
        if on_invalid_level == "empty":
            return CogniteFunctionLogger()
        return CogniteFunctionLogger("INFO", verbose=verbose)
    return CogniteFunctionLogger(ll, verbose=verbose)  # type: ignore[arg-type]


def function_logger_from_data(data: Dict[str, Any]) -> CogniteFunctionLogger:
    """Standard CDF task payload: ``logLevel``, ``verbose`` (default INFO / false)."""
    loglevel = data.get("logLevel", "INFO")
    verbose = bool(data.get("verbose", False))
    return cognite_function_logger(str(loglevel), verbose, on_invalid_level="info")


class StdlibLoggerAdapter:
    """Maps ``CogniteFunctionLogger``-style ``verbose(level, msg)`` to a stdlib ``logging.Logger``."""

    def __init__(self, base: logging.Logger) -> None:
        self._base = base

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._base.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._base.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._base.warning(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._base.error(message, *args, **kwargs)

    def verbose(self, log_level: str, message: str) -> None:
        lvl = getattr(logging, str(log_level).upper(), logging.INFO)
        self._base.log(lvl, "[VERBOSE] %s", message)


def resolve_function_logger(
    data: Dict[str, Any],
    logger: Optional[Any] = None,
    *,
    strict_level_names: bool = False,
) -> Any:
    """
    Logger for ``handle()`` / tests / local runner.

    - ``logger is None``: build from ``data`` (``strict_level_names`` toggles key-extraction legacy
      level validation; default uses ``function_logger_from_data``).
    - Callable ``verbose`` + ``info``: used as-is (assumes CDF-compatible interface).
    - ``logging.Logger``: wrapped with `StdlibLoggerAdapter`.
    - Otherwise returned as-is (callers must ensure `.verbose` exists where engines require it).
    """
    if logger is None:
        if strict_level_names:
            loglevel = data.get("logLevel", "INFO")
            verbose = bool(data.get("verbose", False))
            return cognite_function_logger(
                str(loglevel), verbose, strict_level_names=True
            )
        return function_logger_from_data(data)
    if callable(getattr(logger, "verbose", None)) and callable(
        getattr(logger, "info", None)
    ):
        return logger
    if isinstance(logger, logging.Logger):
        return StdlibLoggerAdapter(logger)
    return logger
