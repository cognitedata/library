import logging
import sys
from typing import Literal


class _StdoutHandler(logging.Handler):
    """Emit through print so output always follows the current sys.stdout."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            print(self.format(record), file=sys.stdout, flush=True)
        except Exception:
            self.handleError(record)


class CogniteFunctionLogger:
    """Logger wrapper around the standard logging module."""

    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO") -> None:
        self._logger = logging.getLogger("bootstrap")
        self._logger.setLevel(log_level.upper())
        self._logger.propagate = False
        if not self._logger.handlers:
            handler = _StdoutHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self._logger.addHandler(handler)

    def debug(self, message: str) -> None:
        self._logger.debug(message)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)
