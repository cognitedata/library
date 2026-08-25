import logging
import os
from typing import Literal


class CogniteFunctionLogger:
    """Thin wrapper over stdlib logging so CDF Functions captures output correctly."""

    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"):
        self.log_level = log_level.upper()
        self._logger = logging.getLogger("cdf_three_dimension_qc")
        self._logger.setLevel(self.log_level)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self._logger.addHandler(handler)
        self._logger.propagate = False

    def debug(self, message: str) -> None:
        self._logger.debug(message)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)


# Default module-level logger (LOG_LEVEL env var optional, e.g. DEBUG, INFO, WARNING, ERROR)
log = CogniteFunctionLogger(os.environ.get("LOG_LEVEL", "INFO"))
