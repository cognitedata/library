import logging
import sys
from typing import Literal

from em_constants import SKIP_MISSING_LOG_LIMIT  # isort: skip


class CogniteFunctionLogger:
    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO") -> None:
        self.logger = logging.getLogger("CogniteFunction")
        self.logger.setLevel(log_level.upper())
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("[%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def missing_property_skip(self, kind: str, external_id: str, skipped_so_far: int) -> None:
        """Log a missing-name skip: first SKIP_MISSING_LOG_LIMIT at WARNING, the rest at DEBUG."""
        message = f"{kind}: {external_id} is missing properties or name, skipping"
        if skipped_so_far <= SKIP_MISSING_LOG_LIMIT:
            self.warning(message)
        else:
            self.debug(message)

    def missing_property_skip_summary(self, resource_plural: str, skipped: int) -> None:
        """After the loop, remind that further skips were muted unless logLevel is DEBUG."""
        if skipped > SKIP_MISSING_LOG_LIMIT:
            self.warning(
                f"Skipped {skipped} {resource_plural} missing properties or name "
                f"(showing first {SKIP_MISSING_LOG_LIMIT}; set logLevel to DEBUG for all)"
            )
