from re import match
from typing import Literal, cast


# Logger using print
class CogniteFunctionLogger:
    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO", verbose = False):
        self.log_level = log_level.upper()
        self.verbose = verbose

    def _print(self, prefix: str, message: str) -> None:
        if "\n" not in message:
            print(f"{prefix} {message}")
            return
        lines = message.split("\n")
        print(f"{prefix} {lines[0]}")
        prefix_len = len(prefix)
        for line in lines[1:]:
            print(f"{' ' * prefix_len} {line}")

    def debug(self, message: str) -> None:
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)

    def info(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)

    def warning(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)

    def error(self, message: str) -> None:
        self._print("[ERROR]", message)

    def verbose(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"], message: str) -> None:
        if self.verbose:
            match log_level:
                case "DEBUG":
                    self.debug("[VERBOSE] " + message)
                case "INFO":
                    self.info("[VERBOSE] " + message)
                case "WARNING":
                    self.warning("[VERBOSE] " + message)
                case "ERROR":
                    self.error("[VERBOSE] " + message)
