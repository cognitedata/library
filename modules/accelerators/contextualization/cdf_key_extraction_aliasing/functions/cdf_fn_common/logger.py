from re import match
from typing import Any, Literal, cast


def _format_log_message(message: str, args: tuple[Any, ...]) -> str:
    """``logging``-style ``%`` formatting when *args* are supplied; else *message* unchanged."""
    if not args:
        return message
    try:
        return message % args
    except (TypeError, ValueError):
        return f"{message} {' '.join(str(a) for a in args)}"


# Logger using print
class CogniteFunctionLogger:
    def __init__(
        self,
        log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
        verbose=False,
    ):
        self.log_level = log_level.upper()
        self.verbose_on = verbose

    def _print(self, prefix: str, message: str) -> None:
        if "\n" not in message:
            print(f"{prefix} {message}")
            return
        lines = message.split("\n")
        print(f"{prefix} {lines[0]}")
        prefix_len = len(prefix)
        for line in lines[1:]:
            print(f"{' ' * prefix_len} {line}")

    def debug(self, message: str, *args: Any) -> None:
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", _format_log_message(message, args))

    def info(self, message: str, *args: Any) -> None:
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", _format_log_message(message, args))

    def warning(self, message: str, *args: Any) -> None:
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", _format_log_message(message, args))

    def error(self, message: str, *args: Any) -> None:
        self._print("[ERROR]", _format_log_message(message, args))

    def verbose(
        self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"], message: str
    ) -> None:
        if self.verbose_on:
            match log_level:
                case "DEBUG":
                    self.debug("[VERBOSE] " + message)
                case "INFO":
                    self.info("[VERBOSE] " + message)
                case "WARNING":
                    self.warning("[VERBOSE] " + message)
                case "ERROR":
                    self.error("[VERBOSE] " + message)
