from typing import Literal
import os
import inspect
from datetime import datetime


class CogniteFunctionLogger:
    def __init__(
        self,
        log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
        write: bool = False,
        filepath: str | None = None,
    ):
        self.log_level = log_level.upper()
        self.write = write
        self.filepath = filepath
        self.file_handler = None

        if self.filepath and self.write:
            try:
                dir_name = os.path.dirname(self.filepath)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                self.file_handler = open(self.filepath, "a", encoding="utf-8")
            except Exception as e:
                print(f"[LOGGER_SETUP_ERROR] Could not open log file {self.filepath}: {e}")
                self.write = False

    def _get_timestamp(self) -> str:
        return datetime.utcnow().isoformat(sep=" ", timespec="milliseconds")

    def _format_message_lines(self, prefix: str, message: str) -> list[str]:
        """
        Formats multi-line messages with consistent indentation.
        Args:
            prefix: The log level prefix (e.g., "[INFO]", "[ERROR]").
            message: The message to format.

        Returns:
            List of formatted message lines with proper indentation.
        """
        timestamp = self._get_timestamp()
        formatted_prefix = f"[{timestamp}] {prefix}"

        formatted_lines = []
        if "\n" not in message:
            formatted_lines.append(f"{formatted_prefix} {message}")
        else:
            lines = message.split("\n")
            formatted_lines.append(f"{formatted_prefix} {lines[0]}")
            padding = " " * len(formatted_prefix)
            for line_content in lines[1:]:
                formatted_lines.append(f"{padding} {line_content}")
        return formatted_lines

    def _print(self, prefix: str, message: str) -> None:
        """
        Prints formatted log messages to console and optionally to file.

        Args:
            prefix: The log level prefix to prepend to the message.
            message: The message to log.

        Returns:
            None
        """
        lines_to_log = self._format_message_lines(prefix, message)
        if self.write and self.file_handler:
            try:
                for line in lines_to_log:
                    print(line)
                    self.file_handler.write(line + "\n")
                self.file_handler.flush()
            except Exception as e:
                print(f"[LOGGER_SETUP_ERROR] Could not write to {self.filepath}: {e}")
        elif not self.write:
            for line in lines_to_log:
                print(line)

    def debug(self, message: str, section: Literal["START", "END", "BOTH"] | None = None) -> None:
        """
        Logs a debug-level message.

        Args:
            message: The debug message to log.
            section: Optional section separator position (START, END, or BOTH).

        Returns:
            None
        """
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def info(self, message: str, section: Literal["START", "END", "BOTH"] | None = None) -> None:
        """
        Logs an info-level message.

        Args:
            message: The informational message to log.
            section: Optional section separator position (START, END, or BOTH).

        Returns:
            None
        """
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def warning(self, message: str, section: Literal["START", "END", "BOTH"] | None = None) -> None:
        """
        Logs a warning-level message.

        Args:
            message: The warning message to log.
            section: Optional section separator position (START, END, or BOTH).

        Returns:
            None
        """
        if section == "START" or section == "BOTH":
            self._section()
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)
        if section == "END" or section == "BOTH":
            self._section()

    def error(
        self, message: str, error: Exception | None = None, section: Literal["START", "END", "BOTH"] | None = None
    ) -> None:
        """
        Logs an error-level message.

        Args:
            message: The error message to log.
            section: Optional section separator position (START, END, or BOTH).

        Returns:
            None
        """
        if section == "START" or section == "BOTH":
            self._section()

        # Get caller information
        stack = inspect.stack()
        # Stack[0] is this function, [1] is caller usually.
        # If wrapped, we might need to look further, but assuming direct call:
        caller_frame = stack[1]
        filename = os.path.basename(caller_frame.filename)
        line_number = caller_frame.lineno
        function_name = caller_frame.function

        context_info = f"\nError occurred in {filename} on line {line_number} in method '{function_name}'"

        error_info = ""
        if error:
            error_info = f"\nError Type: {type(error).__name__}\nError Message: {str(error)}"

        full_message = f"{message}{context_info}{error_info}"

        self._print("[ERROR]", full_message)

        if section == "END" or section == "BOTH":
            self._section()

    def _section(self) -> None:
        """
        Prints a visual separator line for log sections.

        Returns:
            None
        """
        separator = "--------------------------------------------------------------------------------"

        if self.write and self.file_handler:
            self.file_handler.write(f"{separator}\n")
        print(separator)

    def close(self) -> None:
        """
        Closes the file handler if file logging is enabled.

        Returns:
            None
        """
        if self.file_handler:
            try:
                self.file_handler.close()
            except Exception as e:
                print(f"[LOGGER_CLEANUP_ERROR] Error closing log file: {e}")
            self.file_handler = None
