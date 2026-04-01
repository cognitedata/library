"""Tests for ``cdf_fn_common.function_logging`` (CDF-safe logger construction)."""

from __future__ import annotations

import logging
import unittest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
    cognite_function_logger,
    function_logger_from_data,
    resolve_function_logger,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.logger import (
    CogniteFunctionLogger,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.dependencies import (
    create_logger_service as ke_create_logger_service,
)


class TestCogniteFunctionLoggerFactory(unittest.TestCase):
    def test_default_invalid_level_maps_to_info_verbose_preserved(self) -> None:
        log = cognite_function_logger("not-a-level", True)
        self.assertEqual(log.log_level, "INFO")
        self.assertTrue(log.verbose_on)

    def test_strict_invalid_level_uses_defaults_and_drops_verbose(self) -> None:
        log = cognite_function_logger("not-a-level", True, strict_level_names=True)
        self.assertEqual(log.log_level, "INFO")
        self.assertFalse(log.verbose_on)

    def test_strict_case_sensitive_unknown_drops_verbose(self) -> None:
        log = cognite_function_logger("info", True, strict_level_names=True)
        self.assertEqual(log.log_level, "INFO")
        self.assertFalse(log.verbose_on)

    def test_strict_valid_level_keeps_verbose(self) -> None:
        log = cognite_function_logger("DEBUG", True, strict_level_names=True)
        self.assertEqual(log.log_level, "DEBUG")
        self.assertTrue(log.verbose_on)

    def test_function_logger_from_data_lowercase_level_normalized(self) -> None:
        log = function_logger_from_data({"logLevel": "debug", "verbose": False})
        self.assertEqual(log.log_level, "DEBUG")

    def test_key_extraction_dependencies_matches_strict(self) -> None:
        a = ke_create_logger_service("INVALID", True)
        b = cognite_function_logger("INVALID", True, strict_level_names=True)
        self.assertEqual(a.log_level, b.log_level)
        self.assertEqual(a.verbose_on, b.verbose_on)


class TestResolveFunctionLogger(unittest.TestCase):
    def test_none_uses_data(self) -> None:
        log = resolve_function_logger({"logLevel": "WARNING", "verbose": True}, None)
        self.assertIsInstance(log, CogniteFunctionLogger)
        self.assertEqual(log.log_level, "WARNING")
        self.assertTrue(log.verbose_on)

    def test_strict_key_extraction_path(self) -> None:
        log = resolve_function_logger(
            {"logLevel": "BOGUS", "verbose": True}, None, strict_level_names=True
        )
        self.assertIsInstance(log, CogniteFunctionLogger)
        self.assertFalse(log.verbose_on)

    def test_stdlib_logger_wrapped(self) -> None:
        base = logging.getLogger("test_function_logging_stdlib")
        base.setLevel(logging.DEBUG)
        log = resolve_function_logger({}, base)
        self.assertIsInstance(log, StdlibLoggerAdapter)
        self.assertTrue(callable(getattr(log, "verbose", None)))

    def test_injected_cdf_logger_passthrough(self) -> None:
        inner = CogniteFunctionLogger("INFO", verbose=True)
        log = resolve_function_logger({}, inner)
        self.assertIs(log, inner)


if __name__ == "__main__":
    unittest.main()
