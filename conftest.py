"""Repository-root pytest hooks (loaded for all test paths under this repo)."""

from pathlib import Path

from _pytest.python import Module

from tests.module_test_support import (
    cognite_sdk_available,
    function_dir_for_test_file,
    is_module_function_test_file,
    isolate_function_directory,
)

_original_module_getobj = Module._getobj


def _isolated_module_getobj(self: Module):
    function_dir = function_dir_for_test_file(Path(self.path))
    if function_dir is not None:
        isolate_function_directory(function_dir)
    return _original_module_getobj(self)


def pytest_configure(config):
    """Patch test-module import so flat function imports resolve per directory."""
    if getattr(config, "_function_import_isolation_patch", False):
        return
    config._function_import_isolation_patch = True
    Module._getobj = _isolated_module_getobj


def pytest_ignore_collect(collection_path, config):
    """Skip module function tests when cognite-sdk is not installed."""
    if cognite_sdk_available():
        return False
    return is_module_function_test_file(Path(str(collection_path)))
