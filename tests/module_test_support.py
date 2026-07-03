"""Helpers for CDF function tests that use flat module imports (handler, pipeline, …)."""

import importlib.util
import sys
from pathlib import Path

# Flat module basenames reused across deployed function directories.
_FLAT_MODULE_NAMES = (
    "pipeline",
    "handler",
    "config",
    "constants",
    "logger",
    "metadata_optimizations",
    "pipeline_optimizations",
)

_MODULE_TEST_PATH_MARKERS = (
    "modules/contextualization/",
    "modules/sourcesystem/cdf_oid_sync/",
)


def cognite_sdk_available() -> bool:
    """Return True when cognite-sdk is importable."""
    return importlib.util.find_spec("cognite.client") is not None


def is_module_function_test_file(path: Path) -> bool:
    """Return True for module test files that need cognite-sdk at collection time."""
    path_str = path.as_posix()
    if not any(marker in path_str for marker in _MODULE_TEST_PATH_MARKERS):
        return False
    return path.is_file() and path.name.startswith("test_") and path.suffix == ".py"


def function_dir_for_test_file(path: Path) -> Path | None:
    """Return the deployed function directory for a module test file."""
    if not is_module_function_test_file(path):
        return None
    if path.name == "test_oid_sync.py":
        return path.parent / "functions" / "fn_oid_sync"
    return path.parent


def isolate_function_directory(function_dir: Path) -> None:
    """Prefer imports from *function_dir* and drop stale flat-named modules."""
    resolved = function_dir.resolve()
    resolved_str = str(resolved)
    while resolved_str in sys.path:
        sys.path.remove(resolved_str)
    sys.path.insert(0, resolved_str)

    for name in _FLAT_MODULE_NAMES:
        module = sys.modules.get(name)
        if module is None:
            continue
        module_file = getattr(module, "__file__", None)
        if not module_file:
            del sys.modules[name]
            continue
        try:
            loaded_from = Path(module_file).resolve().parent
        except OSError:
            del sys.modules[name]
            continue
        if loaded_from != resolved:
            del sys.modules[name]
