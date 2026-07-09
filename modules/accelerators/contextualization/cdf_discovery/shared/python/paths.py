"""Central path resolution for cdf_discovery submodules."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[2]


def module_root() -> Path:
    env = os.environ.get("CDF_DISCOVERY_ROOT")
    if env:
        return Path(env).resolve()
    return _MODULE_ROOT


def submodules_root() -> Path:
    return module_root() / "submodules"


def governance_root() -> Path:
    env = os.environ.get("CDF_DISCOVERY_GOVERNANCE_ROOT")
    if env:
        return Path(env).resolve()
    return submodules_root() / "governance"


def transform_root() -> Path:
    return submodules_root() / "transform"


def inverted_index_root() -> Path:
    env = os.environ.get("CDF_INVERTED_INDEX_ROOT")
    if env:
        return Path(env).resolve()
    return submodules_root() / "inverted_index"


def etl_functions_root() -> Path:
    return transform_root() / "functions"


def idx_functions_root() -> Path:
    return inverted_index_root() / "functions"


def cdf_fn_common_root() -> Path:
    return module_root() / "shared" / "cdf_fn_common"


def shared_python_root() -> Path:
    return module_root() / "shared" / "python"


def ui_root() -> Path:
    return module_root() / "ui"


def workflows_root() -> Path:
    return module_root() / "workflows"


def data_sets_root() -> Path:
    return module_root() / "data_sets"


def prepare_sys_path() -> list[str]:
    root = module_root()
    candidates = [
        str(root),
        str(shared_python_root()),
        str(module_root() / "shared"),
        str(submodules_root()),
        str(transform_root()),
        str(transform_root() / "scripts"),
        str(etl_functions_root()),
        str(inverted_index_root()),
        str(idx_functions_root()),
        str(root / "scripts"),
    ]
    return [p for p in candidates if Path(p).exists()]


def ensure_sys_path() -> None:
    for p in prepare_sys_path():
        while p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
