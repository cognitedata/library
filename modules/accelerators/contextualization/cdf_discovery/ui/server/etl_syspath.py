"""Ensure ETL ``transform/`` packages win over the module-root ``local_runner`` stub."""

from __future__ import annotations

import sys
from pathlib import Path


def transform_root(module_root: Path) -> Path:
    return module_root / "transform"


def ensure_transform_syspath(module_root: Path) -> Path:
    """
    Prepend ``functions/``, ``transform/``, and ``transform/scripts`` so ``local_runner``
    resolves to ``transform/local_runner`` (full runner), not ``cdf_discovery/local_runner``
    (client-only stub), and ``workflow_build`` is importable for UI compile/build.

    Paths are moved to the front even when already on ``sys.path`` (e.g. from ``PYTHONPATH``),
    because the script directory is always ``cdf_discovery/`` and would otherwise win first.
    """
    tr = transform_root(module_root)
    root = module_root.resolve()
    for p in (str(root / "functions"), str(tr), str(tr / "scripts")):
        while p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    return tr


def invalidate_local_runner_if_not_transform(transform: Path) -> None:
    """Drop cached ``local_runner`` when it was imported from the module-root stub."""
    expected = (transform / "local_runner").resolve()
    lr = sys.modules.get("local_runner")
    if lr is None:
        return
    mod_file = getattr(lr, "__file__", None)
    if not mod_file:
        return
    try:
        loaded_dir = Path(mod_file).resolve().parent
    except OSError:
        return
    if loaded_dir == expected:
        return
    for key in list(sys.modules):
        if key == "local_runner" or key.startswith("local_runner."):
            del sys.modules[key]


def prepare_etl_local_runner(module_root: Path) -> Path:
    """Call before any ``from local_runner.run import ...`` in the UI server."""
    tr = ensure_transform_syspath(module_root)
    invalidate_local_runner_if_not_transform(tr)
    return tr
