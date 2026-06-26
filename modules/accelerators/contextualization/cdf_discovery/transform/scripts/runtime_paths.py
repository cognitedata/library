from __future__ import annotations

import sys
from pathlib import Path


def discovery_root_from_path(start_file: str) -> Path:
    start = Path(start_file).resolve()
    for parent in (start.parent, *start.parents):
        if (parent / "functions").is_dir() and (parent / "transform").is_dir():
            return parent
    raise RuntimeError(f"Could not resolve discovery root from: {start}")


def transform_root_from_path(start_file: str) -> Path:
    root = discovery_root_from_path(start_file)
    transform_root = root / "transform"
    if transform_root.is_dir():
        return transform_root
    raise RuntimeError(f"Missing transform directory under: {root}")


def _drop_stale_transform_functions_path(transform_root: Path) -> None:
    stale_functions = (transform_root / "functions").resolve()
    kept: list[str] = []
    for entry in sys.path:
        if not entry:
            kept.append(entry)
            continue
        try:
            if Path(entry).resolve() == stale_functions:
                continue
        except OSError:
            if entry.replace("\\", "/").endswith("transform/functions"):
                continue
        kept.append(entry)
    sys.path[:] = kept


def ensure_import_paths(start_file: str, *, include_discovery_root: bool = False) -> Path:
    discovery_root = discovery_root_from_path(start_file)
    transform_root = transform_root_from_path(start_file)
    scripts_root = transform_root / "scripts"
    functions_root = discovery_root / "functions"

    _drop_stale_transform_functions_path(transform_root)
    inserts = [str(scripts_root), str(transform_root), str(functions_root)]
    if include_discovery_root:
        inserts.append(str(discovery_root))

    for p in inserts:
        while p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    return discovery_root
