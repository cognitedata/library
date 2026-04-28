"""Deploy KEA Cognite Functions from ``functions/functions.Function.yaml`` via the Cognite SDK.

Each function is packaged from a **staging directory** containing ``cdf_fn_common/``, optional
``common/``, and the function folder (``fn_dm_*`` / ``fn_dm_incremental_state_update``) so
imports match production layout. If ``<externalId>/requirements.txt`` exists, it is also copied to
the staging **root** (CDF requires ``requirements.txt`` at the archive root). Used by
``deploy_scope_cdf.py`` before workflow upsert.

``metadata.kea_source_hash`` records a content hash of shared + function code; ``if-stale``
redeploys when the hash differs or is absent (legacy functions).
"""

from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Literal, TextIO, cast

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes.functions import RunTime

from cdf_workflow_io import shallow_has_toolkit_placeholder

DeployFunctionsMode = Literal["never", "if-missing", "if-stale", "always"]

_IGNORE_COPY = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache", ".mypy_cache")


def _yaml_function_specs(functions_yaml: Path) -> list[dict[str, Any]]:
    raw = yaml.safe_load(functions_yaml.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Expected YAML list in {functions_yaml}")
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            out.append(item)
    return out


def _functions_yaml_path(module_root: Path) -> Path:
    return module_root / "functions" / "functions.Function.yaml"


def _resolve_data_set_id(client: CogniteClient, data_set_external_id: str | None) -> int | None:
    if data_set_external_id is None:
        return None
    ext = str(data_set_external_id).strip().strip("'\"")
    if not ext:
        return None
    ds = client.data_sets.retrieve(external_id=ext)
    if ds is None:
        raise RuntimeError(
            f"Data set not found for external_id={ext!r}. Create it in CDF or deploy datasets first."
        )
    return int(ds.id)


def _sanitize_metadata(meta: Any) -> dict[str, str] | None:
    if not isinstance(meta, dict) or not meta:
        return None
    cleaned: dict[str, str] = {}
    for k, v in meta.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        if shallow_has_toolkit_placeholder(v):
            continue
        cleaned[k] = v
    return cleaned or None


def _iter_hashed_files(paths: list[Path]) -> list[tuple[str, int, int]]:
    """Sorted (relative posix path, size, mtime_ns) for deterministic hashing."""
    entries: list[tuple[str, int, int]] = []
    for root in paths:
        if not root.is_dir():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(root).as_posix()
            if "__pycache__" in rel or rel.endswith(".pyc"):
                continue
            st = p.stat()
            entries.append((f"{root.name}:{rel}", st.st_size, int(st.st_mtime_ns)))
    entries.sort(key=lambda x: x[0])
    return entries


def kea_function_source_hash(functions_root: Path, external_id: str) -> str:
    """Hash ``cdf_fn_common``, ``common`` (if present), and ``<external_id>/`` under ``functions_root``."""
    parts: list[Path] = []
    cfc = functions_root / "cdf_fn_common"
    if cfc.is_dir():
        parts.append(cfc)
    com = functions_root / "common"
    if com.is_dir():
        parts.append(com)
    fn_dir = functions_root / external_id
    if not fn_dir.is_dir():
        raise FileNotFoundError(f"Function folder not found: {fn_dir}")
    parts.append(fn_dir)
    h = hashlib.sha256()
    for path, size, mtime_ns in _iter_hashed_files(parts):
        h.update(path.encode())
        h.update(str(size).encode())
        h.update(str(mtime_ns).encode())
        h.update(b"\0")
    # Bumped when staging layout changes (e.g. requirements.txt only at zip root for CDF).
    h.update(b"kea_staging_layout:v3_absolute_cdf_fn_common_imports\n")
    return h.hexdigest()


def _materialize_staging(functions_root: Path, external_id: str, stage: Path) -> None:
    stage.mkdir(parents=True, exist_ok=True)
    shutil.copytree(functions_root / "cdf_fn_common", stage / "cdf_fn_common", ignore=_IGNORE_COPY)
    com = functions_root / "common"
    if com.is_dir():
        shutil.copytree(com, stage / "common", ignore=_IGNORE_COPY)
    shutil.copytree(functions_root / external_id, stage / external_id, ignore=_IGNORE_COPY)
    # CDF requires a single requirements.txt at the archive root (no copy under subfolders).
    req_src = functions_root / external_id / "requirements.txt"
    if req_src.is_file():
        shutil.copyfile(req_src, stage / "requirements.txt")
        staged_fn_req = stage / external_id / "requirements.txt"
        if staged_fn_req.is_file():
            staged_fn_req.unlink()


def _coerce_runtime(value: Any) -> RunTime | None:
    if value is None or value == "":
        return None
    s = str(value).strip().strip("'\"")
    if s in ("py310", "py311", "py312"):
        return cast(RunTime, s)
    return None


def _coerce_env_vars(raw: Any) -> dict[str, str] | None:
    if not isinstance(raw, dict) or not raw:
        return None
    out: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out or None


def deploy_kea_functions(
    client: CogniteClient | None,
    module_root: Path,
    *,
    mode: DeployFunctionsMode = "if-stale",
    dry_run: bool = False,
    log: TextIO | None = None,
) -> None:
    """Create or refresh KEA functions declared in ``functions/functions.Function.yaml``.

    Args:
        client: Cognite client (required unless ``dry_run``).
        module_root: Path to the ``cdf_key_extraction_aliasing`` module root.
        mode: ``never`` | ``if-missing`` | ``if-stale`` | ``always`` (see module docstring).
        dry_run: Print planned actions only.
        log: stderr-like sink (default ``sys.stderr``).
    """
    sink = log or sys.stderr
    functions_root = module_root / "functions"
    yaml_path = _functions_yaml_path(module_root)
    if not yaml_path.is_file():
        raise FileNotFoundError(f"Missing {yaml_path}")
    if not (functions_root / "cdf_fn_common").is_dir():
        raise FileNotFoundError(f"Missing shared code: {functions_root / 'cdf_fn_common'}")

    specs = _yaml_function_specs(yaml_path)
    if mode == "never":
        print("[functions] mode=never — skipping Cognite Function deploy.", file=sink)
        return

    for spec in specs:
        ext_id = spec.get("externalId")
        if not ext_id or not str(ext_id).strip():
            continue
        ext_id = str(ext_id).strip().strip("'\"")
        name = str(spec.get("name") or ext_id).strip() or ext_id
        fp = str(spec.get("functionPath") or "handler.py").strip() or "handler.py"
        function_rel = f"{ext_id}/{fp}".replace("\\", "/")
        ds_ext = spec.get("dataSetExternalId")
        runtime = _coerce_runtime(spec.get("runtime"))
        env_vars = _coerce_env_vars(spec.get("envVars"))
        base_meta = _sanitize_metadata(spec.get("metadata"))

        local_hash = kea_function_source_hash(functions_root, ext_id)
        meta: dict[str, str] | None = None
        if base_meta:
            meta = {**base_meta, "kea_source_hash": local_hash}
        else:
            meta = {"kea_source_hash": local_hash}

        if dry_run or client is None:
            print(
                f"[functions] dry-run: would ensure function externalId={ext_id!r} "
                f"name={name!r} functionPath={function_rel!r} mode={mode!r}",
                file=sink,
            )
            continue

        cc = cast(CogniteClient, client)
        data_set_id = _resolve_data_set_id(cc, str(ds_ext) if ds_ext is not None else None)
        existing = cc.functions.retrieve(external_id=ext_id)

        do_deploy = False
        if mode == "always":
            do_deploy = True
        elif mode == "if-missing":
            do_deploy = existing is None
        elif mode == "if-stale":
            if existing is None:
                do_deploy = True
            else:
                remote_hash = (existing.metadata or {}).get("kea_source_hash") if existing.metadata else None
                if remote_hash is None or str(remote_hash) != local_hash:
                    do_deploy = True
        else:
            raise ValueError(f"Unknown deploy functions mode: {mode!r}")

        if not do_deploy:
            print(f"[functions] up-to-date externalId={ext_id!r} — skip.", file=sink)
            continue

        if existing is not None:
            print(f"[functions] removing existing externalId={ext_id!r} for redeploy.", file=sink)
            cc.functions.delete(external_id=ext_id)

        with tempfile.TemporaryDirectory(prefix="kea_fn_stage_") as tmp:
            stage_path = Path(tmp)
            _materialize_staging(functions_root, ext_id, stage_path)
            print(
                f"[functions] deploying externalId={ext_id!r} from staging ({function_rel}) …",
                file=sink,
            )
            raw_desc = spec.get("description")
            description = str(raw_desc).strip() if raw_desc not in (None, "") else None
            raw_owner = spec.get("owner")
            owner = str(raw_owner).strip() if raw_owner not in (None, "") else None
            cc.functions.create(
                name=name,
                folder=str(stage_path),
                function_path=function_rel,
                external_id=ext_id,
                description=description,
                owner=owner,
                env_vars=env_vars,
                runtime=runtime,
                metadata=meta,
                skip_folder_validation=True,
                data_set_id=data_set_id,
            )
        print(f"[functions] deployed externalId={ext_id!r}.", file=sink)
