"""Resolve whether local/deployed ETL tasks use in-memory or RAW cohort predecessors."""

from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping

from cdf_fn_common.etl_common import _as_dict, _first_nonempty

MODE_IN_MEMORY = "in_memory"
MODE_COHORT = "cohort"
_MODES = frozenset({MODE_IN_MEMORY, MODE_COHORT})


def _normalize_mode(raw: Any) -> str | None:
    s = str(raw or "").strip().lower().replace("-", "_")
    if s in ("memory", "inmem", "in_mem"):
        return MODE_IN_MEMORY
    if s in _MODES:
        return s
    if s in ("raw", "cohort_raw"):
        return MODE_COHORT
    return None


def resolve_local_predecessor_mode(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> str:
    """
    Precedence: explicit ``data.local_predecessor_mode`` → pipeline ``parameters`` →
    env ``ETL_LOCAL_PREDECESSOR_MODE`` → task config flags → env ``ETL_TRANSFORM_IN_MEMORY``.
    Default: cohort (deployed workflow parity). Use ``ETL_LOCAL_PREDECESSOR_MODE=in_memory`` or
    ``ETL_TRANSFORM_IN_MEMORY=1`` for in-memory handoff.
    """
    explicit = _normalize_mode(data.get("local_predecessor_mode"))
    if explicit:
        return explicit

    configuration = _as_dict(data.get("configuration"))
    params = _as_dict(configuration.get("parameters"))
    from_params = _normalize_mode(params.get("local_predecessor_mode"))
    if from_params:
        return from_params

    from_env = _normalize_mode(os.environ.get("ETL_LOCAL_PREDECESSOR_MODE"))
    if from_env:
        return from_env

    task_cfg = _as_dict(cfg) if cfg is not None else _as_dict(data.get("config"))
    if task_cfg.get("_use_cohort_predecessors") is True:
        return MODE_COHORT
    if task_cfg.get("_use_in_memory_predecessors") is True:
        return MODE_IN_MEMORY
    if task_cfg.get("_use_in_memory_predecessors") is False:
        return MODE_COHORT

    in_mem_env = os.environ.get("ETL_TRANSFORM_IN_MEMORY", "").strip().lower()
    if in_mem_env in ("0", "false", "no"):
        return MODE_COHORT
    if in_mem_env in ("1", "true", "yes"):
        return MODE_IN_MEMORY

    if data.get("_predecessor_rows") is not None and not _as_dict(data.get("persistence")):
        return MODE_IN_MEMORY

    return MODE_COHORT


def use_in_memory_predecessors(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> bool:
    return resolve_local_predecessor_mode(data, cfg) == MODE_IN_MEMORY


def seed_predecessor_mode(
    shared: MutableMapping[str, Any],
    mode: str,
) -> None:
    normalized = _normalize_mode(mode)
    if normalized not in _MODES:
        raise ValueError(f"local_predecessor_mode must be one of {_MODES}, got {mode!r}")
    shared["local_predecessor_mode"] = normalized
