"""Fan-out profile registry."""

from __future__ import annotations

from typing import Any, Dict, Protocol

from cdf_fn_common.etl_fanout_plan.profiles.file_annotation import FileAnnotationFanoutProfile


class FanoutProfile(Protocol):
    name: str

    def required_handles(self, cfg: Dict[str, Any]) -> Dict[str, bool]: ...

    def build_tasks(
        self,
        *,
        client: Any,
        data: Dict[str, Any],
        cfg: Dict[str, Any],
        params: Dict[str, Any],
        log: Any,
    ) -> Dict[str, Any]: ...


_PROFILES: Dict[str, FanoutProfile] = {
    "file_annotation": FileAnnotationFanoutProfile(),
}


def get_fanout_profile(name: str) -> FanoutProfile:
    key = str(name or "file_annotation").strip().lower() or "file_annotation"
    profile = _PROFILES.get(key)
    if profile is None:
        raise ValueError(
            f"Unknown fanout_profile {key!r}; supported: {sorted(_PROFILES.keys())}"
        )
    return profile
