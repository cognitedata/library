"""Which part of the measurements a run is for: everything, some dimensions, or only the configured metrics.

A run measures *dimensions* (the cards of the dashboard) and, when a config file was given, the *configured* links.
`scope` in the payload of `plan` says which of them this run is for; the plan hands it to every piece, and `finalize`
writes it into the run's `complete` marker so a consumer can tell which dimensions a run measured.

    absent                                          everything, plus the configured links if there are any (as before)
    "all"                                           every dimension, no configured links (what the schedule runs)
    "configured"                                    only the configured links
    {"dimensions": ["files", ...] | "all", "configured": true | false}

A dimension is measured whole: every metric of it, from the kinds of data it needs. Two dimensions need a second kind
of data besides their own (the assets): time series for its monitoring coverage, 3D model for its asset coverage.
"""

from dataclasses import dataclass
from typing import Any

# The dimensions in the order of the dashboard, and the kinds of data (cq.metrics.KINDS) each one needs.
DIMENSION_KINDS: dict[str, frozenset[str]] = {
    "asset-hierarchy": frozenset({"assets"}),
    "equipment-asset": frozenset({"equipment"}),
    "time-series": frozenset({"timeseries", "assets"}),
    "maintenance": frozenset({"work_orders", "notifications"}),
    "files": frozenset({"files"}),
    "model-3d": frozenset({"objects3d", "assets"}),
    "file-annotation": frozenset({"annotations"}),
}
DIMENSIONS = tuple(DIMENSION_KINDS)
CONFIGURED = "configured"  # the configured links are not a dimension; this names them where a dimension is named


@dataclass(frozen=True)
class Scope:
    dimensions: frozenset[str]
    configured: bool

    @property
    def partial(self) -> bool:
        """Whether some dimension is left out, so the kinds of data of the others are all that is read."""
        return self.dimensions != frozenset(DIMENSIONS)

    @property
    def kinds(self) -> frozenset[str] | None:
        """The kinds of data to read and count, or None for all of them."""
        if not self.partial:
            return None
        return frozenset().union(*(DIMENSION_KINDS[dimension] for dimension in self.dimensions))

    @property
    def measured(self) -> frozenset[str]:
        """What the run measures, for the dimensions and the configured links alike."""
        return self.dimensions | ({CONFIGURED} if self.configured else frozenset())

    def label(self) -> str:
        """The scope as written into a run's markers: `all`, `all+configured`, `files,time-series`, `configured`..."""
        base = "all" if not self.partial else ",".join(d for d in DIMENSIONS if d in self.dimensions)
        if self.configured:
            return f"{base}+configured" if base else CONFIGURED
        return base

    def to_payload(self) -> dict[str, Any]:
        return {"dimensions": [d for d in DIMENSIONS if d in self.dimensions], "configured": self.configured}


def parse_scope(raw: Any, links_given: bool | None = None) -> Scope:
    """The scope a payload asks for. `links_given` says whether the payload carries configured links; None skips the
    check (the steps after the plan do not have the links). Raises ValueError for a scope that cannot be run."""
    everything = frozenset(DIMENSIONS)
    if raw is None or raw == "" or (isinstance(raw, str) and raw.startswith("${")):
        # no scope (or a workflow reference that was not filled in): what a run was before scopes existed
        return Scope(everything, bool(links_given))
    if raw == "all":
        return Scope(everything, False)
    if raw == CONFIGURED:
        scope = Scope(frozenset(), True)
    elif isinstance(raw, dict):
        unknown_keys = set(raw) - {"dimensions", "configured"}
        if unknown_keys:
            raise ValueError(f"Unknown scope keys {sorted(unknown_keys)}. Expected dimensions and configured")
        dimensions = raw.get("dimensions", "all")
        if dimensions == "all":
            chosen = everything
        elif isinstance(dimensions, list) and all(isinstance(d, str) for d in dimensions):
            unknown = sorted(set(dimensions) - everything)
            if unknown:
                raise ValueError(f"Unknown dimensions {unknown}. Expected some of {list(DIMENSIONS)}")
            chosen = frozenset(dimensions)
        else:
            raise ValueError(f'scope.dimensions must be "all" or a list of dimensions, not {dimensions!r}')
        configured = raw.get("configured", False)
        if not isinstance(configured, bool):
            raise ValueError(f"scope.configured must be true or false, not {configured!r}")
        scope = Scope(chosen, configured)
    else:
        raise ValueError(f'scope must be "all", "configured" or an object, not {raw!r}')
    if not scope.dimensions and not scope.configured:
        raise ValueError("The scope selects nothing: name at least one dimension or the configured metrics")
    if scope.configured and links_given is False:
        raise ValueError("The configured metrics need a config file: extraLinks is empty")
    return scope


def filter_result(result: dict[str, Any], scope: Scope) -> dict[str, Any]:
    """`cq.metrics.compute`'s result without what belongs to a dimension the scope leaves out.

    A call counts the kinds of data its dimensions need, and a kind can feed a dimension that is not in scope (the assets
    of a time series run also yield the asset hierarchy rows); those are dropped here, before anything is written.
    """
    if not scope.partial:
        return result
    dimensions = scope.dimensions
    return {
        **result,
        "agg": {key: value for key, value in result["agg"].items() if key[1] in dimensions},
        "exclusions": {key: value for key, value in result["exclusions"].items() if key[0] in dimensions},
        "drilldown": [item for item in result["drilldown"] if item["dim"] in dimensions],
        "classifications": {key: value for key, value in result["classifications"].items() if key[3] in dimensions},
    }
