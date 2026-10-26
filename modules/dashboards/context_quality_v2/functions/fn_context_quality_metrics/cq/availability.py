"""Which metrics could not be measured, and what the run should warn about.

A metric row is either *measured* (eligibleCount > 0; a measured zero is passingCount 0 out of eligible entities) or
*not applicable* (eligibleCount 0 and notApplicable true): the metric has a source population in the reporting unit but
nothing eligible to count. Without registration a metric with nothing eligible writes no row at all, which is
indistinguishable from "never looked at it" or from "nothing wrong".

Registration is deliberately narrow. A metric is registered for a reporting unit only when the entities it is computed
from exist in that unit, so a unit with no maintenance data gets no maintenance rows (the dimension is simply absent
there), and the number of rows added is bounded by metrics x units, never metrics x slices.
"""

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from cq.extra_links import LinkGap
from cq.types import LoadedData, RunConfig

MAX_LISTED_CODES = 10
MAX_WARNINGS_LENGTH = 4000

TIER_MAPPING = "tier-mapping"

# The populations a metric is computed from: the loaded entity list that must contain something in the unit.
POPULATIONS = {
    "assets": "assets",
    "equipment": "equipment",
    "timeseries": "timeseries",
    "work_orders": "work_orders",
    "notifications": "notifications",
    "files": "files",
    "annotations": "annotations",
    "objects3d": "objects3d",
}
# Populations that come from an optional source view, and the view that provides them.
OPTIONAL_SOURCE_OF = {"work_orders": "cdf_idm.CogniteMaintenanceOrder", "notifications": "cdf_idm.CogniteNotification"}
# Populations whose exclusions are counted in their own dimension, so "everything was excluded" can be recognised.
EXCLUSION_DIMENSION = {"equipment": "equipment-asset", "timeseries": "time-series", "files": "files"}


@dataclass(frozen=True)
class MetricSpec:
    metric: str
    dimension: str
    population: str
    sub_key: str | None = None
    requires: str | None = None


# Every aggregate metric the function writes, except `activeChildrenUnderRetiredParents`: it only counts violations
# (it has no population of its own to be "not applicable" for), so no rows means no violations were found.
METRIC_SPECS: tuple[MetricSpec, ...] = (
    MetricSpec("hierarchyCompletionRate", "asset-hierarchy", "assets"),
    MetricSpec("orphanRate", "asset-hierarchy", "assets"),
    MetricSpec("associationRate", "equipment-asset", "equipment"),
    MetricSpec("serialMfrCompleteness", "equipment-asset", "equipment", "serialNumber"),
    MetricSpec("serialMfrCompleteness", "equipment-asset", "equipment", "manufacturer"),
    MetricSpec("tsToAssetRate", "time-series", "timeseries"),
    MetricSpec("freshness", "time-series", "timeseries"),
    MetricSpec("monitoringCoverage", "time-series", "assets", requires=TIER_MAPPING),
    MetricSpec("woToAssetRate", "maintenance", "work_orders"),
    MetricSpec("woToNotificationRate", "maintenance", "work_orders"),
    MetricSpec("woCompletionRate", "maintenance", "work_orders"),
    MetricSpec("notificationToAssetRate", "maintenance", "notifications"),
    MetricSpec("notificationToWoRate", "maintenance", "notifications"),
    MetricSpec("fileToAssetRate", "files", "files"),
    MetricSpec("metadataCompleteness", "files", "files", "category"),
    MetricSpec("metadataCompleteness", "files", "files", "description"),
    MetricSpec("metadataCompleteness", "files", "files", "sourceId"),
    MetricSpec("threeDToAssetRate", "model-3d", "objects3d"),
    MetricSpec("bboxCompleteness", "model-3d", "objects3d"),
    MetricSpec("asset3dCoverage", "model-3d", "assets", requires=TIER_MAPPING),
    MetricSpec("annotationCount", "file-annotation", "annotations"),
    MetricSpec("confidenceAverage", "file-annotation", "annotations"),
    MetricSpec("approvalMix", "file-annotation", "annotations", "Approved"),
    MetricSpec("approvalMix", "file-annotation", "annotations", "Suggested"),
    MetricSpec("approvalMix", "file-annotation", "annotations", "Rejected"),
    MetricSpec("lowConfidenceQueue", "file-annotation", "annotations"),
    MetricSpec("pendingReview", "file-annotation", "annotations"),
)


@dataclass(frozen=True)
class NotApplicable:
    spec: MetricSpec
    unit: str
    reason: str

    @property
    def subject(self) -> str:
        return self.spec.metric if self.spec.sub_key is None else f"{self.spec.metric}/{self.spec.sub_key}"


def population_counts(data: LoadedData) -> dict[str, dict[str, int]]:
    """population -> {reporting unit: number of entities}, for every facility space."""
    units = list(data["facility_spaces"])
    counts: dict[str, dict[str, int]] = {name: dict.fromkeys(units, 0) for name in POPULATIONS}
    for name, key in POPULATIONS.items():
        for entity in data[key]:  # type: ignore[literal-required]
            if entity["space"] in counts[name]:
                counts[name][entity["space"]] += 1
    return counts


def _reason(
    spec: MetricSpec,
    unit: str,
    cfg: RunConfig,
    populations: dict[str, dict[str, int]],
    excluded: dict[tuple[str, str], int],
) -> str:
    if spec.requires == TIER_MAPPING and cfg["tieringEnabled"] and not cfg["tierByTypeCode"]:
        return "mapping-empty:tierByTypeCode"
    dimension = EXCLUSION_DIMENSION.get(spec.population)
    if dimension and excluded.get((dimension, unit), 0) >= populations[spec.population][unit]:
        return "all-excluded"
    return "no-eligible-entities"


def register_not_applicable(
    agg: dict[tuple, list[Any]],
    data: LoadedData,
    cfg: RunConfig,
    exclusions: dict[tuple, int],
    owner_for: Callable[[str], str | None],
    populations: dict[str, dict[str, int]],
) -> list[NotApplicable]:
    """Adds a zero-count entry for every registered metric that has a source population in a unit but no row there."""
    present = {(key[0], key[8], key[2]) for key in agg}
    excluded: dict[tuple[str, str], int] = defaultdict(int)
    for (dimension, _category, unit, *_), count in exclusions.items():
        excluded[(dimension, unit)] += count
    registered: list[NotApplicable] = []
    for unit, facility in data["facility_spaces"].items():
        for spec in METRIC_SPECS:
            if populations[spec.population][unit] == 0 or (spec.metric, spec.sub_key, unit) in present:
                continue
            key = (
                spec.metric,
                spec.dimension,
                unit,
                facility,
                None,
                None,
                owner_for(spec.dimension),
                None,
                spec.sub_key,
            )
            agg[key] = [0, 0, 0, 0.0]
            registered.append(NotApplicable(spec, unit, _reason(spec, unit, cfg, populations, excluded)))
    return registered


def _warning(code: str, message: str, subject: str | None = None, **details: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"code": code, "message": message}
    if subject is not None:
        out["subject"] = subject
    out.update(details)
    return out


def extra_link_truncation_warnings(gaps: list[LinkGap], cap: int) -> list[dict[str, Any]]:
    """One warning per configured link whose list of instances was cut at `cap` in at least one facility."""
    per: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for gap in gaps:
        per[(gap["model"], gap["view"], gap["property"], gap["space"])] += 1
    cut: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # View.property -> [dropped, facilities]
    for (_model, view, prop, _space), count in per.items():
        if count > cap:
            cut[f"{view}.{prop}"][0] += count - cap
            cut[f"{view}.{prop}"][1] += 1
    return [
        _warning(
            "drilldown-truncated",
            "The drill-down list was cut at the configured limit; the aggregate counts are complete.",
            subject,
            limit=cap,
            dropped=dropped,
            units=units,
        )
        for subject, (dropped, units) in sorted(cut.items())
    ]


def _sample(codes: set[str]) -> list[str]:
    return sorted(codes)[:MAX_LISTED_CODES]


def build_warnings(
    data: LoadedData,
    cfg: RunConfig,
    populations: dict[str, dict[str, int]],
    not_applicable: list[NotApplicable],
    drilldown: list[Any],
) -> list[dict[str, Any]]:
    """Actionable, deterministic warnings about this run. Counts and codes only, never entity ids or names."""
    warnings: list[dict[str, Any]] = []
    total = {name: sum(units.values()) for name, units in populations.items()}

    if cfg["tieringEnabled"] and total["assets"] and not cfg["tierByTypeCode"]:
        warnings.append(
            _warning(
                "mapping-empty",
                "tierByTypeCode is empty: no asset gets a tier, so tier slices are empty and the tier 1 and 2 coverage "
                "metrics cannot be measured.",
                "tierByTypeCode",
            )
        )

    if cfg["tierByTypeCode"] and not cfg["tieringEnabled"]:
        warnings.append(
            _warning(
                "tiering-off",
                "tierByTypeCode is set but tieringEnabled is false, so it is ignored and no asset has a tier.",
                "tierByTypeCode",
            )
        )
    if cfg["tieringEnabled"] and cfg["tierByTypeCode"]:
        untyped, unmapped, codes = 0, 0, set()
        for asset in data["assets"]:
            if asset["root"]:
                continue
            code = asset["code"]
            if code is None:
                untyped += 1
            elif code not in cfg["tierByTypeCode"]:
                unmapped += 1
                codes.add(code)
        if untyped or unmapped:
            warnings.append(
                _warning(
                    "unmapped-asset-types",
                    "Some assets have no type code, or one that tierByTypeCode does not map; they have no tier.",
                    untyped=untyped,
                    unmapped=unmapped,
                    codes=_sample(codes),
                )
            )
    not_loaded = sum(1 for asset in data["assets"] if asset["parent"] is not None and not asset["parent_loaded"])
    if not_loaded:
        warnings.append(
            _warning(
                "parents-not-loaded",
                "Some assets have a parent that is not among the loaded assets. They count as having a parent, and "
                "their depth is a lower bound.",
                assets=not_loaded,
            )
        )

    unavailable = data.get("unavailable_sources", [])
    for source in sorted(unavailable):
        warnings.append(
            _warning(
                "optional-source-unavailable",
                f"The view {source} does not exist in this project; what depends on it is not measured.",
                source,
            )
        )
    for name, count in sorted(total.items()):
        if count == 0 and OPTIONAL_SOURCE_OF.get(name) not in unavailable:
            warnings.append(
                _warning(
                    "no-source-entities",
                    f"No {name} were found in any facility space; their metrics are not measured.",
                    name,
                )
            )

    grouped: dict[tuple[str, str], int] = defaultdict(int)
    for item in not_applicable:
        grouped[(item.subject, item.reason)] += 1
    for (subject, reason), units in sorted(grouped.items()):
        warnings.append(
            _warning(
                "metric-not-applicable",
                "Nothing was eligible to measure, so the metric is written as not applicable instead of as a value.",
                subject,
                reason=reason,
                units=units,
            )
        )

    per_unit: dict[tuple[str, str], int] = defaultdict(int)
    for item in drilldown:
        per_unit[(item["metric"], item["g"]["unit"])] += 1
    cap = cfg["drilldownCap"]
    truncated: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for (metric, _unit), count in per_unit.items():
        if count > cap:
            truncated[metric][0] += count - cap
            truncated[metric][1] += 1
    for metric, (dropped, units) in sorted(truncated.items()):
        warnings.append(
            _warning(
                "drilldown-truncated",
                "The drill-down list was cut at the configured limit; the aggregate counts are complete.",
                metric,
                limit=cap,
                dropped=dropped,
                units=units,
            )
        )
    return sorted(warnings, key=_order)


def _order(warning: dict[str, Any]) -> tuple[str, str, str]:
    return (warning["code"], warning.get("subject") or "", warning.get("reason") or "")


def _dump(warnings: list[dict[str, Any]]) -> str:
    return json.dumps(warnings, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def encode_warnings(warnings: list[dict[str, Any]]) -> str | None:
    """The run marker's `warnings` text: compact, key-sorted JSON of the warnings in a fixed order, or None if empty.

    If they do not fit MAX_WARNINGS_LENGTH the last ones are dropped and a final entry says how many.
    """
    if not warnings:
        return None
    ordered = sorted(warnings, key=_order)
    full = _dump(ordered)
    if len(full) <= MAX_WARNINGS_LENGTH:
        return full
    for kept in range(len(ordered) - 1, -1, -1):
        note = {
            "code": "warnings-truncated",
            "dropped": len(ordered) - kept,
            "message": "More warnings than fit; see a dry run.",
        }
        candidate = _dump([*ordered[:kept], note])
        if len(candidate) <= MAX_WARNINGS_LENGTH:
            return candidate
    return _dump(
        [{"code": "warnings-truncated", "dropped": len(ordered), "message": "More warnings than fit; see a dry run."}]
    )
