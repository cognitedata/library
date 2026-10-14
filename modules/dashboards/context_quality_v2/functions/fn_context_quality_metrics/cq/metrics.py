"""Pure metric computation: loaded views in, raw-count rows out. No CDF calls here.

Rows are counted at the lowest grain: (metric, reporting unit = instance space, division, discipline,
owner, tier). The app sums rows for the slice a user picks and does the division.
"""

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from cq.availability import build_warnings, population_counts, register_not_applicable
from cq.classification import compute_classifications, entity_classes
from cq.types import AssetRow, DrillItem, EntityKey, Grain, LoadedData, Ref, RunConfig


def _owner(cfg: RunConfig, dimension: str, *names: str | None) -> str | None:
    for name in names:
        for rule in cfg["ownerPrefixRules"]:
            if name and name.startswith(rule["prefix"]):
                return rule["ownerId"]
    return cfg["defaultOwners"].get(dimension)


def _tier_label(tier: int | None) -> str:
    return "unassigned tier" if tier is None else f"tier {tier}"


class Acc:
    def __init__(self) -> None:
        self.agg: dict[tuple, list[Any]] = defaultdict(lambda: [0, 0, 0, 0.0])  # eligible, passing, issue, valueSum

    def add(self, key: tuple, eligible: bool = True, passing: bool = False, value: float | None = None) -> None:
        if not eligible:
            return
        a = self.agg[key]
        a[0] += 1
        if passing:
            a[1] += 1
        else:
            a[2] += 1
        if value is not None:
            a[3] += value


def compute(
    data: LoadedData,
    cfg: RunConfig,
    latest_ts: dict[EntityKey, datetime | None],
    now: datetime | None = None,
) -> dict[str, Any]:
    """latest_ts: {(space, externalId): datetime|None} last datapoint per time series."""
    now = now or datetime.now(UTC)
    fac = data["facility_spaces"]
    ex = cfg["exclusions"]
    atype_code = data["asset_types"]
    etype_code = data["equipment_types"]
    fcat_code = data["file_categories"]

    # ---- asset facts
    A: dict[Any, AssetRow] = {(a["space"], a["id"]): a for a in data["assets"]}
    children: dict[Any, int] = defaultdict(int)
    for a in A.values():
        if a["parent"]:
            children[a["parent"]] += 1
    for k, a in A.items():
        code = atype_code.get(a["type"]) if a["type"] else None
        a["code"] = code
        a["tier"] = cfg["tierByTypeCode"].get(code) if code is not None else None
        a["disc"] = cfg["disciplineByTypeCode"].get(code)
        prefix = cfg["divisionTagPrefix"]
        a["div"] = next((t[len(prefix) :] for t in a["tags"] if prefix and t.startswith(prefix)), None)
        a["root"] = a["parent"] is None and children[k] > 0
        a["orphan"] = a["parent"] is None and children[k] == 0
        a["parent_loaded"] = a["parent"] in A
        a["retired"] = bool(cfg["retiredTag"]) and cfg["retiredTag"] in a["tags"]

    def depth(k: Any) -> int:
        d, seen = 1, set()
        while A.get(k) and A[k]["parent"] and k not in seen:
            seen.add(k)
            k = A[k]["parent"]
            d += 1
        return d

    obj3d_linked = {a["object3d"]: k for k, a in A.items() if a["object3d"]}

    def ctx(space: str, asset_key: Ref | None) -> Grain:
        """grain fields for an entity from its own space and the asset it links to."""
        a = A.get(asset_key) if asset_key else None
        return {
            "unit": space,
            "facility": fac.get(space),
            "div": a["div"] if a else None,
            "disc": a["disc"] if a else None,
            "tier": a["tier"] if a else None,
        }

    classes_by_entity = entity_classes(data)
    acc, excl, drill = Acc(), defaultdict(int), []
    drill: list[DrillItem]

    def key(metric: str, dim: str, g: Grain, sub: str | None = None, owner: str | None = None) -> tuple:
        return (metric, dim, g["unit"], g["facility"], g["div"], g["disc"], owner or _owner(cfg, dim), g["tier"], sub)

    def drilldown(
        metric: str, dim: str, g: Grain, ent_id: str, label: str | None, detail: str, owner: str | None = None
    ) -> None:
        drill.append(
            {
                "metric": metric,
                "dim": dim,
                "g": g,
                "id": ent_id,
                "label": label,
                "detail": detail,
                "owner": owner or _owner(cfg, dim),
                "classes": classes_by_entity.get((g["unit"], ent_id)),
            }
        )

    # ---- asset hierarchy
    for k, a in A.items():
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        dim = "asset-hierarchy"
        acc.add(key("hierarchyCompletionRate", dim, g), not a["root"], a["parent"] is not None)
        acc.add(key("orphanRate", dim, g), True, not a["orphan"])
        if a["orphan"]:
            drilldown("orphansByTier", dim, g, a["id"], a["name"], f"{_tier_label(a['tier'])} orphan")
        d = depth(k)
        if d > cfg["maxDepth"]:
            drilldown("depthOutliers", dim, g, a["id"], a["name"], f"depth {d}")
        if a["code"] is None and not a["root"]:
            drilldown("typeCoverageGaps", dim, g, a["id"], a["name"], "no asset type")
    for a in A.values():  # active children under retired parents
        p = A.get(a["parent"]) if a["parent"] else None
        if p and p["retired"] and not a["retired"]:
            g = {
                "unit": a["space"],
                "facility": fac.get(a["space"]),
                "div": a["div"],
                "disc": a["disc"],
                "tier": a["tier"],
            }
            acc.add(key("activeChildrenUnderRetiredParents", "asset-hierarchy", g), True, False)
            drilldown(
                "activeChildrenUnderRetiredParents", "asset-hierarchy", g, a["id"], a["name"], f"parent {p['name']}"
            )

    # ---- equipment
    assets_with_equipment = {e["asset"] for e in data["equipment"] if e["asset"]}
    for e in data["equipment"]:
        g = ctx(e["space"], e["asset"])
        dim = "equipment-asset"
        if any(tag in e["tags"] for tag in ex["decommissionedTags"]):
            excl[("equipment-asset", "user:decommissioned", g["unit"], g["facility"], g["div"], g["disc"])] += 1
            continue
        acc.add(key("associationRate", dim, g), True, e["asset"] is not None)
        if e["asset"] is None:
            drilldown("unmatchedWithCandidates", dim, g, e["id"], e["name"], "no linked asset")
        acc.add(key("serialMfrCompleteness", dim, g, "serialNumber"), True, bool(e["serial"]))
        acc.add(key("serialMfrCompleteness", dim, g, "manufacturer"), True, bool(e["manufacturer"]))
        if e["asset"] and e["etype"]:
            want = A[e["asset"]]["code"] if e["asset"] in A else None
            have = cfg["assetTypeByEquipmentTypeCode"].get(etype_code.get(e["etype"]))
            if want and have:
                ok = want == have
                acc.add(key("typeInconsistencies", dim, g), True, ok)
                if not ok:
                    drilldown(
                        "typeInconsistencies", dim, g, e["id"], e["name"], f"{etype_code.get(e['etype'])} under {want}"
                    )
    for k, a in A.items():
        if a["root"] or a["code"] is None or a["tier"] not in (1, 2):
            continue
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        acc.add(key("assetEquipmentCoverage", "equipment-asset", g), True, k in assets_with_equipment)

    # ---- time series
    ts_assets = {r for t in data["timeseries"] for r in t["assets"]}
    for t in data["timeseries"]:
        link = t["assets"][0] if t["assets"] else None
        g = ctx(t["space"], link)
        dim = "time-series"
        owner = _owner(cfg, dim, t["name"], t["id"])
        if any((t["name"] or "").startswith(p) for p in ex["syntheticTsPrefixes"]):
            excl[("time-series", "synthetic-timeseries", g["unit"], g["facility"], g["div"], g["disc"])] += 1
            continue
        acc.add(key("tsToAssetRate", dim, g, owner=owner), True, link is not None)
        last = latest_ts.get((t["space"], t["id"]))
        fresh = last is not None and (now - last) <= timedelta(days=cfg["freshnessDays"])
        acc.add(key("freshness", dim, g, owner=owner), True, fresh)
        if link is None:
            prefix = (t["name"] or "").replace("_", "-").split("-")[0]
            drilldown("orphansByPrefix", dim, g, t["id"], t["name"], f"prefix {prefix}", owner)
        if not fresh:
            age = "no datapoints" if last is None else f"last {last.date()}"
            drilldown("staleTimeSeries", dim, g, t["id"], t["name"], age, owner)
    for k, a in A.items():
        if a["root"] or a["code"] is None or a["tier"] not in (1, 2):
            continue
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        has = k in ts_assets
        acc.add(key("monitoringCoverage", "time-series", g), True, has)
        if not has and a["tier"] == 1:
            drilldown("tier1CoverageGaps", "time-series", g, a["id"], a["name"], "tier 1 asset without time series")

    # ---- maintenance
    wo_with_notif = {n["work_order"] for n in data["notifications"] if n["work_order"]}
    for w in data["work_orders"]:
        g = ctx(w["space"], w["main_asset"])
        dim = "maintenance"
        acc.add(key("woToAssetRate", dim, g), True, w["main_asset"] is not None)
        has_n = (w["space"], w["id"]) in wo_with_notif
        acc.add(key("woToNotificationRate", dim, g), True, has_n)
        acc.add(key("woCompletionRate", dim, g), True, w["completed"])
        if w["main_asset"] is None:
            drilldown("unlinkedMaintenance", dim, g, w["id"], w["name"], "work order without asset")
        if not has_n and g["tier"] in (1, 2):
            drilldown(
                "woWithoutNotification",
                dim,
                g,
                w["id"],
                w["name"],
                f"{_tier_label(g['tier'])} work order without notification",
            )
    for n in data["notifications"]:
        g = ctx(n["space"], n["asset"])
        dim = "maintenance"
        acc.add(key("notificationToAssetRate", dim, g), True, n["asset"] is not None)
        acc.add(key("notificationToWoRate", dim, g), True, n["work_order"] is not None)
        if n["asset"] is None:
            drilldown("unlinkedMaintenance", dim, g, n["id"], n["name"], "notification without asset")

    # ---- files
    cat_ex, ext_ex = set(ex["locationCentricFileCategories"]), tuple(ex["excludedFileExtensions"])
    for f in data["files"]:
        link = f["assets"][0] if f["assets"] else None
        g = ctx(f["space"], link)
        dim = "files"
        code = fcat_code.get(f["category"]) if f["category"] else None
        f["code"] = code
        if code in cat_ex:
            excl[("files", "location-centric-files", g["unit"], g["facility"], g["div"], g["disc"])] += 1
            f["excluded"] = True
            continue
        if (f["name"] or "").lower().endswith(ext_ex):
            excl[("files", "user:file-extension", g["unit"], g["facility"], g["div"], g["disc"])] += 1
            f["excluded"] = True
            continue
        f["excluded"] = False
        acc.add(key("fileToAssetRate", dim, g), True, link is not None)
        for sub, present in (
            ("category", f["category"] is not None),
            ("description", bool(f["description"])),
            ("sourceId", bool(f["source_id"])),
        ):
            acc.add(key("metadataCompleteness", dim, g, sub), True, present)
        if link is None:
            drilldown("unlinkedByCategory", dim, g, f["id"], f["name"], f"category {code or 'none'}")

    # ---- 3D
    for o in data["objects3d"]:
        owner_asset = obj3d_linked.get((o["space"], o["id"]))
        g = ctx(o["space"], owner_asset)
        dim = "model-3d"
        acc.add(key("threeDToAssetRate", dim, g), True, owner_asset is not None)
        acc.add(key("bboxCompleteness", dim, g), True, o["bbox"])
        if not o["bbox"]:
            drilldown("bboxGaps", dim, g, o["id"], o["name"], "missing bounding box value")
    for a in A.values():
        if a["root"] or a["code"] is None or a["tier"] != 1:
            continue
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": 1}
        acc.add(key("asset3dCoverage", "model-3d", g), True, a["object3d"] is not None)
        if a["object3d"] is None:
            drilldown("unlinkedTier1", "model-3d", g, a["id"], a["name"], "tier 1 asset without 3D object")

    # ---- file annotation
    fa_rows = defaultdict(lambda: [0, 0.0])  # (unit, facility, div, tier, status, bucket) -> count, confSum
    dim = "file-annotation"
    for an in data["annotations"]:
        g = ctx(an["space"], an["asset"])
        conf = an["confidence"]
        acc.add(key("annotationCount", dim, g), True, True)
        if conf is not None:
            acc.add(key("confidenceAverage", dim, g), True, True, value=conf)
        for status in ("Approved", "Suggested", "Rejected"):
            acc.add(key("approvalMix", dim, g, status), True, an["status"] == status)
        low = conf is not None and conf < cfg["lowConfidence"]
        acc.add(key("lowConfidenceQueue", dim, g), True, not low)
        if low:
            drilldown("lowConfidenceQueue", dim, g, an["id"], an["file"][1], f"confidence {conf}")
        pending = an["status"] == "Suggested"
        acc.add(key("pendingReview", dim, g), True, not pending)
        if pending:
            age = (now - an["created"]).days if an["created"] else None
            drilldown(
                "pendingReview",
                dim,
                g,
                an["id"],
                an["file"][1],
                f"awaiting review {age} days" if age is not None else "awaiting review",
            )
        bucket = "none" if conf is None else "high" if conf >= cfg["highConfidence"] else "low" if low else "medium"
        r = fa_rows[(g["unit"], g["facility"], g["div"], g["tier"], an["status"], bucket)]
        r[0] += 1
        r[1] += conf or 0.0

    classifications = compute_classifications(data, cfg, A, ctx, lambda dimension: _owner(cfg, dimension))
    populations = population_counts(data)
    not_applicable = register_not_applicable(
        acc.agg, data, cfg, excl, lambda dimension: _owner(cfg, dimension), populations
    )
    return {
        "agg": acc.agg,
        "exclusions": excl,
        "drilldown": drill,
        "annotation_buckets": fa_rows,
        "classifications": classifications,
        "warnings": build_warnings(data, cfg, populations, not_applicable, drill),
    }
