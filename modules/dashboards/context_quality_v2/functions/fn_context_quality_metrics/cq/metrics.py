"""Pure metric computation: loaded views in, raw-count rows out. No CDF calls here.

Rows are counted at the lowest grain: (metric, reporting unit = instance space, division, discipline,
owner, tier). The app sums rows for the slice a user picks and does the division.
"""

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from cq.availability import build_warnings, population_counts, register_not_applicable
from cq.classification import compute_classifications, entity_classes
from cq.cross import Cross
from cq.types import AssetRow, DrillItem, EntityKey, Grain, LoadedData, Ref, RunConfig

KINDS = ("assets", "equipment", "timeseries", "files", "objects3d", "work_orders", "notifications", "annotations")

_FILE_FIELD_LABEL = {"category": "category", "description": "description", "sourceId": "source ID"}


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
    kinds: frozenset[str] | None = None,
    cross: Cross | None = None,
) -> dict[str, Any]:
    """latest_ts: {(space, externalId): datetime|None} last datapoint per time series.

    `kinds` limits which entity kinds produce rows (every kind when None); a metric belongs to the kind it counts, so
    batches of different kinds never count the same thing twice. `cross` answers what the entities in `data` link to
    but `data` does not hold (cq.cross); without it everything is resolved from `data`, which holds the whole space.
    """
    now = now or datetime.now(UTC)
    want = (lambda kind: True) if kinds is None else (lambda kind: kind in kinds)
    fac = data["facility_spaces"]
    ex = cfg["exclusions"]
    atype_code = data["asset_types"]
    fcat_code = data["file_categories"]
    tiering = cfg["tieringEnabled"]

    def labelled(tier: int | None, text: str) -> str:
        """The drill-down detail: it names the tier only while tiering is on."""
        return f"{_tier_label(tier)} {text}" if tiering else text

    # ---- asset facts
    A: dict[Any, AssetRow] = {(a["space"], a["id"]): a for a in data["assets"]}
    prefix = cfg["divisionTagPrefix"]

    def enrich(a: AssetRow) -> None:
        code = atype_code.get(a["type"]) if a["type"] else None
        a["code"] = code
        a["tier"] = cfg["tierByTypeCode"].get(code) if tiering and code is not None else None
        a["disc"] = None
        a["div"] = next((t[len(prefix) :] for t in a["tags"] if prefix and t.startswith(prefix)), None)
        a["retired"] = bool(cfg["retiredTag"]) and cfg["retiredTag"] in a["tags"]

    # the assets an entity of the batch may need to look at: its own, and the ones `cross` fetched for it
    known: dict[Any, AssetRow] = dict(A)
    for key_, row in (cross.assets if cross else {}).items():
        if key_ not in known:
            enrich(row)
            known[key_] = row
    has_children = {a["parent"] for a in A.values() if a["parent"]} if cross is None else cross.has_children
    for k, a in A.items():
        enrich(a)
        a["root"] = a["parent"] is None and k in has_children
        a["orphan"] = a["parent"] is None and k not in has_children
        a["parent_loaded"] = a["parent"] in known

    def depth(k: Any) -> int:
        d, seen = 1, set()
        while known.get(k) and known[k]["parent"] and k not in seen:
            seen.add(k)
            k = known[k]["parent"]
            d += 1
        return d

    obj3d_linked = {a["object3d"]: k for k, a in A.items() if a["object3d"]} if cross is None else cross.linked_objects

    def ctx(space: str, asset_key: Ref | None) -> Grain:
        """grain fields for an entity from its own space and the asset it links to."""
        a = known.get(asset_key) if asset_key else None
        return {
            "unit": space,
            "facility": fac.get(space),
            "div": a["div"] if a else None,
            "disc": a["disc"] if a else None,
            "tier": a["tier"] if a else None,
        }

    # what is counted here: the kinds this call is responsible for
    scoped: Any = data if kinds is None else {**data, **{kind: [] for kind in KINDS if kind not in kinds}}
    populated = cross.populated if cross else None
    classes_by_entity = entity_classes(scoped, populated)
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
    for k, a in A.items() if want("assets") else ():
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        dim = "asset-hierarchy"
        acc.add(key("hierarchyCompletionRate", dim, g), not a["root"], a["parent"] is not None)
        acc.add(key("orphanRate", dim, g), True, not a["orphan"])
        if a["orphan"]:
            drilldown("orphansByTier", dim, g, a["id"], a["name"], labelled(a["tier"], "orphan"))
        d = depth(k)
        if d > cfg["maxDepth"]:
            drilldown("depthOutliers", dim, g, a["id"], a["name"], f"depth {d}")
        if a["code"] is None and not a["root"]:
            drilldown("typeCoverageGaps", dim, g, a["id"], a["name"], "no asset type")
    for a in A.values() if want("assets") else ():  # active children under retired parents
        p = known.get(a["parent"]) if a["parent"] else None
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
    for e in data["equipment"] if want("equipment") else ():
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
        if not e["serial"]:
            drilldown("equipmentMissingSerial", dim, g, e["id"], e["name"], "equipment without serial number")
        if not e["manufacturer"]:
            drilldown("equipmentMissingManufacturer", dim, g, e["id"], e["name"], "equipment without manufacturer")
    # ---- time series
    ts_assets = {r for t in data["timeseries"] for r in t["assets"]} if cross is None else cross.ts_assets
    for t in data["timeseries"] if want("timeseries") else ():
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
    for k, a in A.items() if want("assets") else ():
        if a["root"] or (tiering and a["tier"] not in (1, 2)):
            continue
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        has = k in ts_assets
        acc.add(key("monitoringCoverage", "time-series", g), True, has)
        if not has and (a["tier"] == 1 or not tiering):
            drilldown(
                "tier1CoverageGaps",
                "time-series",
                g,
                a["id"],
                a["name"],
                labelled(a["tier"], "asset without time series"),
            )

    # ---- maintenance
    wo_with_notif = (
        {n["work_order"] for n in data["notifications"] if n["work_order"]}
        if cross is None
        else cross.wo_with_notification
    )
    for w in data["work_orders"] if want("work_orders") else ():
        g = ctx(w["space"], w["main_asset"])
        dim = "maintenance"
        acc.add(key("woToAssetRate", dim, g), True, w["main_asset"] is not None)
        has_n = (w["space"], w["id"]) in wo_with_notif
        acc.add(key("woToNotificationRate", dim, g), True, has_n)
        acc.add(key("woCompletionRate", dim, g), True, w["completed"])
        if not w["completed"]:
            drilldown("incompleteWorkOrders", dim, g, w["id"], w["name"], "work order not completed")
        if w["main_asset"] is None:
            drilldown("unlinkedMaintenance", dim, g, w["id"], w["name"], "work order without asset")
        if not has_n and (g["tier"] in (1, 2) or not tiering):
            drilldown(
                "woWithoutNotification",
                dim,
                g,
                w["id"],
                w["name"],
                labelled(g["tier"], "work order without notification"),
            )
    for n in data["notifications"] if want("notifications") else ():
        g = ctx(n["space"], n["asset"])
        dim = "maintenance"
        acc.add(key("notificationToAssetRate", dim, g), True, n["asset"] is not None)
        acc.add(key("notificationToWoRate", dim, g), True, n["work_order"] is not None)
        if n["work_order"] is None:
            drilldown("notificationsWithoutWorkOrder", dim, g, n["id"], n["name"], "notification without work order")
        if n["asset"] is None:
            drilldown("unlinkedMaintenance", dim, g, n["id"], n["name"], "notification without asset")

    # ---- files
    cat_ex, ext_ex = set(ex["locationCentricFileCategories"]), tuple(ex["excludedFileExtensions"])
    for f in data["files"] if want("files") else ():
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
        for sub, present, listing in (
            ("category", f["category"] is not None, "filesMissingCategory"),
            ("description", bool(f["description"]), "filesMissingDescription"),
            ("sourceId", bool(f["source_id"]), "filesMissingSourceId"),
        ):
            acc.add(key("metadataCompleteness", dim, g, sub), True, present)
            if not present:
                drilldown(listing, dim, g, f["id"], f["name"], f"file without {_FILE_FIELD_LABEL[sub]}")
        if link is None:
            drilldown("unlinkedByCategory", dim, g, f["id"], f["name"], f"category {code or 'none'}")

    # ---- 3D
    for o in data["objects3d"] if want("objects3d") else ():
        owner_asset = obj3d_linked.get((o["space"], o["id"]))
        g = ctx(o["space"], owner_asset)
        dim = "model-3d"
        acc.add(key("threeDToAssetRate", dim, g), True, owner_asset is not None)
        if owner_asset is None:
            drilldown("unlinked3dObjects", dim, g, o["id"], o["name"], "3D object without asset")
        acc.add(key("bboxCompleteness", dim, g), True, o["bbox"])
        if not o["bbox"]:
            drilldown("bboxGaps", dim, g, o["id"], o["name"], "missing bounding box value")
    for a in A.values() if want("assets") else ():
        if a["root"] or (tiering and a["tier"] != 1):
            continue
        g = {"unit": a["space"], "facility": fac.get(a["space"]), "div": a["div"], "disc": a["disc"], "tier": a["tier"]}
        acc.add(key("asset3dCoverage", "model-3d", g), True, a["object3d"] is not None)
        if a["object3d"] is None:
            drilldown(
                "unlinkedTier1", "model-3d", g, a["id"], a["name"], labelled(a["tier"], "asset without 3D object")
            )

    # ---- file annotation
    fa_rows = defaultdict(lambda: [0, 0.0])  # (unit, facility, div, tier, status, bucket) -> count, confSum
    dim = "file-annotation"
    for an in data["annotations"] if want("annotations") else ():
        g = ctx(an["space"], an["asset"])
        conf = an["confidence"]
        acc.add(key("annotationCount", dim, g), True, True)
        if conf is not None:
            acc.add(key("confidenceAverage", dim, g), True, True, value=conf)
        for status in ("Approved", "Suggested", "Rejected"):
            acc.add(key("approvalMix", dim, g, status), True, an["status"] == status)
        if an["status"] != "Approved":
            detail = an["status"] or "no status"
            drilldown(
                "unapprovedAnnotations",
                dim,
                g,
                an["id"],
                an["file"][1],
                f"{detail}, confidence {conf}" if conf is not None else detail,
            )
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

    classifications = compute_classifications(scoped, cfg, A, ctx, lambda dimension: _owner(cfg, dimension), populated)
    populations = population_counts(scoped)
    not_applicable = register_not_applicable(
        acc.agg, scoped, cfg, excl, lambda dimension: _owner(cfg, dimension), populations
    )
    return {
        "agg": acc.agg,
        "exclusions": excl,
        "drilldown": drill,
        "annotation_buckets": fa_rows,
        "classifications": classifications,
        "warnings": build_warnings(scoped, cfg, populations, not_applicable, drill),
    }
