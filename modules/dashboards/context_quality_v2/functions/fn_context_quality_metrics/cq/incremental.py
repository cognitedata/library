"""Reusing the result of a batch when nothing it reads has changed since it was computed (a run in `incremental` mode).

A batch (cq.runner.compute_batch) counts one slice of one kind of a space, and what it writes is a function of the
instances it reads. Every batch therefore leaves its result in a row of a RAW table (`stateDatabase`, `stateTable`; the
row key is `piece_key`), together with the time of the run that computed it. A run in `incremental` mode asks, for each batch, whether
anything the batch reads has a `lastUpdatedTime` after that time. If nothing does, the stored result is written into
the new run as it is, and the slice is not read again. A batch that did change is counted whole, as in a full run.

What a batch reads is its own slice, plus what it looks up beyond it (`LOOKS_AT`: the whole space of another kind, or of
its own), the reference nodes that name classifications, and, when tier or division is configured, the assets its
entities link to. Whether a classification kind is populated in the space is asked again, as it is judged for the
whole space. A result is only reused when all of these hold, and any doubt (a query that fails, a file that cannot be
read) means the batch is counted.

What a change time cannot show, so a full run is still what makes everything exact:

- an instance that was deleted (the batch's count is compared, which catches a delete that was not paired with an add);
- time series, whose result depends on their datapoints and on the clock (freshness), and are never reused;
- the age in days an annotation has been awaiting review, which is written into the detail text.

The key of a stored result holds everything that decides it: the slice, the dimensions the batch writes, the configuration and
the code of this package, so a changed configuration or a new deployment finds no stored result and counts everything.
"""

import base64
import hashlib
import json
import zlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as f
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError

from cq import lookups
from cq.batching import leaf_filter
from cq.reader import CDM, KIND_VIEWS, _labels
from cq.scope import DIMENSION_KINDS, parse_scope
from cq.types import Row, RunConfig

FULL, INCREMENTAL = "full", "incremental"
# A RAW column holds 2,621,440 characters at most. A result is kept compressed, and one that still does not fit is not kept.
MAX_BODY_CHARS = 2_000_000
# A change made while the run that computed a result was starting is found at the next run, not missed.
SAFETY = timedelta(minutes=5)

# What a batch of a kind looks at in the whole space, besides its own slice (cq.lookups.fetch_cross): the assets above
# and below an asset and the time series linked to it, the assets that link to a 3D object, the notifications of a work order.
LOOKS_AT: dict[str, tuple[str, ...]] = {
    "assets": ("assets", "timeseries"),
    "objects3d": ("assets",),
    "work_orders": ("notifications",),
}
# The nodes that give a classification its label; a change to any of them can change the rows of any batch.
REFERENCE_VIEWS = ("CogniteAssetType", "CogniteAssetClass", "CogniteEquipmentType", "CogniteFileCategory")
# The view of the nodes that label each classification kind of cq.lookups.CLASSIFIED_BY
_LABEL_VIEWS = {
    "assetType": "CogniteAssetType",
    "assetClass": "CogniteAssetClass",
    "equipmentType": "CogniteEquipmentType",
    "fileCategory": "CogniteFileCategory",
}
# Not part of what a batch computes
_PLAN_ONLY = (
    "maxShards",
    "maxConcurrentShards",
    "shardTargetEntities",
    "exportDataSet",
    "exportRetentionDays",
    "stateDatabase",
    "stateTable",
)


def parse_mode(raw: Any) -> str:
    """`full` (every batch is counted) or `incremental`. Absent, or a workflow reference that was not filled in, is full."""
    if raw is None or raw == "" or (isinstance(raw, str) and raw.startswith("${")):
        return FULL
    if raw not in (FULL, INCREMENTAL):
        raise ValueError(f'mode must be "full" or "incremental", not {raw!r}')
    return raw


@dataclass
class PieceResult:
    """What a batch writes, before its rows get the batch's number: the rows, the warnings and the items for the workbook."""

    rows: list[Row]
    annotation_rows: list[Row]
    warnings: list[dict[str, Any]]
    listed: list[tuple[str, ...]]
    populated: list[str] = field(default_factory=list)


@cache
def code_version() -> str:
    """A digest of this package's code: a stored result never outlives the code that made it."""
    digest = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _dimensions_of(payload: dict[str, Any]) -> list[str]:
    """The dimensions this batch writes rows for: those of the scope that its kinds feed. A batch of equipment writes the
    same rows whether the run is for every dimension or for equipment-asset only, so one result serves both."""
    kinds = set(payload.get("kinds") or [])
    scope = parse_scope(payload.get("scope"))
    return [
        dimension
        for dimension in DIMENSION_KINDS
        if dimension in scope.dimensions and DIMENSION_KINDS[dimension] & kinds
    ]


def piece_key(cfg: RunConfig, payload: dict[str, Any]) -> str:
    """The RAW row key of the stored result of a batch: the same slice, scope, configuration and code give the same one."""
    identity = {
        "space": payload["space"],
        "facility": payload["facility"],
        "kinds": sorted(payload.get("kinds") or []),
        "leaves": payload.get("leaves"),
        "groupBatches": payload.get("groupBatches") or 1,
        "dimensions": _dimensions_of(payload),
        "config": {key: value for key, value in cfg.items() if key not in _PLAN_ONLY},
        "code": code_version(),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


def _not_reusable(payload: dict[str, Any]) -> str | None:
    if payload.get("views"):
        return "configured links are always counted"
    if not payload.get("kinds"):
        return "nothing to count"
    if "timeseries" in payload["kinds"]:
        return "time series are always counted (datapoints and the clock decide their result)"
    return None


def _any_since(
    client: CogniteClient, view: ViewId, edge: bool, space: str | None, flt: Filter | None, since: int
) -> bool:
    kind = "edge" if edge else "node"
    recent = f.Range([kind, "lastUpdatedTime"], gt=since)
    found = client.data_modeling.instances.list(
        instance_type=kind,
        sources=view,
        space=space,
        filter=recent if flt is None else f.And(flt, recent),
        limit=1,
    )
    return len(found) > 0


def _what_changed(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any], since: int) -> str | None:
    """What a batch reads that changed after `since` (epoch ms), or None. Raises CogniteAPIError when it cannot tell."""
    space, kinds = payload["space"], list(payload.get("kinds") or [])
    leaves = [(kind, text) for kind, text in payload.get("leaves") or []]
    for kind in kinds:
        view_space, xid, edge = KIND_VIEWS[kind]
        view = ViewId(view_space, xid, "v1")
        # a slice is asked leaf by leaf: the service answers an OR of slices slowly (cq.reader._list_instances)
        for flt in [leaf_filter(leaf, edge) for leaf in leaves] or [None]:
            if _any_since(client, view, edge, space, flt, since):
                return kind
    wider = {other for kind in kinds for other in LOOKS_AT.get(kind, ())}
    if cfg["tieringEnabled"] or cfg["divisionTagPrefix"]:
        wider.add("assets")
    if not leaves:
        wider -= set(kinds)
    for kind in sorted(wider):
        view_space, xid, edge = KIND_VIEWS[kind]
        if _any_since(client, ViewId(view_space, xid, "v1"), edge, space, None, since):
            return f"{kind} of the space"
    for xid in REFERENCE_VIEWS:
        if _any_since(client, ViewId(CDM, xid, "v1"), False, None, None, since):
            return xid
    return None


def _populated_now(client: CogniteClient, payload: dict[str, Any]) -> list[str]:
    kinds = frozenset(payload.get("kinds") or [])
    labels = {
        kind: _labels(client, _LABEL_VIEWS[kind]) for kind, entities, *_ in lookups.CLASSIFIED_BY if entities in kinds
    }
    data: Any = {"class_labels": labels}
    return sorted(lookups._populated(client, data, kinds, payload["space"]))


def _keeps_results(cfg: RunConfig) -> bool:
    return bool(cfg["stateDatabase"] and cfg["stateTable"])


def _encode(body: dict[str, Any]) -> str:
    return base64.b64encode(zlib.compress(json.dumps(body, separators=(",", ":")).encode("utf-8"))).decode("ascii")


def _result_of(encoded: str) -> PieceResult:
    body = json.loads(zlib.decompress(base64.b64decode(encoded)))
    return PieceResult(
        rows=[(row_key, props) for row_key, props in body["rows"]],
        annotation_rows=[(row_key, props) for row_key, props in body["annotationRows"]],
        warnings=body["warnings"],
        listed=[tuple(row) for row in body["listed"]],
        populated=body["populated"],
    )


def reuse(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> tuple[PieceResult | None, str]:
    """The stored result of a batch when it can be reused, and in any case a short note on why or why not."""
    if parse_mode(payload.get("mode")) != INCREMENTAL:
        return None, "full run"
    if not _keeps_results(cfg):
        return None, "no table to keep results in"
    if (why := _not_reusable(payload)) is not None:
        return None, why
    try:
        row = client.raw.rows.retrieve(cfg["stateDatabase"], cfg["stateTable"], piece_key(cfg, payload))
        if row is None or not row.columns:
            return None, "no earlier result"
        kept = row.columns
        if kept.get("count") != payload.get("count"):
            return None, "the number of instances changed"
        computed = datetime.fromisoformat(kept["computedAt"].replace("Z", "+00:00"))
        if (changed := _what_changed(client, cfg, payload, int((computed - SAFETY).timestamp() * 1000))) is not None:
            return None, f"{changed} changed"
        result = _result_of(kept["body"])
        if result.populated != _populated_now(client, payload):
            return None, "the classifications in use changed"
    except (CogniteAPIError, KeyError, ValueError, TypeError, zlib.error) as error:
        return None, f"could not reuse the earlier result ({type(error).__name__}: {str(error)[:120]})"
    return result, "reused"


def remember(
    client: CogniteClient, cfg: RunConfig, payload: dict[str, Any], result: PieceResult, run_ts: datetime
) -> None:
    """Keeps the result of a batch for the next incremental run. Best effort: a run never fails for it. A result too big
    for a row is not kept; the batch is counted again next time, as one with no earlier result."""
    if not _keeps_results(cfg) or _not_reusable(payload) is not None:
        return
    body = {
        "rows": result.rows,
        "annotationRows": result.annotation_rows,
        "warnings": result.warnings,
        "listed": result.listed,
        "populated": result.populated,
    }
    encoded = _encode(body)
    if len(encoded) > MAX_BODY_CHARS:
        return
    kept = {"computedAt": run_ts.astimezone(UTC).isoformat(), "count": payload.get("count"), "body": encoded}
    try:
        client.raw.rows.insert(cfg["stateDatabase"], cfg["stateTable"], {piece_key(cfg, payload): kept})
    except CogniteAPIError:
        pass


def remove_old_results(client: CogniteClient, cfg: RunConfig, now: datetime) -> int:
    """Deletes the stored results that no run has written for `exportRetentionDays`. -> how many. A batch that was
    reused is not written again, but a full run writes every batch, and one runs weekly, so a result in use is young."""
    if not _keeps_results(cfg):
        return 0
    cutoff = int((now - timedelta(days=cfg["exportRetentionDays"])).timestamp() * 1000)
    database, table = cfg["stateDatabase"], cfg["stateTable"]
    removed = 0
    try:
        old = [
            row.key for row in client.raw.rows.list(database, table, max_last_updated_time=cutoff, columns=[], limit=-1)
        ]
        for start in range(0, len(old), 1000):
            client.raw.rows.delete(database, table, old[start : start + 1000])
            removed += len(old[start : start + 1000])
    except CogniteAPIError:
        pass
    return removed
