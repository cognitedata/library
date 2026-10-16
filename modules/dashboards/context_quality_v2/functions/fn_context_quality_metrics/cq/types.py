"""Typed shapes for the dicts that flow through the metrics function.

The function keeps entities as plain dicts that the metric code enriches in place (tier, discipline, ...), so these
are TypedDicts rather than classes: they document the shapes without changing how the code reads or writes them.
"""

from datetime import datetime
from typing import Any, NotRequired, TypedDict

# A direct relation: (space, externalId). The space is None when the source property has no space.
type Ref = tuple[str | None, str]
type EntityKey = tuple[str, str]
type RowProps = dict[str, Any]
type Row = tuple[str, RowProps]


class AssetRow(TypedDict):
    space: str
    id: str
    name: str | None
    tags: list[str]
    parent: Ref | None
    type: Ref | None
    aclass: Ref | None
    object3d: Ref | None
    # filled in by compute() before the metric code reads them
    code: str | None
    tier: int | None
    disc: str | None
    div: str | None
    root: bool
    parent_loaded: bool
    orphan: bool
    retired: bool


class EquipmentRow(TypedDict):
    space: str
    id: str
    name: str | None
    tags: list[str]
    asset: Ref | None
    etype: Ref | None
    serial: str | None
    manufacturer: str | None


class TimeSeriesRow(TypedDict):
    space: str
    id: str
    name: str | None
    description: str | None
    assets: list[Ref]
    unit: str | None


class FileRow(TypedDict):
    space: str
    id: str
    name: str | None
    description: str | None
    source_id: str | None
    category: Ref | None
    assets: list[Ref]
    code: str | None  # filled in by compute()
    excluded: bool  # filled in by compute()


class Object3DRow(TypedDict):
    space: str
    id: str
    name: str | None
    bbox: bool


class WorkOrderRow(TypedDict):
    space: str
    id: str
    name: str | None
    main_asset: Ref | None
    completed: bool


class NotificationRow(TypedDict):
    space: str
    id: str
    name: str | None
    asset: Ref | None
    work_order: Ref | None


class AnnotationRow(TypedDict):
    space: str
    id: str
    file: tuple[str, str]
    asset: tuple[str, str]
    confidence: float | None
    status: str | None
    created: datetime | None


class LoadedData(TypedDict):
    asset_types: dict[Ref, str | None]
    equipment_types: dict[Ref, str | None]
    file_categories: dict[Ref, str | None]
    class_labels: dict[str, dict[Ref, str]]
    assets: list[AssetRow]
    equipment: list[EquipmentRow]
    timeseries: list[TimeSeriesRow]
    files: list[FileRow]
    objects3d: list[Object3DRow]
    work_orders: list[WorkOrderRow]
    notifications: list[NotificationRow]
    annotations: list[AnnotationRow]
    entity_counts_by_space: dict[str, int]
    facility_spaces: dict[str, str]
    # optional views that do not exist in the project, as `space.view`; the run measures what it can without them
    unavailable_sources: NotRequired[list[str]]


class Grain(TypedDict):
    """The slice fields every counted row carries."""

    unit: str
    facility: str | None
    div: str | None
    disc: str | None
    tier: int | None


class DrillItem(TypedDict):
    metric: str
    dim: str
    g: Grain
    id: str
    label: str | None
    detail: str
    owner: str | None
    classes: dict[str, str] | None


class RunConfig(TypedDict):
    stream: str
    schemaSpace: str
    instanceSpace: str
    relationshipContainer: str
    fileAnnotationContainer: str
    facilitySpaces: dict[str, str]
    facilityNames: dict[str, str]
    excludeSpaces: list[str]
    minFacilityEntities: int
    tieringEnabled: bool
    tierByTypeCode: dict[str, int]
    disciplineByTypeCode: dict[str, str]
    assetTypeByEquipmentTypeCode: dict[str, str]
    divisionTagPrefix: str
    retiredTag: str
    ownerPrefixRules: list[dict[str, str]]
    defaultOwners: dict[str, str]
    exclusions: dict[str, list[str]]
    freshnessDays: int
    maxDepth: int
    lowConfidence: float
    highConfidence: float
    drilldownCap: int
    batchSize: int
