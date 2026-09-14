"""Typed structures for the entity matching pipeline."""

import re
from typing import TypedDict


class FunctionInputData(TypedDict, total=False):
    """CDF function invocation payload."""

    ExtractionPipelineExtId: str
    logLevel: str


class RawRowColumns(TypedDict, total=False):
    """Columns on a manual-mapping RAW row."""

    Contextualized: bool
    TsExternalId: str
    AssetExternalId: str


class ManualMappingDefinition(TypedDict):
    key: str
    TsExternalId: str
    AssetExternalId: str


class RuleMappingDefinition(TypedDict):
    key: str
    EntityRegExp: re.Pattern[str]
    AssetRegExp: re.Pattern[str]


class TargetMatchRecord(TypedDict, total=False):
    """Target used as ML/rule matching input."""

    asset_ext_id: str
    org_name: str
    name: str
    rule_keys: list[str] | None


class EntityMatchSource(TypedDict, total=False):
    """Entity submitted for ML/rule matching."""

    entity_ext_id: str
    org_name: str
    name: str
    assets: str
    rule_keys: list[str] | None


class StoredMatch(TypedDict, total=False):
    """Match row written to RAW or carried through the pipeline."""

    match_type: str
    entity_ext_id: str
    entity_name: str
    entity_match_value: str
    entity_view_id: str
    entity_existing_assets: object
    score: float
    asset_name: str
    asset_match_value: str
    asset_ext_id: str
    asset_view_id: str


class EntityMatchTarget(TypedDict, total=False):
    asset_ext_id: str
    org_name: str
    name: str


class EntityMatchResultItem(TypedDict, total=False):
    target: EntityMatchTarget
    score: float


class EntityMatchApiSource(TypedDict, total=False):
    entity_ext_id: str
    org_name: str
    name: str
    assets: str


class EntityMatchingApiMatch(TypedDict, total=False):
    """Single item returned from entity matching prediction."""

    source: EntityMatchApiSource
    matches: list[EntityMatchResultItem]


class DirectRelationLink(TypedDict):
    externalId: str
    space: str
