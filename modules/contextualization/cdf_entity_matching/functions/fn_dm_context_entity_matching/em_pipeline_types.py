"""Typed structures for the entity matching pipeline."""

import re
from typing import NotRequired, TypedDict


class FunctionInputData(TypedDict):
    """CDF function invocation payload."""

    ExtractionPipelineExtId: str
    logLevel: NotRequired[str]


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


class TargetMatchRecord(TypedDict):
    """Target used as ML/rule matching input."""

    asset_ext_id: str
    asset_space: str
    org_name: str
    name: str
    rule_keys: list[str] | None


class EntityMatchSource(TypedDict):
    """Entity submitted for ML/rule matching."""

    entity_ext_id: str
    entity_space: str
    org_name: str
    name: str
    assets: str
    rule_keys: list[str] | None


class StoredMatch(TypedDict):
    """Match row written to RAW or carried through the pipeline."""

    match_type: str
    entity_ext_id: str
    entity_space: str | None
    entity_name: str
    entity_match_value: str
    entity_view_id: str
    entity_existing_assets: object
    score: float
    asset_name: str
    asset_match_value: str
    asset_ext_id: str
    asset_space: str | None
    asset_view_id: str
    entity_rule_keys: NotRequired[str]
    asset_rule_keys: NotRequired[str]


class EntityMatchTarget(TypedDict):
    asset_ext_id: str
    asset_space: NotRequired[str]
    org_name: str
    name: str


class EntityMatchResultItem(TypedDict):
    target: EntityMatchTarget
    score: float


class EntityMatchApiSource(TypedDict):
    entity_ext_id: str
    entity_space: NotRequired[str]
    org_name: str
    name: str
    assets: str


class EntityMatchingApiMatch(TypedDict):
    """Single item returned from entity matching prediction."""

    source: EntityMatchApiSource
    matches: list[EntityMatchResultItem]
