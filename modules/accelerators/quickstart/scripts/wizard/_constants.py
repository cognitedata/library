"""
Constants, compiled regexes, shared dataclasses, and static module registries
for the Quickstart DP setup wizard.
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

# Version constants

MIN_TOOLKIT_VERSION: tuple[int, int, int] = (0, 7, 210)
# Toolkit 0.8.0 changed the build/deploy flag from --env=<env> to -c <config>.
_CONFIG_FLAG_VERSION: tuple[int, int, int] = (0, 8, 0)

# Environment variable names

ENV_VAR_GROUP_SOURCE_ID = "GROUP_SOURCE_ID"
ENV_VAR_OPEN_ID_CLIENT_SECRET = "OPEN_ID_CLIENT_SECRET"

# Valid wizard target environments

VALID_ENVIRONMENTS: frozenset[str] = frozenset({"dev", "prod", "staging"})

# YAML paths for fields the wizard writes

ENV_PROJECT_YAML_PATH: tuple[str, ...] = ("environment", "project")

APP_OWNER_YAML_PATH: tuple[str, ...] = (
    "variables", "modules", "accelerators", "contextualization",
    "cdf_file_annotation", "ApplicationOwner",
)

# SQL mode markers and block anchors (asset.Transformation.sql)

SQL_COMMON_MODE_MARKER = "[COMMON MODE]"
SQL_FILE_ANNOTATION_MODE_MARKER = "[FILE_ANNOTATION MODE]"
SQL_COMMON_BLOCK_ANCHOR = "with parentLookup as ("
SQL_FILE_ANNOTATION_BLOCK_ANCHOR = "with root as ("

# Compiled regexes

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_YAML_LINE_RE = re.compile(r"^(\s*[A-Za-z0-9_]+:\s*)([^#\n]*)(\s*(?:#.*)?)$")

# Data types

@dataclass(frozen=True)
class GroupTarget:
    label: str
    path: tuple[str, ...]
    default_env_var: str
    module: str       # human-readable module path (for per-module prompts)
    description: str  # what this group controls (for per-module prompts)


@dataclass
class ChangeRecord:
    label: str
    old_val: str   # "<not set>" when the field was empty before
    new_val: str

    @property
    def changed(self) -> bool:
        return self.old_val != self.new_val


# Module registry

GROUP_TARGETS: Sequence[GroupTarget] = (
    GroupTarget(
        label="cdf_ingestion.groupSourceId",
        path=("variables", "modules", "accelerators", "cdf_ingestion", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_INGESTION",
        module="accelerators/cdf_ingestion",
        description="Data ingestion pipeline — controls write access for ingestion jobs",
    ),
    GroupTarget(
        label="cdf_entity_matching.entity_matching_processing_group_source_id",
        path=(
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "entity_matching_processing_group_source_id",
        ),
        default_env_var="GROUP_SOURCE_ID_ENTITY_MATCHING",
        module="accelerators/contextualization/cdf_entity_matching",
        description="Entity matching — controls access for the matching processing service",
    ),
    GroupTarget(
        label="cdf_file_annotation.groupSourceId",
        path=(
            "variables", "modules", "accelerators", "contextualization",
            "cdf_file_annotation", "groupSourceId",
        ),
        default_env_var="GROUP_SOURCE_ID_FILE_ANNOTATION",
        module="accelerators/contextualization/cdf_file_annotation",
        description="File annotation — controls access for the annotation processing service",
    ),
    GroupTarget(
        label="open_industrial_data_sync.groupSourceId",
        path=("variables", "modules", "accelerators", "open_industrial_data_sync", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_OID_SYNC",
        module="accelerators/open_industrial_data_sync",
        description="Open Industrial Data sync — controls access for OID data synchronisation",
    ),
    GroupTarget(
        label="rpt_quality.groupSourceId",
        path=("variables", "modules", "dashboards", "rpt_quality", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_QUALITY",
        module="dashboards/rpt_quality",
        description="Quality reporting dashboard — controls read access for quality metrics",
    ),
    GroupTarget(
        label="cdf_pi.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_pi", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_PI",
        module="sourcesystem/cdf_pi",
        description="PI system connector — controls access for the OSIsoft PI data source",
    ),
    GroupTarget(
        label="cdf_sap_assets.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sap_assets", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SAP_ASSETS",
        module="sourcesystem/cdf_sap_assets",
        description="SAP assets connector — controls access for SAP asset hierarchy ingestion",
    ),
    GroupTarget(
        label="cdf_sap_events.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sap_events", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SAP_EVENTS",
        module="sourcesystem/cdf_sap_events",
        description="SAP events connector — controls access for SAP maintenance event ingestion",
    ),
    GroupTarget(
        label="cdf_sharepoint.groupSourceId",
        path=("variables", "modules", "sourcesystem", "cdf_sharepoint", "groupSourceId"),
        default_env_var="GROUP_SOURCE_ID_SHAREPOINT",
        module="sourcesystem/cdf_sharepoint",
        description="SharePoint connector — controls access for SharePoint document ingestion",
    ),
)

ENTITY_MATCHING_UPDATES: Sequence[tuple[tuple[str, ...], str]] = (
    (
        (
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "targetViewSearchProperty",
        ),
        "aliases",
    ),
    (
        (
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "AssetViewExternalId",
        ),
        "Asset",
    ),
    (
        (
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "TimeSeriesViewExternalId",
        ),
        "Enterprise_TimeSeries",
    ),
    (
        (
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "targetViewExternalId",
        ),
        "Asset",
    ),
    (
        (
            "variables", "modules", "accelerators", "contextualization",
            "cdf_entity_matching", "entityViewExternalId",
        ),
        "Enterprise_TimeSeries",
    ),
)
