"""Canonical Discovery tree node ids."""

from __future__ import annotations

CONNECTION_ROOT = "connection"
CONNECTION_INFO = "connection:info"
DATA_ROOT = "data"
DATA_SAVED_QUERIES = "data:sq"
TRANSFORM_ROOT = "transform"
TRANSFORM_PIPELINE_PREFIX = "transform:pipeline:"
TRANSFORM_TEMPLATES = "transform:templates"
TRANSFORM_TEMPLATE_PREFIX = "transform:template:"
TRANSFORM_SCOPE = "transform:scope"
TRANSFORM_WORKFLOW_PREFIX = "transform:workflow:"
FUSION_ROOT = "fusion"
FUSION_DM_ROOT = "fusion:dm"
FUSION_SPACES = "fusion:spaces"
FUSION_ADMIN = "fusion:admin"
FUSION_GROUPS = "fusion:admin:groups"
FUSION_INTEGRATION_ROOT = "fusion:integration"
GOVERNANCE_ROOT = "gov"
GOVERNANCE_SPACES = "gov:spaces"
GOVERNANCE_GROUPS = "gov:groups"
EXTRACT_ROOT = "extract"
MONITOR_ROOT = "monitor"

# Sibling order under ``connection`` (project info is always last).
CONNECTION_ROOT_CHILD_ORDER = (
    DATA_ROOT,
    FUSION_ROOT,
    GOVERNANCE_ROOT,
    EXTRACT_ROOT,
    TRANSFORM_ROOT,
    MONITOR_ROOT,
    CONNECTION_INFO,
)
