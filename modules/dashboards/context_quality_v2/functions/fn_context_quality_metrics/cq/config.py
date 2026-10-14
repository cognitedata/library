"""Run configuration. Nothing project-specific lives in code outside this dict."""

from copy import deepcopy
from typing import Any

from cq.types import RunConfig

DEFAULT_CONFIG: RunConfig = {
    # The resources a run reads and writes are environment-specific, so there is no default: the deploying environment
    # (the workflow task's config) names them, and require_resources() rejects a run that does not.
    "stream": "",
    "schemaSpace": "",
    "instanceSpace": "",
    # The container names are part of the run log contract, not of an environment, so they keep their defaults.
    "relationshipContainer": "ContextQualityRelationship",
    "fileAnnotationContainer": "ContextQualityFileAnnotation",
    # A facility is an instance space. By default they are discovered from where the entity nodes live.
    # Set facilitySpaces ({space: facility id}) to use an explicit allowlist instead of discovery.
    "facilitySpaces": {},
    # Optional display ids for discovered spaces ({space: facility id}); a space without an entry uses its own id.
    "facilityNames": {},
    # Discovery ignores these spaces, and any space holding fewer entity nodes than the minimum
    # (this keeps a shared root or reference space from showing up as a facility).
    "excludeSpaces": ["cdf_cdm", "cdf_idm", "cdf_cdm_units", "cdf_extraction_extensions", "cdf_apps_shared"],
    "minFacilityEntities": 10,
    # asset tier from the CogniteAssetType code ({code: 1, 2 or 3}). Empty by default: tiers are project-specific, an
    # asset whose type is not listed has no tier, and without a mapping the tier coverage metrics are not measured.
    "tierByTypeCode": {},
    # The mappings below are project-specific and empty by default; an empty one means the rule is not applied.
    # discipline from the CogniteAssetType code ({code: discipline id}); without it no asset has a discipline
    "disciplineByTypeCode": {},
    # equipment type code -> asset type code it should sit under (type consistency)
    "assetTypeByEquipmentTypeCode": {},
    # a tag with this prefix gives an asset its division (the rest of the tag); empty means no division is derived
    "divisionTagPrefix": "",
    # assets carrying this tag are retired (children still active under one are reported); empty switches it off
    "retiredTag": "",
    # data owner by name / externalId prefix (first match wins), per dimension default otherwise
    "ownerPrefixRules": [],
    "defaultOwners": {},
    "exclusions": {
        "syntheticTsPrefixes": ["CALC-", "VIRTUAL-", "SYNTH-"],
        "locationCentricFileCategories": ["safety_log", "procedure"],
        "excludedFileExtensions": [".png"],
        # equipment carrying any of these tags is decommissioned and excluded from the equipment metrics
        "decommissionedTags": [],
    },
    "freshnessDays": 30,
    "maxDepth": 8,
    "lowConfidence": 0.5,
    "highConfidence": 0.9,
    "drilldownCap": 500,
    "batchSize": 500,
}


EXCLUSION_LISTS = (
    "syntheticTsPrefixes",
    "locationCentricFileCategories",
    "excludedFileExtensions",
    "decommissionedTags",
)


TIERS = (1, 2, 3)
RESOURCE_KEYS = ("stream", "schemaSpace", "instanceSpace")


def require_resources(cfg: RunConfig, *, writes: bool) -> None:
    """Rejects a run whose resource identifiers are not configured, before anything is read or written.

    The two spaces are needed by every run: the metrics spaces are left out of facility discovery, and they are where
    the rows go. The stream is only written by a real compute_metrics run, so a dry run or validate does not need it.
    """
    needed = RESOURCE_KEYS if writes else ("schemaSpace", "instanceSpace")
    missing = [key for key in needed if not isinstance(cfg[key], str) or not cfg[key].strip()]  # type: ignore[literal-required]
    if missing:
        raise ValueError(
            f"Missing configuration: {', '.join(missing)}. Set them in the task's config to the stream and spaces "
            "this deployment uses; there are no defaults."
        )


def _check_tiers(mapping: Any) -> None:
    """Rejects a tierByTypeCode that is not {type code: 1, 2 or 3}. Nothing is coerced: "1", 1.0, True and null all fail."""
    if not isinstance(mapping, dict):
        raise ValueError(f"tierByTypeCode must be a mapping of type code to tier, not {mapping!r}")
    for code, tier in mapping.items():
        if not isinstance(code, str) or not code:
            raise ValueError(f"tierByTypeCode keys must be non-empty type codes, not {code!r}")
        if type(tier) is not int or tier not in TIERS:
            raise ValueError(f"tierByTypeCode[{code!r}] must be a tier of 1, 2 or 3, not {tier!r}")


def build_config(overrides: dict[str, Any] | None) -> RunConfig:
    """The defaults with the payload's overrides applied.

    An override replaces the default it names, whatever its type: a mapping or list is never merged into the default
    one, so an explicitly empty mapping clears the default rules. Keys the payload does not name keep their defaults,
    and the result shares nothing with DEFAULT_CONFIG or with the overrides.

    `exclusions` is a mapping of four lists. The metric code reads all four, so a supplied `exclusions` mapping
    replaces the whole default and any of the four lists it does not name is empty.

    `tierByTypeCode` is validated here, so a bad tier fails before anything is read or written.

    The old scalar `decommissionedTag` is no longer a setting. Honouring it next to `exclusions.decommissionedTags` would
    need a precedence rule, and any such rule lets it undo an explicitly empty `exclusions`, so it is rejected.
    """
    if "decommissionedTag" in (overrides or {}):
        raise ValueError(
            "The setting decommissionedTag was replaced by exclusions.decommissionedTags (a list of tags)."
        )
    _check_tiers((overrides or {}).get("tierByTypeCode", {}))
    prefix = (overrides or {}).get("divisionTagPrefix", "")
    if not isinstance(prefix, str):
        raise ValueError(f"divisionTagPrefix must be text, not {prefix!r}")
    cfg: dict[str, Any] = dict(deepcopy(DEFAULT_CONFIG))
    for key, value in (overrides or {}).items():
        cfg[key] = deepcopy(value)
    if isinstance(cfg.get("exclusions"), dict):
        for name in EXCLUSION_LISTS:
            cfg["exclusions"].setdefault(name, [])
    return cfg  # type: ignore[return-value]
