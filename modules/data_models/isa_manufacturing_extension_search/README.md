# ISA Manufacturing — Search Solution Model

A search-optimized **solution** data model for ISA-95/ISA-88 manufacturing operations. This module is the solution layer that consumes containers owned by the enterprise module [`isa_manufacturing_extension`](../isa_manufacturing_extension/README.md). The two modules are decoupled by design.

## Architecture

This module follows the layering described in `.cursor/skills/cognite-data-modeling/references/cdf-enterprise-vs-solution.md`:

| Layer | Lives in | Pattern |
|-------|----------|---------|
| **Enterprise** containers + views | `isa_manufacturing_extension` | `implements:` against `cdf_cdm` for canonical CDM semantics |
| **Search solution** views (this module) | `dm_sol_isa_manufacturing_search` | **Mapping** to enterprise containers; `implements: cdf_cdm:CogniteDescribable` only |

Specifically:

- Views in this module **map** to enterprise containers (e.g. `isa_manufacturing_extension`'s `Batch`, `WorkOrder`, `Equipment` containers in `dm_dom_isa_manufacturing`) using `container:` + `containerPropertyIdentifier:`. They do **not** `implements:` enterprise views. This decouples the search model's lifecycle from enterprise view version bumps.
- No view in this module `implements: CogniteAsset`, `CogniteFile`, `CogniteTimeSeries`, or `CogniteSourceable`. Asset-hierarchy properties (`parent`, `root`, `path`, `children`) are exposed on the search `ISAAsset` view by referencing the `cdf_cdm:CogniteAsset` container directly and self-referencing within this space. `ISAFile` and `ISATimeSeries` expose CDM file/time-series properties the same way via explicit `CogniteFile` / `CogniteTimeSeries` container mappings. Use the enterprise views for canonical CDM semantics in Asset Explorer / Industry Canvas.
- All reverse direct relations whose **forward** relation lives on a solution-shaped view (`WorkOrder.assets`, `WorkOrder.equipment`, `ISATimeSeries.assets`, `Equipment.activities`, …) live on the matching view here (`ISAAsset.activities`, `ISAAsset.timeSeries`, `Equipment.activities`, …) — not on the enterprise side.
- View externalIds in this module (`ISAAsset`, `Equipment`, `WorkOrder`, `Batch`, …) intentionally match the enterprise externalIds. Because the views live in a different space (`dm_sol_isa_manufacturing_search` vs. `dm_dom_isa_manufacturing`), this is not a collision; it gives consumers consistent names whether they read the enterprise or search model.
- Both modules write/read instances in the **shared instance space** (`inst_isa_manufacturing`). External IDs are stable across modules.

### Why the model is split into enterprise + search

The model is delivered as **two modules** that version independently:

| Module | Space | Role |
|--------|-------|------|
| `isa_manufacturing_extension` | `dm_dom_isa_manufacturing` | Owns containers, indexes, and the canonical CDM-implementing views. Treated as the durable contract. |
| `isa_manufacturing_extension_search` (this one) | `dm_sol_isa_manufacturing_search` | Maps to the enterprise containers. Hosts solution-shaped reverse relations. Free to bump versions independently. |

Reasons for the split (per `cdf-enterprise-vs-solution.md`):

1. **Containers are the durable contract.** Search views map to enterprise *containers* rather than `implements:`-ing enterprise *views*, so this model can re-shape and re-version without forcing enterprise consumers to migrate.
2. **Reverse relations live with their forward.** Forward direct relations on solution-shaped views (`WorkOrder.assets`, `WorkOrder.equipment`, `ISATimeSeries.assets`, …) belong to this model, so the matching reverses (`ISAAsset.activities`, `Equipment.activities`, `ISAAsset.timeSeries`, …) live on this side — not on the enterprise model.
3. **Single `CogniteAsset` per data model.** Each data model that needs asset semantics defines its own single `CogniteAsset` implementer. The enterprise `ISAAsset` is the enterprise one; this module exposes asset-hierarchy properties (`parent`, `root`, `path`, `children`) on its own `ISAAsset` view, self-referencing within this space, without `implements: CogniteAsset` on the search side.
4. **Same externalIds in different spaces are intentional.** Both modules expose views named `ISAAsset`, `Equipment`, `WorkOrder`, etc. They live in different spaces, so there is no collision; this gives consumers consistent names whether they read the enterprise or search model.
5. **Independent lifecycles.** `enterprise_dm_version` (enterprise) and `search_dm_version` (search) bump separately so a search-side change never forces an enterprise version bump, and vice versa.

## Module structure

```
isa_manufacturing_extension_search/
├── default.config.yaml          # search_space, search_dm_version, enterprise_space refs
├── module.toml
├── data_modeling/
│   ├── dm_sol_isa_manufacturing_search.Space.yaml
│   ├── dm_sol_search_manufacturing.DataModel.yaml
│   └── views/
│       ├── SearchArea.View.yaml
│       ├── SearchBatch.View.yaml
│       ├── SearchControlModule.View.yaml
│       ├── SearchEnterprise.View.yaml
│       ├── SearchEquipment.View.yaml
│       ├── SearchEquipmentModule.View.yaml
│       ├── SearchISAAsset.View.yaml
│       ├── SearchISAFile.View.yaml
│       ├── SearchISATimeSeries.View.yaml
│       ├── SearchMaterial.View.yaml
│       ├── SearchMaterialLot.View.yaml
│       ├── SearchOperation.View.yaml
│       ├── SearchPersonnel.View.yaml
│       ├── SearchPhase.View.yaml
│       ├── SearchProcedure.View.yaml
│       ├── SearchProcessCell.View.yaml
│       ├── SearchProcessParameter.View.yaml
│       ├── SearchProductDefinition.View.yaml
│       ├── SearchProductRequest.View.yaml
│       ├── SearchProductSegment.View.yaml
│       ├── SearchQualityResult.View.yaml
│       ├── SearchRecipe.View.yaml
│       ├── SearchSite.View.yaml
│       ├── SearchUnit.View.yaml
│       ├── SearchUnitProcedure.View.yaml
│       └── SearchWorkOrder.View.yaml
└── locations/
    └── isaManufacturingSearch.LocationFilter.yaml
```

This module owns **views only** — no containers. All storage and btree indexes live in the enterprise module.

## Versioning policy

- `search_dm_version` (in `default.config.yaml`) bumps **independently** from `enterprise_dm_version` in the enterprise module. Bump it for any breaking change to a `Search*` view.
- `enterprise_space` and `enterprise_dm_version` are **read-only references**; never modify enterprise containers from this module.
- Container changes belong in the enterprise module. If you need a new property surfaced for search, add it to the enterprise container first, then add a corresponding mapping in the relevant `Search*` view.

## Consumers

Maintain this list. Update in the same PR that bumps `search_dm_version`. See `cdf-enterprise-vs-solution.md` §10.

| Consumer | Pinned version | Owner | Notes |
|----------|---------------|-------|-------|
| _(none yet)_ | — | — | Add entries as apps adopt this model. |

When deprecating a `Search*` view, mark it in the view `description` with `[DEPRECATED — replaced by …]` and keep it deployed for a minimum 90-day window.

## Pending work — phase 2

All 26 ISA Manufacturing enterprise views have search-side `Search*` mirrors in v1. Unlike the CFIHOS search model (which defers equipment-class views to phase 2), this module ships with full ISA-95/ISA-88 entity coverage. Remaining follow-ups:

- **Consumer onboarding:** Pin apps in the Consumers table above as they adopt `dm_isa_manufacturing_domain_model_search`.
- **`ISAAsset.asset_specific` connection typing:** Neat reports a missing end node type on this polymorphic relation; define the value type when consumers need typed traversal beyond the enterprise container mapping.
- **Enterprise index baseline:** Btree indexes on direct-relation properties (including list relations with `maxListSize`) are maintained in `isa_manufacturing_extension`; search views inherit storage via container mapping.

## Deployment

Deploy the enterprise module **first**, then this module. The search module depends on enterprise containers existing in CDF.

```bash
# 1. Build and deploy enterprise containers/views (required before search Neat validation passes)
cdf build -c config.dev.yaml
cdf deploy --env dev   # with only isa_manufacturing_extension selected, or deploy enterprise resources first

# 2. Build and deploy search (requires dm_dom_isa_manufacturing:* containers in CDF)
cdf build -c config.dev.yaml
cdf deploy --env dev
```

### Neat build validation

`cdf build` runs Neat on each data model. The search model maps to enterprise containers in `dm_dom_isa_manufacturing`, which Neat resolves from CDF in rebuild mode — not from the local build output. If you see `EnumerationMissingName: Container dm_dom_isa_manufacturing:Area not found`, the enterprise containers are not in your CDF project yet (common after a space rename). Deploy `isa_manufacturing_extension` first, then rebuild with both modules selected.
