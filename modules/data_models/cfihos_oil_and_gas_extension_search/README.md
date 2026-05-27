# CFIHOS Oil and Gas — Search Solution Model

A search-optimized **solution** data model for oil and gas operations. This module is the solution layer that consumes containers owned by the enterprise module [`cfihos_oil_and_gas_extension`](../cfihos_oil_and_gas_extension/README.md). The two modules are decoupled by design.

## Architecture

This module follows the layering described in `.cursor/skills/cognite-data-modeling/references/cdf-enterprise-vs-solution.md`:

| Layer | Lives in | Pattern |
|-------|----------|---------|
| **Enterprise** containers + views | `cfihos_oil_and_gas_extension` | `implements:` against `cdf_cdm` / `cdf_idm` for canonical CDM/IDM semantics |
| **Search solution** views (this module) | `dm_sol_oil_and_gas_search` | **Mapping** to enterprise containers; `implements: cdf_cdm:CogniteDescribable` only |

Specifically:

- Views in this module **map** to enterprise containers (e.g. `cfihos_oil_and_gas_extension`'s `Tag`, `WorkOrder`, `TimeSeriesData` containers) using `container:` + `containerPropertyIdentifier:`. They do **not** `implements:` enterprise views. This decouples the search model's lifecycle from enterprise view version bumps.
- No view in this module `implements: CogniteAsset`. Asset-hierarchy properties (`parent`, `root`, `path`, `children`) are exposed on the search `Tag` view by referencing the `cdf_cdm:CogniteAsset` container directly and self-referencing within this space, so search-side traversal stays inside the search model. Use the enterprise `Tag` view for canonical `CogniteAsset` semantics in Asset Explorer / Industry Canvas.
- All reverse direct relations whose **forward** relation lives on a solution-shaped view (`WorkOrder.assets`, `Notification.assets`, `TimeSeriesData.assets`, …) live on the matching view here (`Tag.workOrders`, `Tag.notifications`, `Tag.timeSeries`, …) — not on the enterprise side.
- View externalIds in this module (`Tag`, `Equipment`, `WorkOrder`, `Notification`, …) intentionally match the enterprise externalIds. Because the views live in a different space (`dm_sol_oil_and_gas_search` vs. `dm_dom_oil_and_gas`), this is not a collision; it gives consumers consistent names whether they read the enterprise or search model.
- Both modules write/read instances in the **shared instance space** (`inst_location`). External IDs are stable across modules.

### Why the model is split into enterprise + search

The model is delivered as **two modules** that version independently:

| Module | Space | Role |
|--------|-------|------|
| `cfihos_oil_and_gas_extension` | `dm_dom_oil_and_gas` | Owns containers, indexes, and the canonical CDM/IDM-implementing views. Treated as the durable contract. |
| `cfihos_oil_and_gas_extension_search` (this one) | `dm_sol_oil_and_gas_search` | Maps to the enterprise containers. Hosts solution-shaped reverse relations. Free to bump versions independently. |

Reasons for the split (per `cdf-enterprise-vs-solution.md`):

1. **Containers are the durable contract.** Search views map to enterprise *containers* rather than `implements:`-ing enterprise *views*, so this model can re-shape and re-version without forcing enterprise consumers to migrate.
2. **Reverse relations live with their forward.** Forward direct relations on solution-shaped views (`WorkOrder.assets`, `Notification.assets`, `TimeSeriesData.assets`, …) belong to this model, so the matching reverses (`Tag.workOrders`, `Tag.notifications`, `Tag.timeSeries`, …) live on this side's `Tag` view — not on the enterprise `Tag`.
3. **Single `CogniteAsset` per data model.** Each data model that needs asset semantics defines its own single `CogniteAsset` implementer. The enterprise `Tag` is the enterprise one; this module exposes asset-hierarchy properties (`parent`, `root`, `path`, `children`) on its own `Tag` view, self-referencing within this space, without `implements: CogniteAsset` on the search side.
4. **Same externalIds in different spaces are intentional.** Both modules expose views named `Tag`, `Equipment`, `WorkOrder`, etc. They live in different spaces, so there is no collision; this gives consumers consistent names whether they read the enterprise or search model.
5. **Independent lifecycles.** `dm_version` (enterprise) and `search_dm_version` (search) bump separately so a search-side change never forces an enterprise version bump, and vice versa.

## Module structure

```
cfihos_oil_and_gas_extension_search/
├── default.config.yaml          # search_space, search_dm_version, enterprise_space refs
├── module.toml
├── data_modeling/
│   ├── sp_dm_sol_oil_and_gas_search.Space.yaml
│   ├── dm_sol_search_oil_and_gas.DataModel.yaml
│   └── views/
│       ├── SearchTag.View.yaml
│       ├── SearchEquipment.View.yaml
│       ├── SearchTimeSeriesData.View.yaml
│       ├── SearchFiles.View.yaml
│       ├── SearchWorkOrder.View.yaml
│       ├── SearchWorkOrderOperation.View.yaml
│       ├── SearchNotification.View.yaml
│       ├── SearchFailureMode.View.yaml
│       ├── SearchFunctionalLocation.View.yaml
│       ├── SearchFunctionalLocationProperties.View.yaml
│       ├── SearchMaintenanceAndIntegrity.View.yaml
│       └── SearchCommonLCIProperties.View.yaml
└── locations/
    └── Cfihos_OG_Search.LocationFilter.yaml
```

## Versioning policy

- `search_dm_version` (in `default.config.yaml`) bumps **independently** from `dm_version` in the enterprise module. Bump it for any breaking change to a `Search*` view.
- `enterprise_space` and `enterprise_dm_version` are **read-only references**; never modify enterprise containers from this module.
- Container changes belong in the enterprise module. If you need a new property surfaced for search, add it to the enterprise container first, then add a corresponding mapping in the relevant `Search*` view.

## Consumers

Maintain this list. Update in the same PR that bumps `search_dm_version`. See `cdf-enterprise-vs-solution.md` §10.

| Consumer | Pinned version | Owner | Notes |
|----------|---------------|-------|-------|
| _(none yet)_ | — | — | Add entries as apps adopt this model. |

When deprecating a `Search*` view, mark it in the view `description` with `[DEPRECATED — replaced by …]` and keep it deployed for a minimum 90-day window.

## Pending work — phase 2

The 17 CFIHOS equipment-class views (`Pump`, `Valve`, `Compressor`, `HeatExchanger`, etc.) in the enterprise module do **not** yet have search-side mirrors. They are accessed in this model via the polymorphic `Tag.classSpecificProperties` relation (which has no `source` block — points to whichever class container the tag belongs to). Adding `Pump`, `Valve`, etc. as mapped views in this space is a follow-up.

The 18 CFIHOS class containers in the enterprise module also have **no btree indexes** yet. Adding a minimal index baseline (e.g. on identifier-like properties used in queries) is part of the same follow-up.

## Deployment

### Prerequisites

- **Cognite Toolkit 0.7.210 or above** (`cdf --version` to check).
- A CDF project with valid authentication configured for your target environment.
- A `cdf.toml` in your Toolkit project directory.

### Choose your setup path

### 1. Existing Toolkit project

If you already have a Toolkit project, ensure your `cdf.toml` uses the official library URL:

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

In the same `cdf.toml`, ensure deployment packs are enabled:

```toml
[alpha_flags]
deployment-pack = true
```

Then add this module:

```bash
cdf modules add -d cfihos_oil_and_gas_extension_search
```

Build and deploy:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

### 2. Starting from scratch

In an empty directory:

```bash
cdf modules init .
```

In the interactive selector:

1. Choose **Data models**.
2. Use **Space** to select **cfihos_oil_and_gas_extension_search**.
3. Press **Enter**.

Then run:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

Deploy the enterprise module **first**, then this module. The search module depends on enterprise containers existing.
