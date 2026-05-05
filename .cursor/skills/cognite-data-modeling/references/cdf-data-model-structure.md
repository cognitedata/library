# CDF Data Model Structure

## Property Limits
- Max **100 properties per container**. Plan ahead — properties cannot be removed from deployed containers.
- Use `additionalProperties` (type: json) as a catch-all for overflow or rarely-queried fields.
- Count properties before adding new ones. If near the limit, evaluate which properties are truly needed.

## Normalization vs Denormalized Access Views
Use CDM/IDM as the normalized semantic base, but design consumer-facing access to be denormalized by default.

### Default Strategy (Strong Recommendation)
- Keep canonical semantics in CDM/IDM (`implements` + `requires`) so model meaning stays standard and interoperable.
- Prefer **one extended, denormalized access view per entity** that includes commonly needed properties for read/use cases.
- Add properties to that extended view instead of creating multiple sibling views for the same entity unless there is a hard requirement.
- Optimize for "easy to read in one query": reduce multi-hop joins for common analytics, search, and application reads.

### When to Split into Separate Views
Only create separate views when one of these is true:
1. Access control requires strict property isolation between audiences
2. Property count/shape limits make a single view impractical
3. Lifecycle/versioning differs significantly between property sets
4. Performance tests show a measurable benefit from separation that indexing/denormalization cannot solve

### Anti-Pattern
- Do not create fragmented "technical" views that force consumers to join many views just to read core business context.


## Extending CDM Types
When extending a Cognite Data Model type (CogniteAsset, CogniteTimeSeries, etc.):

**View**: use `implements` to inherit from the CDM view:
```yaml
implements:
- space: cdf_cdm
  externalId: CogniteAsset
  version: v1
  type: view
```

**Container**: use `requires` constraints to declare the CDM dependency:
```yaml
constraints:
  assetPresent:
    constraintType: requires
    require:
      space: cdf_cdm
      externalId: CogniteAsset
      type: container
```

### Query Optimization via `requires`
CDF uses `requires` constraints to optimize joins at query time. If a view exposes properties from or has relationships to a CDM/IDM type, the underlying **`usedFor: node`** container **must** declare a `requires` constraint on that type's container. This does **not** apply to **`usedFor: record`** containers (they have no `constraints` block).

**Important:** Do **not** add unrelated `requires` constraints just to silence optimization warnings. A `requires` constraint is an ingest-time dependency and will make writes fail unless the required container is also present on the same node.

For example, a file can validly exist without any time series relation. Therefore, do **not** require `cdf_cdm:CogniteTimeSeries` on a file extension container unless file nodes are truly guaranteed to also be time series nodes in your domain (which is usually not the case).

Common required constraints by pattern:

| View implements | Container must require (in addition to direct parent) |
|----------------|------------------------------------------------------|
| `CogniteMaintenanceOrder` | `cdf_cdm:CogniteActivity` (activity is the CDM base) |
| `CogniteNotification` | `cdf_cdm:CogniteActivity` |
| `CogniteOperation` | `cdf_cdm:CogniteActivity` |
| `CogniteDescribable` (view-level) | `cdf_cdm:CogniteDescribable` (container-level) |

Always run `cdf build` or deploy to check for "not optimized for querying" warnings, then add only semantically valid `requires` constraints.

## usedFor: node vs record
- `usedFor: node` — standard entity data (assets, equipment, work orders). Requires a corresponding view. Use **`constraints`** (including `requires` where valid) and **`indexes`** for ingest dependencies, query optimization, and btree-backed filters or reverse relations.
- `usedFor: record` — event/time-stamped record data (alarms, log entries). Does **not** need a view; records are queried directly against the container, not through the data model API. **Omit `constraints` and `indexes`** entirely on record containers — they are not graph nodes; do not model `requires` or btree tuning there. Optional relations (e.g. a `tag` pointer on an alarm row) stay as plain properties only.

## CogniteAsset — Single Implementation Only
Only **one view** in your data model should `implements: CogniteAsset`. Multiple views implementing CogniteAsset causes UI navigation problems in CDF applications (e.g., IndustryCanvas, Asset Explorer). If you need multiple asset-like entities, have one primary asset view implement CogniteAsset and use direct relations from the others.

## IDM Types for Work Management
Prefer implementing the **IDM types** from `cdf_idm` for work order, notification, and operation entities. This gives standardized semantics (`mainAsset`, `type`, `status`, `priority`, etc.) that CDF applications expect.

| Entity | View implements | Container requires | Key IDM properties |
|--------|----------------|-------------------|-------------------|
| WorkOrder | `cdf_idm:CogniteMaintenanceOrder` | `cdf_idm:CogniteMaintenanceOrder` | `mainAsset`, `type`, `status`, `priority`, `priorityDescription` |
| Notification | `cdf_idm:CogniteNotification` | `cdf_idm:CogniteNotification` | `asset`, `type`, `status`, `priority`, `priorityDescription` |
| WorkOrderOperation | `cdf_idm:CogniteOperation` | `cdf_idm:CogniteOperation` | `maintenanceOrder`, `mainAsset`, `phase`, `status`, `sequence`, `mainDiscipline`, `personHours` |

Source these properties from the IDM container (`space: cdf_idm`), not from custom containers.

If `implements` is not used, you must satisfy the minimum mapping requirements in **Minimum CDM/IDM Mapping Without `implements`** so the view is still recognized correctly by CDF clients.

## CDM Properties — Never Duplicate
When a view `implements` a CDM type, properties like `name`, `description`, `tags`, `aliases` (CogniteDescribable), `sourceId`, `source`, `sourceCreatedTime` (CogniteSourceable), and `startTime`, `endTime`, `scheduledStartTime` (CogniteSchedulable) must always reference the CDM container (`space: cdf_cdm`). Never create custom container properties that shadow or duplicate these — always source them from the CDM container.
- Verify canonical CogniteDescribable mappings to avoid copy-paste mistakes: `name -> name`, `description -> description`, `labels/tags -> tags`, `aliases -> aliases`.

## Minimum CDM/IDM Mapping Without `implements`
If you do not use `implements` for CDM/IDM types, map the minimum required CDM/IDM container properties so CDF clients can classify the view correctly.

### Asset Classification (Search/Canvas)
- To be treated as an Asset, map at least one CogniteAsset container property in the view (for example `type`).
- Search detects mapped containers in the view; if the view maps to CogniteAsset container properties, it is treated as an asset and gets the asset icon.

### Time Series Classification
- To be treated as a Time Series, map `isStep` and `type` from CogniteTimeSeries container properties.
- When these mappings are present, the time series icon appears automatically.

### Event Classification (Charts)
- To be treated as an Event and show in Charts, map both `startTime` and `endTime` from CogniteSchedulable container properties and ensure both are populated with data.
- Also map `assets` from CogniteActivity container properties for activity-to-asset context.

### Property ExternalId Stability
- Do not change view property externalIds when mapping container properties.
- You may change the display `name`, but the view property externalId and `containerPropertyIdentifier` should normally match the mapped container property externalId.
- Explicit aliases are allowed when they improve domain clarity or avoid naming collisions (for example `labels` mapped to `CogniteDescribable.tags`). When aliasing, keep `containerPropertyIdentifier` mapped correctly and document the alias intent in the property description.

## Container-View Alignment
- Every `usedFor: node` container should have a corresponding `*.View.yaml` exposing its properties.
- `usedFor: record` containers do **not** need a view — they are not part of the data model's view layer.
- Every container property exposed in a view must use `containerPropertyIdentifier` matching the container's property key.
- Don't expose container properties in a view without a clear use case — views are the query API.
- Every direct relation in a view **must** have a `source` block specifying the target view with `space`, `externalId`, `version`, and `type: view`. This tells the system which view to resolve the referenced node through, enabling typed navigation in the UI.

```yaml
# Required for all direct relation properties in views
source:
  space: "{{space}}"
  externalId: Tag
  version: "{{dm_version}}"
  type: view
```

Only omit `source` when the target is intentionally polymorphic (e.g., `classSpecific` pointing to multiple equipment class views) or when no view exists for the target node type.

For reverse traversals, align forward `source`, reverse host view, and actual stored edge targets (parent vs satellite); details in **`cdf-direct-relations.md`** (*Forward–reverse pairing and anchor view*, NEAT-DMS-CONNECTIONS-REVERSE-009).

## Descriptions
- Every container and view must have a top-level `description`.
- **Every view property must have a `description` field.** Properties without descriptions make the model opaque to AI tools, search engines, and developers. A property named `pi_compDev` is meaningless without "PI compression deviation threshold".
- Keep descriptions concise (one sentence) and domain-specific — not dictionary definitions.
- Container and view descriptions for the same entity should be consistent.
- Top-level description must match the entity represented by `externalId` (avoid cross-entity copy-paste, e.g., a Well container described as a Shortfall event).
- No stray quotes or YAML escaping artifacts in description values.
- Never copy-paste descriptions between views without adapting context (e.g., "tags the time series is related to" is wrong in a Files view).
- Explain abbreviations on first use (e.g., "SPN priority" → "SPN (Safety Priority Number) priority score").
- Reverse relations should describe the relationship direction (e.g., "Notifications that reference this failure mode").
- Top-level view descriptions should be specific about what the view exposes, not generic ("A view that represents information about tags" is bad; "Core tag properties including area, facility, system, and relationships to equipment classes" is good).

## Property Aliasing — `labels` vs `tags`
When the CogniteAsset view is named `Tag`, the inherited `tags` property from CogniteDescribable (text-based labels) creates a naming conflict. Expose it as `labels` in views and always use `labels` in transformations and queries — never `tags`, which will be confused with the Tag view or its direct relations.

## Naming and Semantic Integrity
- Treat property identifiers as long-lived API contracts: avoid typos and accidental renames (for example `isoloations` instead of `isolations`).
- Ensure `sourceCreated*` and `sourceUpdated*` properties have matching names and descriptions (no semantic swaps).
- For any alias mapping, ensure the `description` clearly states the underlying container property to reduce ambiguity for users and AI tools.

## Container-View Property Completeness
- Every property in a `usedFor: node` container **should** be exposed in the corresponding view. Unmapped container properties are invisible to the query API and AI tools.
- When adding properties to a container, always add corresponding view properties in the same change.
- Audit regularly: compare container `properties:` keys against view `containerPropertyIdentifier` values to find gaps.

## CDM Container References — Match the Implemented Type
When a view `implements` a CDM type (e.g., `CogniteFile`) and exposes CDM-defined properties like `assets`, the `container` reference in the view **must** point to the correct CDM container for that type. Common mistake: copy-pasting `CogniteTimeSeries` as the container when the view actually implements `CogniteFile`.

```yaml
# WRONG — Files view using CogniteTimeSeries container
assets:
  container:
    space: cdf_cdm
    externalId: CogniteTimeSeries  # Bug: should be CogniteFile
    type: container
  containerPropertyIdentifier: assets

# CORRECT — container matches the implemented type
assets:
  container:
    space: cdf_cdm
    externalId: CogniteFile
    type: container
  containerPropertyIdentifier: assets
```

Always verify that CDM container references match the `implements` declaration at the top of the view.

### Explicit Verification Rule: Container vs Source
- For CDM-defined properties exposed in a view, the `container` must reference the matching CDM container for the implemented type (e.g., `implements: CogniteAsset` -> `container.externalId: CogniteAsset`; `implements: CogniteFile` -> `container.externalId: CogniteFile`).
- For direct relation properties, the `source` should reference the most specific view in this data model when available (your extended/implemented view), not a generic CDM ancestor view.
- In short: **CDM container for property storage semantics; specific model view for relation navigation semantics.**

## Structural Audit Checklist
When reviewing or modifying a data model, verify:

1. **Property mapping**: Every container property has a corresponding view property (for `usedFor: node` containers)
2. **Descriptions**: Every view property has a `description` field; no copy-paste errors
3. **Direct relation sources**: Every `type: direct` container property exposed in a view has a `source` block (unless polymorphic)
4. **Index coverage**: Every `type: direct` container property has a btree index
5. **Reverse relation validity**: Every `through.identifier` matches an actual property name in the referenced view
6. **No `source` on non-direct**: `source` blocks only appear on properties backed by `type: direct` container properties
7. **Property count**: No container exceeds 100 properties
8. **CDM sourcing**: Properties from CogniteDescribable/CogniteSourceable/CogniteSchedulable reference CDM containers, not custom ones
9. **Top-level fields**: Node containers include `constraints` and `indexes`; record containers omit them; views have `space`, `externalId`, `description`, `version`, `properties`
10. **CDM container match**: Every CDM property's `container.externalId` matches the type declared in `implements` (not copy-pasted from another view)
11. **View-to-data-model coverage**: Every `*.View.yaml` file in the module is referenced by at least one `*.DataModel.yaml` file (no orphaned views)
12. **No unrelated requires**: Every `constraintType: requires` reflects a true entity dependency, not a workaround (e.g., no `Files -> CogniteTimeSeries` unless files are guaranteed to be time series nodes)
13. **Alias clarity**: Any property alias (for example `labels <- tags`) is intentional, documented, and keeps correct `containerPropertyIdentifier` mapping
14. **Naming integrity**: No typo-like property identifiers and no semantic swap between `sourceCreated*` and `sourceUpdated*` fields
15. **Canonical CDM mapping**: CogniteDescribable mappings are semantically correct (`aliases` is not mapped to `description`, etc.)
16. **Description-entity consistency**: Top-level container/view description semantically matches its own `externalId` entity

## Reserved External IDs
Never use these as `externalId` for containers or views — they are reserved by CDF:
`Boolean`, `Date`, `File`, `Float`, `Float32`, `Float64`, `Int`, `Int32`, `Int64`, `JSONObject`, `Mutation`, `Numeric`, `PageInfo`, `Query`, `Sequence`, `String`, `Subscription`, `TimeSeries`, `Timestamp`.

## Required Top-Level Fields
- **`usedFor: node` containers:** `space`, `externalId`, `description`, `properties`, `constraints`, `indexes`, `usedFor`
- **`usedFor: record` containers:** `space`, `externalId`, `description`, `properties`, `usedFor` — do **not** include `constraints` or `indexes`
- **Views:** `space`, `externalId`, `description`, `version`, `properties` (plus `implements` when extending CDM)

## Data Models
- Data model files list all views that form the model's public API.
- Each view entry under `views:` may include **`name`** (human-readable display label). Some validators (e.g. NEAT) flag missing names on these references even when the `*.View.yaml` already defines `name` — keep the data model entry in sync with the view file.
- Every view in a data model file must exist as a `*.View.yaml` file.
- **Every `*.View.yaml` file in the module must be referenced by at least one `*.DataModel.yaml` file.** Orphaned view files are never deployed and silently drift from the live model. When adding a new view, always add a corresponding entry in the data model file. When auditing, compare the set of `*.View.yaml` filenames (minus extension) against all `externalId` values with `space: "{{space}}"` across the module's `*.DataModel.yaml` files — any view file not matched is orphaned.
- Use `{{dm_version}}` for version and `{{space}}` for space to support multi-environment deployment.
- For **view version bumps**, **container migration**, and **uniqueness** constraints, see `cdf-schema-versioning.md`.

## View-Only UX Modules
- In UX/exploration modules that primarily expose views (and rely on containers in other spaces), keep property mappings semantically aligned with the source-domain views.
- Propagate mapping bug fixes across mirrored views (for example `aliases -> aliases`, corrected source metadata labels, and corrected `containerPropertyIdentifier` values) to avoid drift between domain and UX layers.
