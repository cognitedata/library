# CDF Direct Relations & Connections

## Direct Relations in Containers
A `type: direct` property stores a reference to another node. Define with:
```yaml
tag:
  type:
    list: false
    type: direct
  immutable: false
  nullable: true
  autoIncrement: false
  name: tag
```

For multi-valued relations, use `list: true` with `maxListSize`:
```yaml
notifications:
  type:
    list: true
    maxListSize: 2000
    type: direct
  autoIncrement: false
  name: notifications
```

## Exposing Relations in Views
Always specify the `source` view so the system knows the target type:
```yaml
tag:
  container:
    space: "{{space}}"
    externalId: MyContainer
    type: container
  containerPropertyIdentifier: tag
  source:
    space: "{{space}}"
    externalId: Tag
    version: "{{dm_version}}"
    type: view
```

Omitting `source` makes the relation a raw node reference — the UI cannot navigate it as a typed entity. Only omit `source` when the target intentionally has no view.

> **Warning — Polymorphic relations:** When a direct relation can point to multiple different view types (e.g., `classSpecificProperties` in `Tag.View.yaml` pointing to Pump, Valve, Compressor, etc.), omitting `source` is intentional and correct. Adding a `source` would restrict the relation to a single target view. If you encounter a direct relation without `source` during an audit, verify whether it is polymorphic before flagging it as a violation.

**`source` is only valid on `direct` relation properties.** Never add a `source` block to `text`, `int32`, `timestamp`, `boolean`, `json`, or any other non-direct property type. The API will reject it with: _"only direct relation properties can have source defined"_.

## Reverse Relations
Use `multi_reverse_direct_relation` to expose the "other side" of a direct relation without storing redundant data:
```yaml
workOrders:
  source:
    space: "{{space}}"
    externalId: WorkOrder
    version: "{{dm_version}}"
    type: view
  through:
    source:
      space: "{{space}}"
      externalId: WorkOrder
      version: "{{dm_version}}"
      type: view
    identifier: mainAsset
  name: Work orders
  connectionType: multi_reverse_direct_relation
```

Key rules:
- `through.source` is the view that holds the forward relation.
- `through.identifier` must match an actual property name on that view.
- Use `single_reverse_direct_relation` when the reverse is guaranteed to be 1:1.

## Forward–reverse pairing and anchor view (NEAT-DMS-CONNECTIONS-REVERSE-009)

Validators (e.g. NEAT) expect symmetry: if view **A** declares a reverse through view **B**’s property **`p`**, then property **`p`** on **B** must have `source` pointing to **A** — **only when** the container behind **`p`** stores references to instances that are meant to be opened as **A** (same view as the reverse host).

**Satellite / properties pattern:** A forward property may correctly point at a **child or properties** view (e.g. `FunctionalLocationProperties`) because ingest stores `node_reference` targets for that container. Do **not** change `source` to a **parent** view (e.g. `FunctionalLocation`) just to satisfy a reverse declared on the parent: resolution would treat stored ids as the wrong type. Instead:

- Put the reverse on the view that matches the stored target (**B → satellite**), or
- Add a separate forward property and ingest path that references the parent view if you truly need a reverse on **A**.

**Align `source` with transformations:** For each direct relation, confirm instance load SQL (e.g. `cdf_nodes('…', 'SomeContainer', …)` and `node_reference`) targets the same logical entity as the property’s `source` view. A mismatched `source` (copy-paste from another entity) is a common cause of REVERSE-009 noise.

**One reverse host per edge shape:** Avoid defining the same reverse (`through.identifier` on the same forward view) on two different “anchor” views (e.g. parent and satellite) unless two distinct forward properties exist with matching `source` targets.

## Index Direct Relations
Indexing policy for direct relations applies to **`usedFor: node`** containers only. Do **not** add btree indexes on **`usedFor: record`** containers.

Indexing policy for direct relations:
- **MUST** index any direct relation used as `through.identifier` by a reverse relation.
- **MUST** index direct relations used in primary query paths (filters, navigation, graph traversal).
- **SHOULD** index other direct relations that are likely to become queryable.
- **EXCEPTION**: for `direct[]` with `maxListSize > 300`, btree indexes are not supported. In those cases, document the trade-off in container YAML and module docs/README, and provide an alternative query/traversal pattern.
- **CDM/IDM caveat**: if the direct relation is sourced from managed CDM/IDM containers (`cdf_cdm` / `cdf_idm`), container index changes are not available to you; apply this policy to your own containers and treat managed-container gaps as informational.

## Container Reference Consistency
When a view property references a container via `container:` + `containerPropertyIdentifier`, the container **must** match the entity the view represents. Common copy-paste error: a Notification view property accidentally references `CogniteOperation` instead of `CogniteNotification`, or a `source` points to `WorkOrderOperation` instead of `WorkOrder`.

**Verify for every direct relation property in a view:**
1. The `container.externalId` is the correct container for this view's entity (not a container from a different entity)
2. The `containerPropertyIdentifier` exists as a property in that container
3. The `source` view matches the semantic target (e.g., a Notification's `maintenanceOrder` should source to `WorkOrder`, not `WorkOrderOperation`)
4. The `description` matches the view's context (e.g., "The work order the **notification** is related to", not "the **operation** is related to")

> **Real example caught:** `Notification.View.yaml` had `maintenanceOrder` pointing to container `CogniteOperation` with source `WorkOrderOperation` — both wrong. Should have been container `CogniteNotification` with source `WorkOrder`. The description also said "operation" instead of "notification". This was a copy-paste from `WorkOrderOperation.View.yaml`.

## Source Specificity (NEAT-DMS-CONNECTIONS-REVERSE-008)
When a direct relation property's `source` configures a reverse connection in another view, the `source` must point to the **most specific view** in the data model — not a CDM ancestor.

**Problem:** A view inherits or defines a direct relation pointing to a CDM type (e.g., `CogniteTimeSeries`), but the data model has a more specific extension (e.g., `HIOTimeSeries`). The reverse relation in the target view references this property via `through.identifier`, but the forward source still points to the ancestor.

**Fix:** Override the property in the view to point `source` to the specific view:
```yaml
# ❌ BAD — inherited source points to CDM ancestor
timeSeries:
  container:
    space: cdf_cdm
    externalId: CogniteActivity
    type: container
  containerPropertyIdentifier: timeSeries
  source:
    space: cdf_cdm
    externalId: CogniteTimeSeries   # ancestor
    version: v1
    type: view

# ✅ GOOD — overridden source points to specific view in the data model
timeSeries:
  container:
    space: cdf_cdm
    externalId: CogniteActivity
    type: container
  containerPropertyIdentifier: timeSeries
  source:
    space: myspace
    externalId: MyTimeSeries   # specific extension in this data model
    version: v1
    type: view
```

**When to apply:** For every reverse relation (`multi_reverse_direct_relation`) in a view, trace `through.identifier` to the forward property and verify its `source` points to the same view type as the reverse relation's `source`. If the forward property points to an ancestor, add an explicit property override.

> **CDM views are immutable.** If the forward property lives in a `cdf_cdm` view (e.g., `CogniteActivity.equipment`), it cannot be overridden. These produce informational warnings only. Focus on fixing forward properties in your own views.

## Re-sourcing CDM Properties
When a view needs a property that already exists in a CDM container (e.g., `description` in `CogniteDescribable`, `scheduledStartTime` in `CogniteSchedulable`), source it from the CDM container — never duplicate it in a custom container.

The view property can use a different `name` and `description` for domain context while still sourcing from CDM:
```yaml
# ✅ Re-sourced from CDM with domain-specific naming
scheduledStartTimestamp:
  name: scheduledStartTimestamp
  description: Planned start date for executing the operation.
  container:
    space: cdf_cdm
    externalId: CogniteSchedulable
    type: container
  containerPropertyIdentifier: scheduledStartTime
```

Common CDM containers to source from:
- **CogniteDescribable**: `name`, `description`, `tags`, `aliases`
- **CogniteSchedulable**: `startTime`, `endTime`, `scheduledStartTime`, `scheduledEndTime`
- **CogniteSourceable**: `sourceId`, `sourceContext`, `source`, `sourceCreatedTime`, `sourceUpdatedTime`, `sourceCreatedUser`, `sourceUpdatedUser`

**Corollary:** Custom containers should **never** define properties that duplicate CDM container properties (including name variants like `*_name`, `*_description`). If the view `implements` a CDM type, those properties are already available.

## Audit Checklist
When reviewing or adding relationships, verify:
1. **Container property exists**: `type: direct` property in the source container
2. **Index policy satisfied**: btree index exists for MUST paths, or a documented exception exists (`direct[]` with `maxListSize > 300`)
3. **View exposes with `source`**: property in the view has `source` pointing to the correct target view
4. **Reverse relation valid**: target view has a reverse relation with correct `through.identifier`
5. **Container reference correct**: `container.externalId` matches the right container for the entity (not copy-pasted from another view)
6. **`containerPropertyIdentifier` exists**: the referenced property actually exists in the referenced container
7. **`source` view is semantically correct**: the source view matches the intended target entity
8. **Description matches context**: description text refers to the correct entity, not a copy-paste from another view
9. **Source specificity**: direct relation `source` points to the most specific view in the data model, not a CDM ancestor (see NEAT-DMS-CONNECTIONS-REVERSE-008)
10. **Reverse pairing / anchor view**: reverse on view **A** through **B`.`p`** requires **`p`.`source` → A** only if stored refs resolve as **A**; otherwise move the reverse to the view that matches stored targets, or add a dedicated forward property (see NEAT-DMS-CONNECTIONS-REVERSE-009 section above)
11. **No CDM property duplication**: custom containers don't redefine properties available from CDM containers (`CogniteDescribable`, `CogniteSchedulable`, `CogniteSourceable`)
12. **CDM properties re-sourced correctly**: view properties for CDM concepts (name, description, startTime, etc.) source from CDM containers, not custom containers
13. **Canonical describable mapping**: if CogniteDescribable properties are re-sourced, verify `aliases` maps to `aliases` and not another container property by mistake

## Checklist for Adding a New Relationship
1. Add `type: direct` property to the source container
2. Add an index for the property in the same container
3. Expose the property in the source view with `source` pointing to the target view
4. Add a reverse relation in the target view using `multi_reverse_direct_relation`
5. Verify `through.identifier` matches the property name in the source view
