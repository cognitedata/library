---
name: cognite-data-modeling
description: CDF data model design patterns for containers, views, and CDM/IDM extensions in YAML. Use when creating or modifying data model YAML files, extending CogniteCore or IDM types, designing containers and views, adding indexes or direct relations, auditing data models, or deploying via Cognite Toolkit.
---

# CDF Data Modeling

Read this skill first for orientation, then read the relevant reference file:
- `references/cdf-data-model-structure.md` -- containers, views, CDM/IDM extensions, structure conventions, audit checklist
- `references/cdf-data-model-indexes.md` -- indexing best practices, btree restrictions, reverse relation indexing
- `references/cdf-direct-relations.md` -- direct relation patterns, reverse relations, source specificity, forward–reverse pairing (REVERSE-009)
- `references/cdf-schema-versioning.md` -- view version bumps, data model `name` on view refs, container change safety

For querying and fetching data from a model, use the `cognite-data-fetching` skill instead.

## 1. The Three-Layer Model

| Layer | YAML file type | Purpose |
|-------|---------------|---------|
| **Container** | `*.Container.yaml` | Physical property storage; defines types; `usedFor: node` adds constraints and indexes for graph/query semantics |
| **View** | `*.View.yaml` | Logical query API; maps container properties, declares relations |
| **Data Model** | `*.DataModel.yaml` | Public API surface; lists which views are exposed |

Every `usedFor: node` container needs a matching view. Every view needs an entry in a data model file. `usedFor: record` containers are queried directly — no view needed. **Do not** add `constraints` or `indexes` on `usedFor: record` containers (no graph-node ingest rules or btree tuning for that shape).

## 2. Extending CDM/IDM Types

For **`usedFor: node`** containers: extend via `implements` in the view and `requires` in the container. **`usedFor: record`** containers skip `constraints` / `requires` (see §1).

```yaml
# View — inherit semantics
implements:
  - space: cdf_cdm
    externalId: CogniteAsset
    version: v1
    type: view

# Container — declare ingest-time dependency
constraints:
  assetPresent:
    constraintType: requires
    require:
      space: cdf_cdm
      externalId: CogniteAsset
      type: container
```

**CDM properties are never duplicated.** `name`, `description`, `tags`, `aliases`, `sourceId`, `startTime`, `endTime` etc. always map to CDM containers (`cdf_cdm`), never to custom containers.

## 3. Key Design Rules

- **One CogniteAsset implementation per data model** — multiple views implementing CogniteAsset breaks CDF UI navigation.
- **Max 100 properties per container** — plan ahead; properties cannot be removed after deployment.
- **Max 20 indexes per container** (`usedFor: node` only) — index strategically (see `cdf-data-model-indexes.md`).
- **Every direct relation in a view must have a `source` block** — omit only for intentionally polymorphic relations.
- **Every view property must have a `description`** — unnamed properties are opaque to AI tools and search.
- **Use `{{space}}` and `{{dm_version}}` template variables** — never hardcode space or version values.

## 4. IDM Work Management Types

Prefer `cdf_idm` types for work orders, notifications, and operations:

| Entity | View implements | Key properties |
|--------|----------------|----------------|
| WorkOrder | `cdf_idm:CogniteMaintenanceOrder` | `mainAsset`, `type`, `status`, `priority` |
| Notification | `cdf_idm:CogniteNotification` | `asset`, `type`, `status`, `priority` |
| WorkOrderOperation | `cdf_idm:CogniteOperation` | `maintenanceOrder`, `mainAsset`, `phase`, `status` |

## 5. Quick Audit Checklist

Before deploying or reviewing a data model:
- [ ] Every `usedFor: node` container property is exposed in the view
- [ ] Every view property has a `description`
- [ ] Every `type: direct` property has a btree index (unless `maxListSize > 300`)
- [ ] Every `type: direct` in a view has a `source` block (unless polymorphic)
- [ ] Every `through.identifier` in a reverse relation matches an actual indexed property
- [ ] Forward `source` on that property matches the view type instances actually store (see `cdf-direct-relations.md` — reverse pairing and ingest alignment)
- [ ] No container exceeds 100 properties or 20 indexes
- [ ] Every `*.View.yaml` appears in a `*.DataModel.yaml` (no orphaned views)
- [ ] CDM property containers match the `implements` declaration (no copy-paste mismatch)
- [ ] No `requires` constraints added for unrelated containers
- [ ] `usedFor: record` containers have no `constraints` or `indexes` sections

## 6. Schema, versioning, and validators

- **View `version`:** For breaking view changes, bump `version` in the view YAML **and** update every other view / data model that references that view’s `version` (see `cdf-schema-versioning.md`).
- **Data model `views:`:** Only `space`, `externalId`, `version`, `type` — no extra fields (Toolkit warns on e.g. `name`). Display names belong on `*.View.yaml`.
- **Containers:** Prefer additive schema changes; property removal and type / `usedFor` changes on deployed containers need a documented migration path — do not treat containers as freely mutable.
- **Broader architecture:** Source → Enterprise → Solution layering, instance-space strategy, and access-control matrices are org-specific. This skill stays focused on **Toolkit YAML** correctness; pull those patterns from your enterprise data-model playbook when scoping a full program.

## 7. When to Check Docs

Use the `SearchCogniteDocs` MCP tool for:
- Specific CDM/IDM property lists and container schemas
- Cognite Toolkit YAML syntax and deployment commands
- New CogniteCore versions or IDM type additions
- `cdf build` warning messages and how to resolve them
