# Industry Domain Extension — Agent adaptation contract (SPEC)

> Extracted from `industry_domain_extension.PLAN.md` §8. Start at **How to extend** when wiring this template to a real project.

## 8. Agent adaptation contract (`SPEC.md`, summarized in README)

The cog-vd `cdf-data-modeling` skill (W1a) tells agents to read the **README** first, so each data model README
includes *How to extend* (§8.0 condensed), *Stable contract vs scaffolding* (8.1 condensed) and *Adapting this
template* linking `SPEC.md` and the commands in §9. `SPEC.md` holds the full contract below. **Start at §8.0**
when wiring this template to a real project — everything else in §8 is reference for that procedure.

### 8.0 How to extend — replace demo with real sources

This template ships **demo** RAW tables, sample rows, scaffold property names and example values so the pack runs
end to end. Adopting it means swapping that demo surface for the project's real extractors and field names
**before the first production deploy** (after that, container properties cannot be removed — §8.7).

#### Markers agents must grep

| Marker | Where | Meaning |
|--------|-------|---------|
| `SOURCE_SCAFFOLD:` | Property `description` on containers and views | Demo / illustrative property — rename, replace or delete to match the source |
| `CONTRACT:` | Property / view descriptions | Keep; change only with model-owner approval |
| `raw_sample_all_*` / `*_industry_data_dump` | Config, RAW, transformations, workflow | Demo source modules — replace with project RAW / extractors |
| `Example:` in descriptions | Same properties as `SOURCE_SCAFFOLD` | Synthetic illustration only — not a real code list |

```bash
# Inventory every scaffold property (from a Toolkit / library checkout)
rg -n "SOURCE_SCAFFOLD:" modules/datamodels/industry_domain_extension modules/datamodels/industry_domain_extension_search
rg -n "raw_sample_all_|industry_data_dump" modules/sourcesystem modules/common/cdf_industry_ingestion
```

#### Ordered procedure (do not skip)

1. **Inventory the demo surface.** Run the greps above. List every `SOURCE_SCAFFOLD` property (§5.1 table) and every
   demo RAW table / transformation / workflow task (§8.9 touch map, §12).
2. **Map real sources → template.** For each real source table or API entity, fill the worksheet (§8.2):
   source system → source field → target view → container property → action (`keep` / `replace` / `drop`).
   Confirm the table with the developer before editing YAML (cog-vd W1a).
3. **Trim unused types first.** If a demo entity has no real source and no consumer, remove it with §8.6 + the
   touch-map row (§8.9) — container, views, DataModel entries, Search reverse, transformation, workflow task, RAW
   table. Free now; hard later.
4. **Swap scaffold properties for source fields.** For each `replace` / `drop` row:
   - **drop** — delete the property from the enterprise container + view, the Search view map, indexes, and the
     SQL / Function that wrote it.
   - **replace (rename)** — use §8.10: same touch-map row; keep one camelCase id everywhere (container, view,
     search, SQL alias, `through.identifier`).
   - **replace (new meaning)** — prefer renaming the scaffold id to the source name when it is still pre-prod;
     do not leave a dead demo property beside the real one.
   - Never invent a custom property that duplicates CDM/IDM (§8.4); map to the CDM/IDM field instead.
5. **Point ingestion at real RAW.** Change transformation `source` databases/tables and column lists to the
   project extractors. Update `config.<env>.yaml` RAW database variables. Remove or stop deploying the
   `*_industry_data_dump` sample modules when real data is connected (§8.1 Sample data row).
6. **Extend only for gaps.** If the source has fields with no template home and no CDM/IDM equivalent:
   - Prefer a new property on the **existing adopter-owned** enterprise container (still pre-prod), tagged
     without `SOURCE_SCAFFOLD` once it is production-shaped.
   - After first prod deploy, add properties only on a **new extension container** mapped into the view
     (cog-vd W1a step 5 / W2) — never edit CDM/IDM containers.
   - New entity type → §8.5; new high-volume event type → §5.11 (records), not nodes.
7. **Align Search `_SOL`.** Update or remove Search views and reverses for every container you kept, renamed,
   trimmed or added. Search stays schema-only (no containers, no transforms).
8. **Drop demo values from docs and examples.** Rewrite property descriptions: remove `SOURCE_SCAFFOLD:` and
   synthetic `Example: PLANT01`-style values; use real domain wording and a non-sensitive example from the
   source. Update README *Adapting this template* with the filled §8.2 worksheet.
9. **Validate.** Run §9 (naming, structure, relations, build, dry-run). Confirm instance / record counts against
   the source after the first non-demo deploy.

#### Done when

- [ ] `rg SOURCE_SCAFFOLD:` returns no hits in the adopted modules (or only properties still intentionally
      temporary, listed in the README)
- [ ] No transformation or Function still reads `raw_sample_all_*` in the target environment
- [ ] §8.2 worksheet is filled and checked into the project (README or `SPEC.md`)
- [ ] §9 gate is green

#### Do not

- Rename or remove `CONTRACT` wiring (`implements`, typed `source`, CDM/IDM maps) without approval
- Leave demo RAW modules enabled beside production extractors writing the same nodes
- Add source-system or site names into permanent identifiers (spaces, view ids, property ids) — put those in
  config / instance data only (§8.8, §9.1)

### 8.1 Stable vs scaffolding

| Kind | Examples | Agent action |
|------|----------|--------------|
| CONTRACT | `implements`, `requires`, typed `source` on CDM/IDM directs, DataModel membership of core types, search module = schema only | Keep; change only with model-owner approval |
| SOURCE_SCAFFOLD | `site`, `documentNumber`, `historianTagName`, `workOrderNumber`, `failureModeCode`, … | Rename/replace/delete to match the source before first prod deploy |
| CUSTOM type | Material, OperatingLimit | Keep pattern (no Describable); replace props freely |
| RECORD type | `AlarmEvent` container + record view + stream | Keep `usedFor: record`, no constraints/indexes, `streamId`; rename/replace scaffold props **before** the first record is ingested |
| Sample data | `*_industry_data_dump` modules, `cdf_industry_ingestion` | Replace with real extractors/RAW before production |

The enterprise containers are **adopter-owned scaffolding**, not an upstream contract: adapting them in place before
the first production deploy satisfies W1a step 5. After that, new properties go on a new extension container.

### 8.2 Mapping worksheet (fill per adoption)

| Source system | Source field | Target view | Container property | Action (keep / replace / drop) |
|---------------|--------------|-------------|--------------------|--------------------------------|
| | | | | |

Fill one row per source field during §8.0 step 2. §12.3 is a filled-in example for the **demo** sample data —
replace those rows with the real mapping; do not ship the demo worksheet as the project contract.

### 8.3 Replacement recipes

| Entity | Typical sources | What to edit |
|--------|-----------------|--------------|
| Tag | ERP functional locations, tag register, asset registry | `Tag` scaffold props; keep CogniteAsset hierarchy (`parent` written by ingest) |
| Equipment | ERP / EAM equipment master | `modelNumber`; map class/code to CogniteEquipmentType nodes; use CDM `manufacturer` / `serialNumber` |
| Document | Document management, SharePoint, engineering systems | Document scaffolds; document type → CogniteFileCategory nodes; keep CogniteFile maps |
| TimeSeriesData | Historian, OPC UA, SCADA, IoT platform | Replace/remove `historian*`; add source-family props (e.g. `opcUaNodeId`); keep CogniteTimeSeries maps; leave CDM `unit` for UoM |
| WorkOrder / Operation / Notification | ERP / EAM work management | Keep IDM implements; map status/type/priority/schedule to IDM properties; custom props only for what IDM lacks |
| Material / OperatingLimit | MRO catalog, operating envelopes | Custom props only; never add Describable |

### 8.4 Collision blacklist (views that `implements` CDM/IDM)

Never use these as **custom** property ids on a view that `implements` a CDM/IDM type (or a container that type
requires) — they already exist there. Label-only custom types without `implements` (Material, OperatingLimit) may
use `name`, `asset`, `equipment` and `timeSeries` as their own direct relations.

`name`, `description`, `tags`, `aliases`, `sourceId`, `source`, `sourceContext`, `sourceCreatedTime`,
`sourceUpdatedTime`, `sourceCreatedUser`, `sourceUpdatedUser`, `unit`, `sourceUnit`, `status`, `type`, `priority`,
`priorityDescription`, `phase`, `sequence`, `mainDiscipline`, `numberOfMainDiscipline`, `personHours`, `parent`,
`root`, `path`, `assets`, `asset`, `mainAsset`, `maintenanceOrder`, `equipment`, `equipmentType`, `manufacturer`,
`serialNumber`, `files`, `timeSeries`, `category`, `mimeType`, `directory`, `isUploaded`, `uploadedTime`, `isStep`,
`stateSet`, `startTime`, `endTime`, `scheduledStartTime`, `scheduledEndTime`, …

Also never use reserved container property names (`space`, `externalId`, `createdTime`, `lastUpdatedTime`,
`deletedTime`, `version`, …) or reserved view/container ids (`File`, `TimeSeries`, `String`, `Query`, …).

If the source has a conflicting concept, pick a distinct id (`locationUnit`, `siteStatus`, …).

### 8.5 Add custom type checklist

1. New `*.Container.yaml` with `usedFor: node`, description, properties, indexes; **no** CogniteDescribable require
   (prefer local `name`).
2. New `*.View.yaml` — no `implements` for label-only types; `source` on every direct.
3. Add view to `enterprise.DataModel.yaml`.
4. Matching `Search*.View.yaml` (maps the new enterprise container only) + `search.DataModel.yaml` entry + reverses.
5. New transformation + workflow task + ingestion variable (§8.9 row pattern).
6. Update README `consumers:` if already deployed.

**Add a record type** (events, logs) — follow §5.11 instead: `usedFor: record` container without constraints or
indexes, record view with `streamId` (not in a data model, no search view), reuse `{{ alarm_stream_external_id }}`
or add a stream only if retention/mutability differ (project stream limits are small), ingestion by extractor or
Function — never a transformation.

### 8.6 Trim checklist (W1a — before prod)

Use the touch map (§8.9) for the type being removed:

1. Remove the container, view and DataModel entry.
2. Remove the matching Search view, its DataModel entry and its reverses on other search views.
3. Remove the transformation, its workflow task and ingestion variable, and the RAW table + sample data.
4. Remove unused scaffold properties (containers are additive after deploy — trim **now**).

### 8.7 Permanence and versioning (cog-vd W2)

- **Container and property identifiers are permanent once deployed.** Rename them only before the first production
  deploy. After that, add a new property, migrate data, then retire the old one (expand-and-contract).
- Containers: unversioned; additive after deploy.
- **Record containers are stricter:** deleting `AlarmEvent` makes every record written through it permanently
  inaccessible, even if it is recreated with the same id. Never delete or recreate it once records exist; add
  properties only. The stream template is fixed at creation — choose `BasicArchive` before going to production.
- Every property **written** to a stream counts against the stream's max unique properties budget; don't create
  throw-away properties on record containers.
- Breaking view change → bump `dm_enterprise_version` or `dm_search_version` and every reference (`source`,
  `through`, `implements`, data model `views:`). Also bump the `AlarmEvent` record view `version` (it uses
  `{{ dm_enterprise_version }}` even though it is not listed in the DataModel) and any Function / client that
  references `AlarmEvent/{{ dm_enterprise_version }}`.
- Keep previous view versions through the agreed deprecation window (90 days default); mark deprecated views in
  their `description`.
- README `consumers:` lists apps and pinned versions from day one (stubs are fine for the template — Search UI,
  InField-shaped work views, sample workflow).

### 8.8 Naming

| Resource | Pattern | Example |
|----------|---------|---------|
| Data model | `PascalCase_DOM` / `_SOL` | `Industry_DOM` |
| DM space | `dm_dom_*` / `dm_sol_*` | `dm_dom_industry` |
| Instance space | `inst_*` | `inst_industry` |
| View / container | PascalCase, singular, not reserved | `TimeSeriesData`, `Document` |
| Property | camelCase | `documentNumber` |
| Location filter | `loc_{location}` | `loc_industry_all` |
| Data set | `ds_{data_type}_{location}` | `ds_industry_all` |
| RAW database | `raw_{data_type}_{location}_{source}` | `raw_sample_all_sap` |
| Transformation | `tr_{source}_{location}_to_{target}` | `tr_sap_all_to_industry_tag` |
| Stream | `{source}_{location}_{concept}` (snake_case) | `pi_all_alarm_event` |
| Record | `{source}_{location}_{machine}_{id}` (snake_case) | `pi_all_<tag>_<eventTime>_<eventType>` |
| Function | `fn_{source}_{location}_{intent}_{target}` | `fn_pi_all_ingest_alarm_event` |
| Workflow | `wf_{location}_{intent}` | `wf_all_industry_ingestion` |
| Workflow trigger | `trigger_` + workflow id (cog-vd workflow skill) | `trigger_wf_all_industry_ingestion` |
| Access group / service principal | persona-led `<persona>_[segment_]<type>_<environment>`; persona `admin` / `producer` / `consumer`; segment `industry`; **type** from the vocabulary below | `consumer_industry_sol_prod` |
| Function space | `sp_function_{snake_case}` — code upload only; never reuse `instance_space` | `sp_function_industry` |

**Access group type vocabulary (this template).** cog-vd naming-conventions lists building-block types such as
`pp`, `ep`, `all`. This template uses a documented alternative scheme (allowed when stated in project governance):

| Type token | Meaning | Used by |
|------------|---------|---------|
| `dom` | Enterprise / domain data model scope | `admin_industry_dom_*`, `consumer_industry_dom_*` |
| `sol` | Search / solution data product scope | `admin_industry_sol_*`, `consumer_industry_sol_*` |
| `pp` | Processing pipeline (transformations + function runtime) | `producer_industry_pp_*` |
| `wf` | Workflow operator (run / monitor only) | `producer_industry_wf_*` |

`cdf-naming-check` must treat these four tokens as approved for this pack — do not flag them as unknown types.

### 8.9 Touch map — every file per entity

| Entity | Enterprise | Search | Processing |
|--------|-----------|--------|------------|
| Tag | `Tag.Container`, `Tag.View`, DataModel | `SearchTag.View` (+ all reverses), DataModel | `tr_sap_all_to_industry_tag`, `raw_sample_all_sap.tag`, workflow task |
| Equipment | `Equipment.Container`, `Equipment.View`, DataModel | `SearchEquipment.View`, `SearchTag.equipment`, `SearchEquipment.materials`, DataModel | `tr_sap_all_to_industry_equipment_type`, `tr_sap_all_to_industry_equipment`, `tr_sharepoint_all_to_industry_equipment_document_connection`, `raw_sample_all_sap.equipment`, workflow tasks |
| Document | `Document.Container`, `Document.View`, DataModel | `SearchDocument.View`, `SearchTag.documents`, DataModel | `tr_sharepoint_all_to_industry_file_category`, `tr_sharepoint_all_to_industry_document`, `tr_sharepoint_all_to_industry_diagram_annotation`, `raw_sample_all_sharepoint.file` / `.diagram_annotation`, workflow tasks |
| TimeSeriesData | `TimeSeriesData.Container`, `TimeSeriesData.View`, DataModel | `SearchTimeSeriesData.View`, `SearchTag.timeSeries`, `SearchTimeSeriesData.operatingLimits`, DataModel | `tr_pi_all_to_industry_time_series`, `raw_sample_all_pi.timeseries`, workflow task |
| WorkOrder | `WorkOrder.Container`, `WorkOrder.View`, DataModel | `SearchWorkOrder.View`, `SearchTag.workOrders`, `SearchWorkOrder.operations`, DataModel | `tr_sap_all_to_industry_work_order`, `raw_sample_all_sap.work_order`, workflow task |
| WorkOrderOperation | `WorkOrderOperation.View`, DataModel | `SearchWorkOrderOperation.View`, DataModel | `tr_sap_all_to_industry_work_order_operation`, `raw_sample_all_sap.work_order_operation`, workflow task |
| Notification | `Notification.Container`, `Notification.View`, DataModel | `SearchNotification.View`, `SearchTag.notifications`, DataModel | `tr_sap_all_to_industry_notification`, `raw_sample_all_sap.notification`, workflow task |
| Material | `Material.Container`, `Material.View`, DataModel | `SearchMaterial.View`, `SearchEquipment.materials`, DataModel | `tr_sap_all_to_industry_material`, `raw_sample_all_sap.material`, workflow task |
| OperatingLimit | `OperatingLimit.Container`, `OperatingLimit.View`, DataModel | `SearchOperatingLimit.View`, `SearchTimeSeriesData.operatingLimits`, DataModel | `tr_pi_all_to_industry_operating_limit`, `raw_sample_all_pi.timeseries` + `raw_sample_all_sap.instrument_equipment`, workflow task |

| AlarmEvent (record) | `AlarmEvent.Container` (`usedFor: record`), `AlarmEvent.View` (record view), `alarm.Streams.yaml` — **no** DataModel entry | none | `fn_pi_all_ingest_alarm_event` (+ `pyproject.toml`, `requirements.txt`, `PACKAGE_SPECS` entry), `raw_sample_all_pi.alarm_event`, workflow task |

Every transformation and function id also appears as a variable in `cdf_industry_ingestion/default.config.yaml`.

### 8.10 Rename recipes

| Change | Where | How |
|--------|-------|-----|
| Spaces, data model ids/names, versions, data set, location filters, group names, RAW databases, workflow | `config.<env>.yaml` → `variables.modules` | Config only (§3.2) — no YAML edits |
| Rename a property | Touch-map row of the entity | Container property key + index, enterprise view property + `containerPropertyIdentifier`, search view `containerPropertyIdentifier`, transformation SQL alias, any `through.identifier` |
| Rename a type (e.g. `Tag` → `FunctionalLocation`) | Touch-map row + every `source:` / `through:` | Grep `externalId: Tag`, `destinationType: Tag`, `'Tag'` in SQL; rename container/view files; update DataModel lists; rename search view (`SearchTag` → `SearchFunctionalLocation`) |
| Remove a type | §8.6 | Follow the touch-map row |

Before the first production deploy all of the above are free; after it, follow §8.7.
