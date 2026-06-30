import yaml from "js-yaml";
import type { MessageKey } from "../i18n/types";
import {
  DEFAULT_DIRECT_RELATION_VIEWS,
  DEFAULT_EDGE_VIEWS,
  WRITE_MODES,
  defaultDirectRelationLinkConfig,
  emptyAnnotationIndexConfig,
  emptyDirectRelationConfig,
  emptyGeneralConfig,
  emptyRawTermPartitionConfig,
  emptyIndexFieldProperty,
  emptyIndexFieldView,
  emptyScopeConfig,
  emptySubscriptionConfig,
  emptyTargetDrivenQueryConfig,
  emptyVirtualTagCreationConfig,
  type AnnotationIndexConfig,
  type ConfigSection,
  type DirectRelationConfig,
  type DirectRelationLinkConfig,
  type DmViewRef,
  type GeneralConfig,
  type IndexFieldProperty,
  type IndexFieldView,
  type RawTermPartitionConfig,
  type ScopeConfig,
  type ScopeLevelPaths,
  type ScopeResolveCandidate,
  type SubscriptionConfig,
  type TargetDrivenQueryConfig,
  type VirtualTagCreationConfig,
  type WriteMode,
} from "../types/invertedIndexConfig";
import type { JsonObject } from "../types/jsonConfig";

export const CONFIG_SECTIONS: { id: ConfigSection; labelKey: MessageKey }[] = [
  { id: "general", labelKey: "config.section.general" },
  { id: "scope", labelKey: "config.section.scope" },
  { id: "indexFields", labelKey: "config.section.indexFields" },
  { id: "annotation", labelKey: "config.section.annotation" },
  { id: "targetDriven", labelKey: "config.section.targetDriven" },
  { id: "linking", labelKey: "config.section.linking" },
  { id: "virtualTags", labelKey: "config.section.virtualTags" },
];

export function parseDefaultDocument(content: string): Record<string, unknown> {
  const doc = yaml.load(content);
  if (doc == null) return {};
  if (typeof doc !== "object" || Array.isArray(doc)) {
    throw new Error("default.config.yaml root must be a mapping");
  }
  return doc as Record<string, unknown>;
}

export function stringifyDefaultDocument(doc: Record<string, unknown>): string {
  return yaml.dump(doc, { lineWidth: -1, noRefs: true, sortKeys: false });
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((v) => String(v).trim()).filter(Boolean);
}

function normalizeResolveCandidate(raw: unknown): ScopeResolveCandidate | null {
  if (typeof raw === "string") {
    const path = raw.trim();
    if (!path) return null;
    return { path, extractPattern: "" };
  }
  const o = asRecord(raw);
  const path = o.path != null ? String(o.path).trim() : "";
  if (!path) return null;
  const extractPattern =
    o.extract_pattern != null ? String(o.extract_pattern).trim() : "";
  return { path, extractPattern };
}

function normalizeResolveCandidates(raw: unknown): ScopeResolveCandidate[] {
  if (Array.isArray(raw)) {
    return raw
      .map((item) => normalizeResolveCandidate(item))
      .filter((item): item is ScopeResolveCandidate => item != null);
  }
  const single = normalizeResolveCandidate(raw);
  return single ? [single] : [];
}

function normalizeLevelPaths(raw: unknown): ScopeLevelPaths {
  const out: ScopeLevelPaths = {};
  const o = asRecord(raw);
  for (const [level, candidates] of Object.entries(o)) {
    const list = normalizeResolveCandidates(candidates);
    if (list.length) out[level] = list;
  }
  return out;
}

function cleanResolveCandidates(candidates: ScopeResolveCandidate[]): ScopeResolveCandidate[] {
  return candidates
    .map((c) => ({
      path: c.path.trim(),
      extractPattern: c.extractPattern.trim(),
    }))
    .filter((c) => c.path.length > 0);
}

function serializeResolveCandidate(
  candidate: ScopeResolveCandidate
): string | Record<string, unknown> {
  if (candidate.extractPattern) {
    return {
      path: candidate.path,
      extract_mode: "regex",
      extract_pattern: candidate.extractPattern,
    };
  }
  return candidate.path;
}

function serializeResolveCandidates(
  candidates: ScopeResolveCandidate[]
): Array<string | Record<string, unknown>> {
  return cleanResolveCandidates(candidates).map((candidate) =>
    serializeResolveCandidate(candidate)
  );
}

export function termPartitionFromDoc(doc: Record<string, unknown>): RawTermPartitionConfig {
  const base = emptyRawTermPartitionConfig();
  const tp = asRecord(doc.index_raw_term_partition);
  const bucketMode = tp.bucket_mode === "ascii_first_char" ? "ascii_first_char" : "script_aware";
  const activate =
    tp.activate_above_rows != null ? Number(tp.activate_above_rows) : base.activateAboveRows;
  return {
    enabled: Boolean(tp.enabled),
    strategy: "first_char",
    activateAboveRows: Number.isFinite(activate) && activate > 0 ? activate : base.activateAboveRows,
    bucketMode,
  };
}

export function generalFromDoc(doc: Record<string, unknown>): GeneralConfig {
  const base = emptyGeneralConfig();
  const backend = doc.index_storage_backend;
  return {
    name: doc.name != null ? String(doc.name) : base.name,
    organization: doc.organization != null ? String(doc.organization) : base.organization,
    indexStorageBackend: backend === "dm" ? "dm" : "raw",
    indexRawDatabase:
      doc.index_raw_database != null ? String(doc.index_raw_database) : base.indexRawDatabase,
    termPartition: termPartitionFromDoc(doc),
  };
}

export function mergeGeneralIntoDoc(doc: Record<string, unknown>, general: GeneralConfig): void {
  doc.name = general.name;
  doc.organization = general.organization;
  doc.index_storage_backend = general.indexStorageBackend;
  doc.index_raw_database = general.indexRawDatabase;
  doc.index_raw_term_partition = {
    enabled: general.termPartition.enabled,
    strategy: general.termPartition.strategy,
    activate_above_rows: general.termPartition.activateAboveRows,
    bucket_mode: general.termPartition.bucketMode,
  };
}

export function scopeFromDoc(doc: Record<string, unknown>): ScopeConfig {
  const base = emptyScopeConfig();
  const s = asRecord(doc.scope);
  const resolveFrom: Record<string, ScopeLevelPaths> = {};
  for (const [view, levels] of Object.entries(asRecord(s.resolve_from))) {
    resolveFrom[view] = normalizeLevelPaths(levels);
  }
  return {
    enabled: Boolean(s.enabled),
    levels: stringList(s.levels),
    scopeKeyTemplate:
      s.scope_key_template != null ? String(s.scope_key_template) : base.scopeKeyTemplate,
    strictScope: Boolean(s.strict_scope),
    fallbackScopeKey:
      s.fallback_scope_key != null ? String(s.fallback_scope_key) : base.fallbackScopeKey,
    annotationScopeViaLinkedFile:
      s.annotation_scope_via_linked_file != null
        ? Boolean(s.annotation_scope_via_linked_file)
        : base.annotationScopeViaLinkedFile,
    resolveFrom,
    resolveFromDefault: normalizeLevelPaths(s.resolve_from_default),
  };
}

export function mergeScopeIntoDoc(doc: Record<string, unknown>, scope: ScopeConfig): void {
  const resolveFrom: Record<string, Record<string, Array<string | Record<string, unknown>>>> = {};
  for (const [view, levels] of Object.entries(scope.resolveFrom)) {
    const levelOut: Record<string, Array<string | Record<string, unknown>>> = {};
    for (const [level, candidates] of Object.entries(levels)) {
      const serialized = serializeResolveCandidates(candidates);
      if (serialized.length) levelOut[level] = serialized;
    }
    if (Object.keys(levelOut).length) resolveFrom[view] = levelOut;
  }
  const resolveFromDefault: Record<string, Array<string | Record<string, unknown>>> = {};
  for (const [level, candidates] of Object.entries(scope.resolveFromDefault)) {
    const serialized = serializeResolveCandidates(candidates);
    if (serialized.length) resolveFromDefault[level] = serialized;
  }
  doc.scope = {
    enabled: scope.enabled,
    levels: scope.levels,
    scope_key_template: scope.scopeKeyTemplate,
    strict_scope: scope.strictScope,
    fallback_scope_key: scope.fallbackScopeKey,
    annotation_scope_via_linked_file: scope.annotationScopeViaLinkedFile,
    resolve_from: resolveFrom,
    resolve_from_default: resolveFromDefault,
  };
}

function normalizeProperty(raw: unknown): IndexFieldProperty {
  const base = emptyIndexFieldProperty();
  const o = asRecord(raw);
  const sourceType = o.source_type === "file_metadata" ? "file_metadata" : "asset_metadata";
  const extractPattern =
    o.extract_pattern != null ? String(o.extract_pattern).trim() : base.extractPattern;
  const extractMode = extractPattern ? "regex" : "passthrough";
  return {
    path: o.path != null ? String(o.path) : base.path,
    sourceType,
    extractMode,
    extractPattern,
  };
}

function normalizeFilters(raw: unknown): JsonObject[] {
  if (!Array.isArray(raw)) return [];
  return raw.filter((f): f is JsonObject => f != null && typeof f === "object" && !Array.isArray(f));
}

function normalizeView(raw: unknown): IndexFieldView {
  const base = emptyIndexFieldView();
  const o = asRecord(raw);
  const properties = Array.isArray(o.properties)
    ? o.properties.map((p) => normalizeProperty(p))
    : base.properties;
  return {
    view: o.view != null ? String(o.view) : base.view,
    viewSpace: o.view_space != null ? String(o.view_space) : base.viewSpace,
    version: o.version != null ? String(o.version) : base.version,
    instanceSpaces: stringList(o.instance_spaces),
    filters: normalizeFilters(o.filters),
    properties: properties.length ? properties : [emptyIndexFieldProperty()],
  };
}

export function indexFieldsFromDoc(doc: Record<string, unknown>): IndexFieldView[] {
  const raw = doc.index_field_config;
  if (!Array.isArray(raw)) return [];
  return raw.map((v) => normalizeView(v));
}

export function mergeIndexFieldsIntoDoc(doc: Record<string, unknown>, views: IndexFieldView[]): void {
  doc.index_field_config = views.map((v) => ({
    view: v.view,
    view_space: v.viewSpace,
    version: v.version || undefined,
    instance_spaces: v.instanceSpaces,
    filters: v.filters.length ? v.filters : undefined,
    properties: v.properties
      .filter((p) => p.path.trim())
      .map((p) => {
        const row: Record<string, unknown> = {
          path: p.path,
          source_type: p.sourceType,
        };
        const pattern = p.extractPattern.trim();
        if (pattern) {
          row.extract_mode = "regex";
          row.extract_pattern = pattern;
        }
        return row;
      }),
  }));
}

export function annotationFromDoc(doc: Record<string, unknown>): AnnotationIndexConfig {
  const base = emptyAnnotationIndexConfig();
  const o = asRecord(doc.annotation_index_config);
  return {
    view: o.view != null ? String(o.view) : base.view,
    viewSpace: o.view_space != null ? String(o.view_space) : base.viewSpace,
    version: o.version != null ? String(o.version) : base.version,
    instanceType: o.instance_type != null ? String(o.instance_type) : base.instanceType,
    textProperty: o.text_property != null ? String(o.text_property) : base.textProperty,
    confidenceProperty:
      o.confidence_property != null ? String(o.confidence_property) : base.confidenceProperty,
    statusProperty: o.status_property != null ? String(o.status_property) : base.statusProperty,
    pageProperty: o.page_property != null ? String(o.page_property) : base.pageProperty,
    bboxProperties: stringList(o.bbox_properties),
  };
}

export function mergeAnnotationIntoDoc(doc: Record<string, unknown>, cfg: AnnotationIndexConfig): void {
  doc.annotation_index_config = {
    view: cfg.view,
    view_space: cfg.viewSpace,
    version: cfg.version,
    instance_type: cfg.instanceType,
    text_property: cfg.textProperty,
    confidence_property: cfg.confidenceProperty,
    status_property: cfg.statusProperty,
    page_property: cfg.pageProperty,
    bbox_properties: cfg.bboxProperties,
  };
}

export function subscriptionFromDoc(doc: Record<string, unknown>): SubscriptionConfig {
  const base = emptySubscriptionConfig();
  const o = asRecord(doc.subscription);
  return {
    enabled: o.enabled != null ? Boolean(o.enabled) : base.enabled,
    trigger: o.trigger != null ? String(o.trigger) : base.trigger,
    watchProperty: o.watch_property != null ? String(o.watch_property) : base.watchProperty,
    instanceSpaces: stringList(o.instance_spaces),
    watchViewKeys: stringList(o.watch_view_keys).length
      ? stringList(o.watch_view_keys)
      : base.watchViewKeys,
  };
}

export function targetDrivenQueryFromDoc(doc: Record<string, unknown>): TargetDrivenQueryConfig {
  const base = emptyTargetDrivenQueryConfig();
  const o = asRecord(doc.target_driven);
  const fallbacks = stringList(o.query_property_fallbacks);
  return {
    queryProperty:
      o.query_property != null ? String(o.query_property) : base.queryProperty,
    queryPropertyFallbacks: fallbacks.length ? fallbacks : base.queryPropertyFallbacks,
    excludeEmptyAliases:
      o.exclude_empty_aliases != null
        ? Boolean(o.exclude_empty_aliases)
        : base.excludeEmptyAliases,
  };
}

export function mergeTargetDrivenIntoDoc(
  doc: Record<string, unknown>,
  subscription: SubscriptionConfig,
  query: TargetDrivenQueryConfig
): void {
  doc.target_driven = {
    query_property: query.queryProperty,
    query_property_fallbacks: query.queryPropertyFallbacks,
    exclude_empty_aliases: query.excludeEmptyAliases,
  };
  doc.subscription = {
    enabled: subscription.enabled,
    trigger: subscription.trigger,
    watch_property: subscription.watchProperty,
    instance_spaces: subscription.instanceSpaces,
    watch_view_keys: subscription.watchViewKeys,
  };
  delete doc.instance_spaces;
}

export function virtualTagCreationFromDoc(
  doc: Record<string, unknown>
): VirtualTagCreationConfig {
  const base = emptyVirtualTagCreationConfig();
  const o = asRecord(doc.virtual_tag_creation);
  const criteria = asRecord(o.missing_tag_criteria);
  return {
    enabled: o.enabled != null ? Boolean(o.enabled) : base.enabled,
    incrementalEnabled:
      o.incremental_enabled != null
        ? Boolean(o.incremental_enabled)
        : base.incrementalEnabled,
    termSelectionMode:
      o.term_selection_mode === "all" || o.term_selection_mode === "missing_tags_only"
        ? o.term_selection_mode
        : base.termSelectionMode,
    instanceSpace:
      o.instance_space != null ? String(o.instance_space) : base.instanceSpace,
    missingTagCriteria: {
      requirePatternDetection:
        criteria.require_pattern_detection != null
          ? Boolean(criteria.require_pattern_detection)
          : base.missingTagCriteria.requirePatternDetection,
      checkExistingCogniteAsset:
        criteria.check_existing_cognite_asset != null
          ? Boolean(criteria.check_existing_cognite_asset)
          : base.missingTagCriteria.checkExistingCogniteAsset,
      excludeWithCogniteAssetMetadata:
        criteria.exclude_with_cognite_asset_metadata != null
          ? Boolean(criteria.exclude_with_cognite_asset_metadata)
          : base.missingTagCriteria.excludeWithCogniteAssetMetadata,
    },
  };
}

export function mergeVirtualTagCreationIntoDoc(
  doc: Record<string, unknown>,
  cfg: VirtualTagCreationConfig
): void {
  const existing = asRecord(doc.virtual_tag_creation);
  doc.virtual_tag_creation = {
    ...existing,
    enabled: cfg.enabled,
    incremental_enabled: cfg.incrementalEnabled,
    term_selection_mode: cfg.termSelectionMode,
    instance_space: cfg.instanceSpace,
    missing_tag_criteria: {
      require_pattern_detection: cfg.missingTagCriteria.requirePatternDetection,
      check_existing_cognite_asset: cfg.missingTagCriteria.checkExistingCogniteAsset,
      exclude_with_cognite_asset_metadata:
        cfg.missingTagCriteria.excludeWithCogniteAssetMetadata,
    },
  };
}

function isWriteMode(value: string): value is WriteMode {
  return (WRITE_MODES as readonly string[]).includes(value);
}

function deepMergeRecord(base: Record<string, unknown>, override: Record<string, unknown>): Record<string, unknown> {
  const merged = { ...base };
  for (const [key, val] of Object.entries(override)) {
    if (
      val != null &&
      typeof val === "object" &&
      !Array.isArray(val) &&
      typeof merged[key] === "object" &&
      merged[key] != null &&
      !Array.isArray(merged[key])
    ) {
      merged[key] = deepMergeRecord(
        merged[key] as Record<string, unknown>,
        val as Record<string, unknown>
      );
    } else {
      merged[key] = val;
    }
  }
  return merged;
}

function effectiveDirectRelationDoc(
  doc: Record<string, unknown>,
  runtimeDr?: Record<string, unknown>
): Record<string, unknown> {
  const yamlDr = asRecord(doc.direct_relation_config);
  if (!runtimeDr || !Object.keys(runtimeDr).length) return yamlDr;
  return deepMergeRecord(runtimeDr, yamlDr);
}

function dmViewRefFromRecord(raw: unknown, base: DmViewRef): DmViewRef {
  const o = asRecord(raw);
  return {
    space: o.space != null ? String(o.space) : base.space,
    externalId: o.external_id != null ? String(o.external_id) : base.externalId,
    version: o.version != null ? String(o.version) : base.version,
  };
}

function dmViewRefsFromRecord(
  raw: unknown,
  defaults: Record<string, DmViewRef>
): Record<string, DmViewRef> {
  const o = asRecord(raw);
  const keys = new Set([...Object.keys(defaults), ...Object.keys(o)]);
  const out: Record<string, DmViewRef> = {};
  for (const key of keys) {
    out[key] = dmViewRefFromRecord(o[key], defaults[key] ?? { space: "cdf_cdm", externalId: "", version: "v1" });
  }
  return out;
}

function directRelationLinkFromRecord(raw: unknown): DirectRelationLinkConfig {
  const o = asRecord(raw);
  const base = defaultDirectRelationLinkConfig(
    o.forward_view != null ? String(o.forward_view) : "",
    o.target_view != null ? String(o.target_view) : "",
    o.property != null ? String(o.property) : ""
  );
  const edge = asRecord(o.edge);
  const diagram = asRecord(o.diagram_annotation);
  const writeModes = stringList(o.write_modes).filter(isWriteMode);
  const sourceTypes = stringList(o.source_types);
  const incomingViews = stringList(o.incoming_views);
  return {
    label: o.label != null ? String(o.label) : undefined,
    enabled: o.enabled != null ? Boolean(o.enabled) : base.enabled,
    writeModes: writeModes.length ? writeModes : base.writeModes,
    forwardView: o.forward_view != null ? String(o.forward_view) : base.forwardView,
    targetView: o.target_view != null ? String(o.target_view) : base.targetView,
    property: o.property != null ? String(o.property) : base.property,
    cardinality:
      o.cardinality === "single"
        ? "single"
        : o.cardinality === "list"
          ? "list"
          : base.cardinality,
    overwriteExisting:
      o.overwrite_existing != null ? Boolean(o.overwrite_existing) : base.overwriteExisting,
    incomingViews: incomingViews.length ? incomingViews : base.incomingViews,
    sourceTypes: sourceTypes.length ? sourceTypes : base.sourceTypes,
    edgeViewKey: edge.edge_view != null ? String(edge.edge_view) : base.edgeViewKey,
    diagramAnnotation: {
      createStatus:
        diagram.create_status != null ? String(diagram.create_status) : base.diagramAnnotation.createStatus,
      updateEndNodeOnly:
        diagram.update_end_node_only != null
          ? Boolean(diagram.update_end_node_only)
          : base.diagramAnnotation.updateEndNodeOnly,
      fileFromReference:
        diagram.file_from_reference != null
          ? Boolean(diagram.file_from_reference)
          : base.diagramAnnotation.fileFromReference,
      annotationIdPath:
        diagram.annotation_id_path != null
          ? String(diagram.annotation_id_path)
          : base.diagramAnnotation.annotationIdPath,
    },
    resolveByIncomingView: o.resolve_by_incoming_view,
  };
}

export function directRelationFromDoc(
  doc: Record<string, unknown>,
  runtimeDr?: Record<string, unknown>
): DirectRelationConfig {
  const base = emptyDirectRelationConfig();
  const o = effectiveDirectRelationDoc(doc, runtimeDr);
  const linksRaw = asRecord(o.links);
  const linkKeys = Object.keys(linksRaw);
  const links = Object.fromEntries(
    linkKeys.map((key) => [key, directRelationLinkFromRecord(linksRaw[key])])
  );
  const linkOrderRaw = stringList(o.link_order);
  const linkOrder = linkOrderRaw.length ? linkOrderRaw : linkKeys.length ? linkKeys : base.linkOrder;
  const globalSourceTypes = stringList(o.source_types);
  const requireStatus = o.require_annotation_status;
  return {
    enabled: o.enabled != null ? Boolean(o.enabled) : base.enabled,
    minConfidence: typeof o.min_confidence === "number" ? o.min_confidence : base.minConfidence,
    allowedAnnotationStatuses: stringList(o.allowed_annotation_statuses).length
      ? stringList(o.allowed_annotation_statuses)
      : base.allowedAnnotationStatuses,
    requireAnnotationStatus:
      requireStatus != null && String(requireStatus).trim()
        ? String(requireStatus)
        : base.requireAnnotationStatus,
    sourceTypes: globalSourceTypes.length ? globalSourceTypes : base.sourceTypes,
    maxListSize: typeof o.max_list_size === "number" ? o.max_list_size : base.maxListSize,
    linkOrder,
    views: dmViewRefsFromRecord(o.views, DEFAULT_DIRECT_RELATION_VIEWS),
    edgeViews: dmViewRefsFromRecord(o.edge_views, DEFAULT_EDGE_VIEWS),
    links,
  };
}

function serializeDmViewRef(ref: DmViewRef): Record<string, string> {
  return {
    space: ref.space,
    external_id: ref.externalId,
    version: ref.version,
  };
}

function serializeDirectRelationLink(
  link: DirectRelationLinkConfig,
  existing: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = {
    ...existing,
    enabled: link.enabled,
    write_modes: link.writeModes,
    forward_view: link.forwardView,
    target_view: link.targetView,
    property: link.property,
    cardinality: link.cardinality,
    incoming_views: link.incomingViews,
  };
  if (link.label?.trim()) {
    out.label = link.label.trim();
  }
  if (link.overwriteExisting) {
    out.overwrite_existing = true;
  } else if ("overwrite_existing" in out) {
    delete out.overwrite_existing;
  }
  if (link.sourceTypes.length) {
    out.source_types = link.sourceTypes;
  }
  if (link.writeModes.includes("edge")) {
    out.edge = {
      ...asRecord(existing.edge),
      ...(link.edgeViewKey ? { edge_view: link.edgeViewKey } : {}),
    };
  }
  if (link.writeModes.includes("diagram_annotation")) {
    out.diagram_annotation = {
      ...asRecord(existing.diagram_annotation),
      create_status: link.diagramAnnotation.createStatus,
      update_end_node_only: link.diagramAnnotation.updateEndNodeOnly,
      file_from_reference: link.diagramAnnotation.fileFromReference,
      annotation_id_path: link.diagramAnnotation.annotationIdPath,
    };
  }
  if (link.resolveByIncomingView != null) {
    out.resolve_by_incoming_view = link.resolveByIncomingView;
  }
  return out;
}

export function mergeDirectRelationIntoDoc(doc: Record<string, unknown>, cfg: DirectRelationConfig): void {
  const existing = asRecord(doc.direct_relation_config);
  const existingLinks = asRecord(existing.links);
  const linkKeys = cfg.linkOrder.length
    ? [...new Set([...cfg.linkOrder, ...Object.keys(cfg.links)])]
    : Object.keys(cfg.links);
  const links: Record<string, unknown> = {};
  for (const key of linkKeys) {
    const link = cfg.links[key];
    if (!link) continue;
    links[key] = serializeDirectRelationLink(link, asRecord(existingLinks[key]));
  }
  const views: Record<string, unknown> = {};
  for (const [key, ref] of Object.entries(cfg.views)) {
    views[key] = serializeDmViewRef(ref);
  }
  const edgeViews: Record<string, unknown> = {};
  for (const [key, ref] of Object.entries(cfg.edgeViews)) {
    edgeViews[key] = serializeDmViewRef(ref);
  }
  doc.direct_relation_config = {
    ...existing,
    enabled: cfg.enabled,
    min_confidence: cfg.minConfidence,
    allowed_annotation_statuses: cfg.allowedAnnotationStatuses,
    require_annotation_status: cfg.requireAnnotationStatus || null,
    source_types: cfg.sourceTypes,
    max_list_size: cfg.maxListSize,
    link_order: linkKeys,
    views,
    edge_views: edgeViews,
    links,
  };
}

export function docFromYaml(content: string): Record<string, unknown> {
  return parseDefaultDocument(content);
}

export function yamlFromDoc(doc: Record<string, unknown>): string {
  return stringifyDefaultDocument(doc);
}
