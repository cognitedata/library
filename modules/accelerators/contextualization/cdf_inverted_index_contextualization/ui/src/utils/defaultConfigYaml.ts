import yaml from "js-yaml";
import type { MessageKey } from "../i18n/types";
import {
  DIRECT_RELATION_LINK_KEYS,
  emptyAnnotationIndexConfig,
  emptyDirectRelationTopLevel,
  emptyGeneralConfig,
  emptyIndexFieldProperty,
  emptyIndexFieldView,
  emptyScopeConfig,
  emptySubscriptionConfig,
  type AnnotationIndexConfig,
  type ConfigSection,
  type DirectRelationTopLevel,
  type GeneralConfig,
  type IndexFieldProperty,
  type IndexFieldView,
  type ScopeConfig,
  type ScopeLevelPaths,
  type ScopeResolveCandidate,
  type SubscriptionConfig,
} from "../types/invertedIndexConfig";

export const CONFIG_SECTIONS: { id: ConfigSection; labelKey: MessageKey }[] = [
  { id: "general", labelKey: "config.section.general" },
  { id: "scope", labelKey: "config.section.scope" },
  { id: "indexFields", labelKey: "config.section.indexFields" },
  { id: "annotation", labelKey: "config.section.annotation" },
  { id: "targetDriven", labelKey: "config.section.targetDriven" },
  { id: "linking", labelKey: "config.section.linking" },
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

export function generalFromDoc(doc: Record<string, unknown>): GeneralConfig {
  const base = emptyGeneralConfig();
  const backend = doc.index_storage_backend;
  return {
    name: doc.name != null ? String(doc.name) : base.name,
    organization: doc.organization != null ? String(doc.organization) : base.organization,
    indexStorageBackend: backend === "dm" ? "dm" : "raw",
    indexRawDatabase:
      doc.index_raw_database != null ? String(doc.index_raw_database) : base.indexRawDatabase,
  };
}

export function mergeGeneralIntoDoc(doc: Record<string, unknown>, general: GeneralConfig): void {
  doc.name = general.name;
  doc.organization = general.organization;
  doc.index_storage_backend = general.indexStorageBackend;
  doc.index_raw_database = general.indexRawDatabase;
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
  return {
    path: o.path != null ? String(o.path) : base.path,
    sourceType,
    extractPattern:
      o.extract_pattern != null ? String(o.extract_pattern).trim() : base.extractPattern,
  };
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
    properties: v.properties
      .filter((p) => p.path.trim())
      .map((p) => {
        const row: Record<string, unknown> = {
          path: p.path,
          source_type: p.sourceType,
        };
        if (p.extractPattern) {
          row.extract_mode = "regex";
          row.extract_pattern = p.extractPattern;
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
    assetViews: stringList(o.asset_views).length ? stringList(o.asset_views) : base.assetViews,
    fileViews: stringList(o.file_views).length ? stringList(o.file_views) : base.fileViews,
    defaultInstanceType:
      o.default_instance_type != null ? String(o.default_instance_type) : base.defaultInstanceType,
  };
}

export function instanceSpacesFromDoc(doc: Record<string, unknown>): string[] {
  return stringList(doc.instance_spaces);
}

export function mergeTargetDrivenIntoDoc(
  doc: Record<string, unknown>,
  subscription: SubscriptionConfig,
  instanceSpaces: string[]
): void {
  doc.subscription = {
    enabled: subscription.enabled,
    trigger: subscription.trigger,
    watch_property: subscription.watchProperty,
    instance_spaces: subscription.instanceSpaces,
    asset_views: subscription.assetViews,
    file_views: subscription.fileViews,
    default_instance_type: subscription.defaultInstanceType,
  };
  if (instanceSpaces.length) {
    doc.instance_spaces = instanceSpaces;
  } else {
    delete doc.instance_spaces;
  }
}

export function directRelationFromDoc(doc: Record<string, unknown>): DirectRelationTopLevel {
  const base = emptyDirectRelationTopLevel();
  const o = asRecord(doc.direct_relation_config);
  const links = asRecord(o.links);
  const linkEnabled: Record<string, boolean> = { ...base.linkEnabled };
  for (const key of DIRECT_RELATION_LINK_KEYS) {
    const link = asRecord(links[key]);
    linkEnabled[key] = link.enabled != null ? Boolean(link.enabled) : true;
  }
  return {
    enabled: o.enabled != null ? Boolean(o.enabled) : base.enabled,
    minConfidence: typeof o.min_confidence === "number" ? o.min_confidence : base.minConfidence,
    allowedAnnotationStatuses: stringList(o.allowed_annotation_statuses).length
      ? stringList(o.allowed_annotation_statuses)
      : base.allowedAnnotationStatuses,
    writeOnSuggestedAnnotations:
      o.write_on_suggested_annotations != null
        ? Boolean(o.write_on_suggested_annotations)
        : base.writeOnSuggestedAnnotations,
    sourceTypes: stringList(o.source_types),
    maxListSize: typeof o.max_list_size === "number" ? o.max_list_size : base.maxListSize,
    linkEnabled,
  };
}

export function mergeDirectRelationIntoDoc(doc: Record<string, unknown>, cfg: DirectRelationTopLevel): void {
  const existing = asRecord(doc.direct_relation_config);
  const links = { ...asRecord(existing.links) };
  for (const key of DIRECT_RELATION_LINK_KEYS) {
    const link = { ...asRecord(links[key]) };
    link.enabled = cfg.linkEnabled[key] ?? true;
    links[key] = link;
  }
  doc.direct_relation_config = {
    ...existing,
    enabled: cfg.enabled,
    min_confidence: cfg.minConfidence,
    allowed_annotation_statuses: cfg.allowedAnnotationStatuses,
    write_on_suggested_annotations: cfg.writeOnSuggestedAnnotations,
    source_types: cfg.sourceTypes.length ? cfg.sourceTypes : existing.source_types,
    max_list_size: cfg.maxListSize,
    links,
  };
}

export function docFromYaml(content: string): Record<string, unknown> {
  return parseDefaultDocument(content);
}

export function yamlFromDoc(doc: Record<string, unknown>): string {
  return stringifyDefaultDocument(doc);
}
