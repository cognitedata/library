import type { CogniteClient, FilterDefinition, NodeOrEdge } from "@cognite/sdk";
import type { PatternRecord, PipelineConfig, PipelineFilter, ViewConfig } from "./types";

export const GLOBAL_SCOPE = "GLOBAL";
const NONE_SCOPE = "__NONE__";

export interface PatternDraft {
  sample: string;
  resourceType: string;
  annotationType: string;
  patternScope: string;
}

export interface CachePreviewSummary {
  patternScope: string;
  assetEntities: number;
  fileEntities: number;
  assetPatternSamples: number;
  filePatternSamples: number;
  manualPatternSamples: number;
  combinedPatternSamples: number;
  lastUpdate?: string;
}

interface PatternSample {
  sample: string | string[];
  resource_type?: string;
  annotation_type?: string;
  created_by?: string;
}

interface AnnotationEntity {
  external_id: string;
  name?: string;
  space: string;
  annotation_type?: string;
  resource_type?: string;
  search_property: string[];
}

function isFullNumericAlias(value: unknown): boolean {
  const alias = String(value || "").trim();
  return /\d/.test(alias) && !/[A-Za-z]/.test(alias);
}

function removeFullNumericAssetAliases(entities: AnnotationEntity[]): AnnotationEntity[] {
  return entities.map((entity) => {
    if (entity.annotation_type !== "diagrams.AssetLink") return entity;
    const filteredAliases = (entity.search_property || []).filter((alias) => !isFullNumericAlias(alias));
    return { ...entity, search_property: filteredAliases };
  });
}


abstract class PatternSampleGenerator {
  getMessage(): string | undefined {
    return undefined;
  }

  prepareEntitiesForCache(entities: AnnotationEntity[]): AnnotationEntity[] {
    return entities;
  }

  abstract generateFromEntities(entities: AnnotationEntity[]): PatternSample[];
}

class DefaultPatternSampleGenerator extends PatternSampleGenerator {
  generateFromEntities(entities: AnnotationEntity[]): PatternSample[] {
    const patternBuilders: Record<string, { patterns: Record<string, Set<string>[][]>; annotationType?: string }> = {};

    const parseAlias = (alias: string, resourceTypeKey: string) => {
      const tokens: string[] = [];
      let current = "";

      for (const ch of alias) {
        if (/^[a-zA-Z0-9]$/.test(ch)) {
          current += ch;
        } else {
          if (current) tokens.push(current);
          current = "";
          tokens.push(ch);
        }
      }
      if (current) tokens.push(current);

      const templateParts: string[] = [];
      const variableParts: string[][][] = [];

      const isSeparator = (token: string) => token.length === 1 && !/^[a-zA-Z0-9]$/.test(token);

      for (let i = 0; i < tokens.length; i += 1) {
        const part = tokens[i];
        if (!part) continue;
        if (isSeparator(part)) {
          if (part === "-" || part === " ") {
            templateParts.push(part);
          } else if (part !== "[" && part !== "]") {
            templateParts.push(`[${part}]`);
          }
          continue;
        }

        const leftOk = i === 0 || isSeparator(tokens[i - 1]);
        const rightOk = i === tokens.length - 1 || isSeparator(tokens[i + 1]);
        if (leftOk && rightOk && part === resourceTypeKey) {
          templateParts.push(`[${part}]`);
          continue;
        }

        const segmentTemplate = part.replace(/\d/g, "0").replace(/[A-Za-z]/g, "A");
        templateParts.push(segmentTemplate);

        const letterGroups = part.match(/[A-Za-z]+/g) || [];
        if (letterGroups.length > 0) {
          variableParts.push(letterGroups.map((group) => [group]));
        }
      }

      return { templateKey: templateParts.join(""), variableParts };
    };

    for (const entity of entities) {
      const resourceType = entity.resource_type || "";
      if (!resourceType) continue;
      if (!patternBuilders[resourceType]) {
        patternBuilders[resourceType] = { patterns: {}, annotationType: entity.annotation_type };
      }

      const builder = patternBuilders[resourceType];
      if (!builder.annotationType && entity.annotation_type) {
        builder.annotationType = entity.annotation_type;
      }

      for (const alias of entity.search_property) {
        if (!alias) continue;
        const { templateKey, variableParts } = parseAlias(alias, resourceType);
        if (!templateKey) continue;

        if (!builder.patterns[templateKey]) {
          builder.patterns[templateKey] = variableParts.map((group) =>
            group.map((letters) => new Set(letters))
          );
        } else {
          const existing = builder.patterns[templateKey];
          variableParts.forEach((group, groupIndex) => {
            if (!existing[groupIndex]) {
              existing[groupIndex] = group.map((letters) => new Set(letters));
              return;
            }
            group.forEach((letters, letterIndex) => {
              if (!existing[groupIndex][letterIndex]) {
                existing[groupIndex][letterIndex] = new Set(letters);
                return;
              }
              letters.forEach((letter) => existing[groupIndex][letterIndex].add(letter));
            });
          });
        }
      }
    }

    const result: PatternSample[] = [];

    const hasAlphaOrClass = (value: string) => /[A-Za-z]/.test(value) || /\[[^\]]*\|[^\]]*\]/.test(value);

    for (const [resourceType, data] of Object.entries(patternBuilders)) {
      const samples: string[] = [];
      const templates = data.patterns || {};

      for (const [templateKey, variableGroups] of Object.entries(templates)) {
        let varIndex = 0;
        const parts = templateKey.match(/\[[^\]]+\]|[A-Za-z0-9]+|[^A-Za-z0-9]/g) || [templateKey];

        const finalParts = parts.map((part) => {
          const isBracketLiteral = /^\[[^\]]+\]$/.test(part);
          if (isBracketLiteral || !/A/.test(part)) return part;
          const lettersForSegment = variableGroups[varIndex] || [];
          varIndex += 1;
          let letterGroupIndex = 0;
          return part.replace(/A+/g, () => {
            const set = lettersForSegment[letterGroupIndex] || new Set<string>();
            letterGroupIndex += 1;
            return `[${Array.from(set).sort().join("|")}]`;
          });
        });

        samples.push(finalParts.join(""));
      }

      const filtered = samples.filter(hasAlphaOrClass);
      if (filtered.length > 0) {
        result.push({
          sample: filtered.sort(),
          resource_type: resourceType,
          annotation_type: data.annotationType,
        });
      }
    }

    return result;
  }
}

class ElPasoPatternSampleGenerator extends DefaultPatternSampleGenerator {
  private static readonly numericAliasRegex = /^\d{3}-\d{3}-\d{5}-\d{3}$/;
  private static readonly numericSample = "000-000-00000-000";

  getMessage(): string | undefined {
    return "Extended PatternSampleGenerator - Generating full-numeric file patterns and excluding full-numeric asset aliases";
  }

  prepareEntitiesForCache(entities: AnnotationEntity[]): AnnotationEntity[] {
    return removeFullNumericAssetAliases(entities);
  }

  generateFromEntities(entities: AnnotationEntity[]): PatternSample[] {
    const baseEntities = this.prepareEntitiesForCache(entities).map((entity) => {
      if (entity.annotation_type !== "diagrams.FileLink") return entity;
      const filteredAliases = entity.search_property.filter((aliasRaw) => /[A-Za-z]/.test(String(aliasRaw || "")));
      return { ...entity, search_property: filteredAliases };
    });

    const baseSamples = super.generateFromEntities(baseEntities);
    const merged: Record<string, { annotationType?: string; samples: Set<string> }> = {};

    const addSample = (resourceType: string, annotationType: string | undefined, sample: string) => {
      if (!resourceType || !sample) return;
      if (!merged[resourceType]) {
        merged[resourceType] = { annotationType, samples: new Set() };
      }

      merged[resourceType].samples.add(sample);
      if (!merged[resourceType].annotationType && annotationType) {
        merged[resourceType].annotationType = annotationType;
      }
    };

    for (const pattern of baseSamples) {
      const resourceType = pattern.resource_type || "";
      const annotationType = pattern.annotation_type;
      const samples = Array.isArray(pattern.sample)
        ? pattern.sample.map((value) => String(value))
        : pattern.sample != null
          ? [String(pattern.sample)]
          : [];

      for (const sample of samples) {
        addSample(resourceType, annotationType, sample);
      }
    }

    for (const entity of entities) {
      if (entity.annotation_type !== "diagrams.FileLink") continue;
      const resourceType = entity.resource_type || "";
      for (const aliasRaw of entity.search_property || []) {
        const alias = String(aliasRaw || "").trim();
        if (!alias) continue;
        if (alias === "PD-92-10178-009") {
          console.info("Pattern sample alias read", {
            alias,
            resourceType,
            externalId: entity.external_id,
          });
        }
        if (/[A-Za-z]/.test(alias)) continue;
        if (!ElPasoPatternSampleGenerator.numericAliasRegex.test(alias)) continue;

        addSample(resourceType, entity.annotation_type, ElPasoPatternSampleGenerator.numericSample);
      }
    }

    return Object.entries(merged)
      .map(([resourceType, data]) => ({
        resource_type: resourceType,
        annotation_type: data.annotationType,
        sample: Array.from(data.samples).sort(),
      }))
      .filter((item) => Array.isArray(item.sample) && item.sample.length > 0);
  }
}

function getPatternSampleGenerator(pipelineId?: string | null): PatternSampleGenerator {
  const suffix = (pipelineId || "").split("_").pop()?.toLowerCase();

  if (suffix === "elp") {
    return new ElPasoPatternSampleGenerator();
  }
  return new DefaultPatternSampleGenerator();
}

export interface CacheRow {
  key: string;
  columns: Record<string, unknown>;
}

export type ProgressCallback = (message: string, progress?: number) => void;

const DEFAULT_RETRY_ATTEMPTS = 5;
const DEFAULT_RETRY_DELAY_MS = 2000;
const CHUNK_SIZE = 1000;

async function insertRawRows(
  sdk: CogniteClient,
  dbName: string,
  tableName: string,
  items: Array<{ key: string; columns: Record<string, unknown> }>
) {
  try {
    await sdk.raw.insertRows(dbName, tableName, items);
  } catch (error) {
    await sdk.post(`/api/v1/projects/${sdk.project}/raw/dbs/${dbName}/tables/${tableName}/rows`, {
      data: { items },
    });
  }
}

async function callWithRetries<T>(
  fn: () => Promise<T>,
  options?: { maxAttempts?: number; delayMs?: number; onProgress?: ProgressCallback; label?: string }
): Promise<T> {
  const maxAttempts = options?.maxAttempts ?? DEFAULT_RETRY_ATTEMPTS;
  const delayMs = options?.delayMs ?? DEFAULT_RETRY_DELAY_MS;
  const label = options?.label || "Request";

  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      if (attempt > 1) {
        options?.onProgress?.(`${label}: retry ${attempt}/${maxAttempts}`);
      }
      return await fn();
    } catch (error) {
      const status = typeof error === "object" && error && "status" in error ? (error as { status?: number }).status : undefined;
      const code = typeof error === "object" && error && "code" in error ? (error as { code?: number }).code : undefined;
      const shouldRetry = status === 408 || status === 429 || code === 408 || code === 429;

      if (!shouldRetry || attempt >= maxAttempts) {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

function buildFilter(view: ViewConfig, filter: PipelineFilter): FilterDefinition | undefined {
  if (!filter.targetProperty) return undefined;
  const property = [view.schemaSpace, `${view.externalId}/${view.version}`, filter.targetProperty];
  const operator = (filter.operator || "Exists").toLowerCase();
  const values = Array.isArray(filter.values) ? filter.values : filter.values != null ? [filter.values] : [];

  let built: FilterDefinition | undefined;

  if (operator === "exists") {
    built = { exists: { property } };
  } else if (operator === "equals") {
    built = { equals: { property, value: values[0] } };
  } else if (operator === "in") {
    built = { in: { property, values } };
  } else if (operator === "containsall") {
    built = { containsAll: { property, values } };
  } else if (operator === "search") {
    built = { containsAll: { property, values } } as FilterDefinition;
  }

  if (!built) return undefined;
  if (filter.negate) return { not: built } as FilterDefinition;
  return built;
}

function buildFilterFromConfig(view: ViewConfig, filters?: PipelineFilter[]): FilterDefinition | undefined {
  if (!filters || filters.length === 0) return undefined;
  const compiled = filters.map((filter) => buildFilter(view, filter)).filter(Boolean) as FilterDefinition[];
  if (compiled.length === 0) return undefined;
  if (compiled.length === 1) return compiled[0];
  return { and: compiled } as FilterDefinition;
}

function buildFilterWithInstanceSpace(view: ViewConfig, filters?: PipelineFilter[]): FilterDefinition | undefined {
  const baseFilter = buildFilterFromConfig(view, filters);
  const instanceSpace = view.instanceSpace;
  if (!instanceSpace) return baseFilter;

  const spaceFilter: FilterDefinition = {
    equals: { property: ["node", "space"], value: instanceSpace },
  } as FilterDefinition;

  if (!baseFilter) return spaceFilter;
  return { and: [spaceFilter, baseFilter] } as FilterDefinition;
}

function applyInstanceSpaceFilter(
  view: ViewConfig,
  filterObj: FilterDefinition | undefined
): FilterDefinition | undefined {
  const instanceSpace = view.instanceSpace;
  if (!instanceSpace) return filterObj;

  const spaceFilter: FilterDefinition = {
    equals: { property: ["node", "space"], value: instanceSpace },
  } as FilterDefinition;

  if (!filterObj) return spaceFilter;
  return { and: [spaceFilter, filterObj] } as FilterDefinition;
}

function buildPropertyRef(view: ViewConfig, propertyName: string) {
  return [view.schemaSpace, `${view.externalId}/${view.version}`, propertyName];
}

function buildEqualsFilter(
  view: ViewConfig,
  propertyName: string | undefined,
  value: string | undefined
): FilterDefinition | undefined {
  if (!propertyName || !value) return undefined;
  return { equals: { property: buildPropertyRef(view, propertyName), value } } as FilterDefinition;
}

function buildInFilter(
  view: ViewConfig,
  propertyName: string,
  values: string[]
): FilterDefinition | undefined {
  if (!propertyName || values.length === 0) return undefined;
  return { in: { property: buildPropertyRef(view, propertyName), values } } as FilterDefinition;
}

function mergeAnd(filters: Array<FilterDefinition | undefined>): FilterDefinition | undefined {
  const parts = filters.filter(Boolean) as FilterDefinition[];
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0];
  return { and: parts } as FilterDefinition;
}

function mergeOr(filters: Array<FilterDefinition | undefined>): FilterDefinition | undefined {
  const parts = filters.filter(Boolean) as FilterDefinition[];
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0];
  return { or: parts } as FilterDefinition;
}

function buildSiteWideDetectScopedFilter(
  view: ViewConfig,
  filters: PipelineFilter[] | undefined,
  config: PipelineConfig,
  primaryValue: string | undefined,
  secondaryValue: string | undefined
): FilterDefinition | undefined {
  const filterEntities = buildFilterFromConfig(view, filters);
  const filterSiteWideDetect = buildInFilter(view, "tags", ["SiteWideDetect"]);

  if (!primaryValue) {
    return mergeOr([filterEntities, filterSiteWideDetect]);
  }

  const filterPrimary = buildEqualsFilter(view, config.primaryScopeProperty, primaryValue);
  const filterSecondary = buildEqualsFilter(view, config.secondaryScopeProperty, secondaryValue);
  const left = filterEntities ? mergeAnd([filterPrimary, filterSecondary, filterEntities]) : undefined;
  const right = filterPrimary && filterSiteWideDetect
    ? mergeAnd([filterPrimary, filterSiteWideDetect])
    : undefined;

  return mergeOr([left, right]) ?? filterEntities;
}

function buildDefaultScopedFilter(
  view: ViewConfig,
  filters: PipelineFilter[] | undefined,
  config: PipelineConfig,
  primaryValue: string | undefined,
  secondaryValue: string | undefined
): FilterDefinition | undefined {
  const filterEntities = buildFilterFromConfig(view, filters);
  if (!primaryValue) return filterEntities;

  const filterPrimary = buildEqualsFilter(view, config.primaryScopeProperty, primaryValue);
  const filterSecondary = buildEqualsFilter(view, config.secondaryScopeProperty, secondaryValue);

  return mergeAnd([filterPrimary, filterSecondary, filterEntities]);
}

function getElPasoLogisticsSecondaryValue(secondaryValue: string | undefined): string | undefined {
  if (!secondaryValue) return undefined;
  const normalized = secondaryValue.trim();
  if (normalized.length !== 4) return undefined;
  if (!normalized.startsWith("0") || !normalized.endsWith("0")) return undefined;
  return `${normalized.slice(0, 3)}L`;
}

function buildElPasoScopedFilter(
  view: ViewConfig,
  filters: PipelineFilter[] | undefined,
  config: PipelineConfig,
  primaryValue: string | undefined,
  secondaryValue: string | undefined
): FilterDefinition | undefined {
  const baseFilter = buildDefaultScopedFilter(view, filters, config, primaryValue, secondaryValue);
  const logisticsSecondary = getElPasoLogisticsSecondaryValue(secondaryValue);
  if (!primaryValue || !logisticsSecondary) return baseFilter;

  const filterEntities = buildFilterFromConfig(view, filters);
  const filterPrimary = buildEqualsFilter(view, config.primaryScopeProperty, primaryValue);
  const filterSecondary = buildEqualsFilter(view, config.secondaryScopeProperty, logisticsSecondary);
  const logisticsFilter = mergeAnd([filterPrimary, filterSecondary, filterEntities]);

  return mergeOr([baseFilter, logisticsFilter]) ?? baseFilter;
}

abstract class ScopeFilterBuilder {
  getMessage(): string | undefined {
    return undefined;
  }

  build(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined {
    const scopedFilter = this.buildScopedFilter(view, filters, config, primaryValue, secondaryValue);
    return applyInstanceSpaceFilter(view, scopedFilter);
  }

  protected abstract buildScopedFilter(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined;
}

class DefaultScopeFilterBuilder extends ScopeFilterBuilder {
  protected buildScopedFilter(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined {
    return buildDefaultScopedFilter(view, filters, config, primaryValue, secondaryValue);
  }
}

class SiteWideDetectScopeFilterBuilder extends ScopeFilterBuilder {
  getMessage(): string | undefined {
    return "Extended ScopeFilterBuilder - Using SiteWideDetect scope filter: include entities tagged SiteWideDetect on all scopes.";
  }

  protected buildScopedFilter(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined {
    return buildSiteWideDetectScopedFilter(view, filters, config, primaryValue, secondaryValue);
  }
}

class LosAngelesScopeFilterBuilder extends ScopeFilterBuilder {
  getMessage(): string | undefined {
    return "Extended ScopeFilterBuilder - Using Los Angeles scope filter: SiteWideDetect filter builder, but only for RPS units.";
  }

  protected buildScopedFilter(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined {
    const isRpsUnit = (secondaryValue || "").startsWith("RPS");
    if (isRpsUnit) {
      return buildSiteWideDetectScopedFilter(view, filters, config, primaryValue, secondaryValue);
    }
    return buildDefaultScopedFilter(view, filters, config, primaryValue, secondaryValue);
  }
}

class ElPasoScopeFilterBuilder extends ScopeFilterBuilder {
  getMessage(): string | undefined {
    return "Extended ScopeFilterBuilder - Using default scoped filter plus logistics-unit assets for El Paso South unit scopes.";
  }

  protected buildScopedFilter(
    view: ViewConfig,
    filters: PipelineFilter[] | undefined,
    config: PipelineConfig,
    primaryValue: string | undefined,
    secondaryValue: string | undefined
  ): FilterDefinition | undefined {
    return buildElPasoScopedFilter(view, filters, config, primaryValue, secondaryValue);
  }
}

function getScopeFilterBuilder(pipelineId?: string | null): ScopeFilterBuilder {
  const suffix = (pipelineId || "").split("_").pop()?.toLowerCase();

  switch (suffix) {
    case "ken":
    case "can":
      return new SiteWideDetectScopeFilterBuilder();
    case "lar":
      return new LosAngelesScopeFilterBuilder();
    case "elp":
      return new ElPasoScopeFilterBuilder();
    default:
      return new DefaultScopeFilterBuilder();
  }
}

function getScopeFilterBuilderForEntity(
  pipelineId: string | null | undefined,
  entityType: "asset" | "file"
): ScopeFilterBuilder {
  const suffix = (pipelineId || "").split("_").pop()?.toLowerCase();
  if (suffix === "elp" && entityType === "file") {
    return new DefaultScopeFilterBuilder();
  }
  return getScopeFilterBuilder(pipelineId);
}

function getInstanceProperties(instance: NodeOrEdge, view: ViewConfig): Record<string, unknown> | null {
  const properties = (instance as { properties?: Record<string, Record<string, unknown>> }).properties;
  if (!properties) return null;

  const viewKey = `${view.externalId}/${view.version}`;
  if (properties[view.schemaSpace] && properties[view.schemaSpace][viewKey]) {
    return properties[view.schemaSpace][viewKey] as Record<string, unknown>;
  }

  const allValues = Object.values(properties);
  if (allValues.length === 0) return null;
  const first = allValues[0];
  if (typeof first === "object" && first !== null) return first as Record<string, unknown>;
  return null;
}

async function listInstances(
  sdk: CogniteClient,
  view: ViewConfig,
  filter?: FilterDefinition
): Promise<NodeOrEdge[]> {
  const items: NodeOrEdge[] = [];
  let cursor: string | undefined;

  do {
    const response = await callWithRetries(
      () => sdk.instances.list({
        instanceType: "node",
        sources: [{ source: { type: "view", space: view.schemaSpace, externalId: view.externalId, version: view.version } }],
        filter,
        limit: CHUNK_SIZE,
        cursor,
      }),
      { label: "List instances" }
    );

    items.push(...response.items);
    cursor = response.nextCursor;
  } while (cursor);

  return items;
}

function convertInstancesToEntities(
  instances: NodeOrEdge[],
  view: ViewConfig,
  resourceProperty: string | undefined,
  searchProperty: string,
  entityType: "asset" | "file"
): AnnotationEntity[] {
  const entities: AnnotationEntity[] = [];

  for (const instance of instances) {
    const props = getInstanceProperties(instance, view);
    if (!props) continue;

    const resourceType = resourceProperty && props[resourceProperty] != null
      ? String(props[resourceProperty])
      : view.externalId;

    const searchValues = props[searchProperty];
    let searchList: string[] = [];
    if (Array.isArray(searchValues)) {
      searchList = searchValues.map((value) => String(value)).filter(Boolean);
    } else if (searchValues != null) {
      searchList = [String(searchValues)];
    } else if (props.name != null) {
      searchList = [String(props.name)];
    }

    entities.push({
      external_id: String(instance.externalId || ""),
      name: props.name as string | undefined,
      space: String(instance.space || view.instanceSpace || view.schemaSpace),
      annotation_type: entityType === "file" ? "diagrams.FileLink" : "diagrams.AssetLink",
      resource_type: resourceType,
      search_property: searchList,
    });
  }

  return entities;
}

async function fetchEntitiesForScope(
  sdk: CogniteClient,
  view: ViewConfig,
  filters: PipelineFilter[] | undefined,
  config: PipelineConfig,
  primaryValue: string,
  secondaryValue: string | undefined,
  entityType: "asset" | "file",
  scopeFilterBuilder: ScopeFilterBuilder
): Promise<AnnotationEntity[]> {
  const filterObj = scopeFilterBuilder.build(view, filters, config, primaryValue, secondaryValue);
  const instances = await listInstances(sdk, view, filterObj);

  const resourceProperty = entityType === "asset" ? config.assetResourceProperty : config.fileResourceProperty;
  const searchProperty = entityType === "asset" ? config.targetEntitiesSearchProperty : config.fileSearchProperty;

  return convertInstancesToEntities(instances, view, resourceProperty, searchProperty || "aliases", entityType);
}

function normalizePatternSamples(patterns: PatternSample[] | undefined): string[] {
  if (!patterns || patterns.length === 0) return [];

  const normalized: string[] = [];
  for (const pattern of patterns) {
    const resource = pattern.resource_type || "";
    const annotation = pattern.annotation_type || "";
    const samples = Array.isArray(pattern.sample)
      ? pattern.sample.map((value) => String(value))
      : pattern.sample != null
        ? [String(pattern.sample)]
        : [];

    for (const sample of samples) {
      normalized.push(`${sample}::${resource}::${annotation}`.toLowerCase());
    }
  }

  return normalized.sort();
}

function mergePatternSamples(autoSamples: PatternSample[], manualSamples: PatternSample[]): PatternSample[] {
  const merged: PatternSample[] = [];
  const seen = new Set<string>();

  const add = (pattern: PatternSample) => {
    const resource = pattern.resource_type || "";
    const annotation = pattern.annotation_type || "";
    const samples = Array.isArray(pattern.sample)
      ? pattern.sample.map((value) => String(value))
      : pattern.sample != null
        ? [String(pattern.sample)]
        : [];

    for (const sample of samples) {
      const key = `${sample}::${resource}::${annotation}`.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push({
        sample,
        resource_type: resource,
        annotation_type: annotation,
        created_by: pattern.created_by,
      });
    }
  };

  autoSamples.forEach(add);
  manualSamples.forEach(add);
  return merged;
}

function getManualPatternsForCacheKey(
  cacheKey: string,
  manualPatternsByScope: Record<string, PatternSample[]>
): PatternSample[] {
  const result: PatternSample[] = [];

  const add = (scope: string) => {
    const patterns = manualPatternsByScope[scope];
    if (!patterns) return;
    result.push(...patterns);
  };

  if (cacheKey) add(cacheKey);
  if (cacheKey.includes("_")) {
    const primary = cacheKey.split("_", 1)[0];
    if (primary) add(primary);
  }
  add(GLOBAL_SCOPE);

  return result;
}

export async function writeCacheRows(
  sdk: CogniteClient,
  config: PipelineConfig,
  rows: CacheRow[],
  batchSize = 10,
  onProgress?: ProgressCallback
): Promise<number> {
  if (!config.rawDb || !config.rawTablePatternCache || rows.length === 0) return 0;

  let totalWritten = 0;
  const totalBatches = Math.ceil(rows.length / batchSize);
  let batchIndex = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize).map((row) => ({ key: row.key, columns: row.columns }));
    await insertRawRows(sdk, config.rawDb, config.rawTablePatternCache, chunk);
    totalWritten += chunk.length;
    batchIndex += 1;
    const progress = Math.min(100, Math.round((totalWritten / rows.length) * 100));
    onProgress?.(
      `Writing cache rows: batch ${batchIndex}/${totalBatches} (+${chunk.length}) total ${totalWritten} row(s)`,
      progress
    );
  }

  return totalWritten;
}

export async function syncManualPatternsToCache(
  sdk: CogniteClient,
  config: PipelineConfig,
  manualPatterns: PatternRecord[],
  changedScopes?: string[],
  onProgress?: ProgressCallback
): Promise<number> {
  if (!config.rawDb || !config.rawTablePatternCache) return 0;

  const iterator = sdk.raw.listRows(config.rawDb, config.rawTablePatternCache, { limit: CHUNK_SIZE });

  const manualPatternsByScope = buildManualPatternsByScope(manualPatterns);
  const changed = new Set((changedScopes || []).filter(Boolean));

  let totalWritten = 0;
  const batchSize = 10;
  const pending: Array<{ key: string; columns: Record<string, unknown> }> = [];
  let batchIndex = 0;

  for await (const row of iterator) {
    const cacheKey = String(row.key || "");
    if (!cacheKey) continue;
    if (changed.size > 0 && !changed.has(GLOBAL_SCOPE) && !changed.has(cacheKey)) {
      const primary = cacheKey.includes("_") ? cacheKey.split("_", 1)[0] : cacheKey;
      if (!changed.has(primary)) continue;
    }

    const columns = { ...(row.columns as Record<string, unknown>) };

    const manualPatternSamples = getManualPatternsForCacheKey(cacheKey, manualPatternsByScope);
    const autoPatternSamples = [
      ...((columns.AssetPatternSamples as PatternSample[]) || []),
      ...((columns.FilePatternSamples as PatternSample[]) || []),
    ];

    const combinedPatternSamples = mergePatternSamples(autoPatternSamples, manualPatternSamples);

    const manualNormalized = normalizePatternSamples(columns.ManualPatternSamples as PatternSample[] | undefined);
    const combinedNormalized = normalizePatternSamples(columns.CombinedPatternSamples as PatternSample[] | undefined);

    const nextManual = normalizePatternSamples(manualPatternSamples);
    const nextCombined = normalizePatternSamples(combinedPatternSamples);

    if (
      manualNormalized.join("|") === nextManual.join("|") &&
      combinedNormalized.join("|") === nextCombined.join("|")
    ) {
      continue;
    }

    columns.ManualPatternSamples = manualPatternSamples;
    columns.CombinedPatternSamples = combinedPatternSamples;
    columns.LastUpdateTimeUtcIso = new Date().toISOString();

    pending.push({ key: cacheKey, columns });
    if (pending.length >= batchSize) {
      const batch = pending.splice(0, pending.length);
      await insertRawRows(sdk, config.rawDb, config.rawTablePatternCache, batch);
      totalWritten += batch.length;
      batchIndex += 1;
      onProgress?.(
        `annotation_entities_cache: wrote chunk ${batchIndex} (+${batch.length}) total ${totalWritten} row(s)`
      );
    }
  }

  if (pending.length > 0) {
    const finalCount = pending.length;
    await insertRawRows(sdk, config.rawDb, config.rawTablePatternCache, pending);
    totalWritten += finalCount;
    batchIndex += 1;
    onProgress?.(
      `annotation_entities_cache: wrote chunk ${batchIndex} (+${finalCount}) total ${totalWritten} row(s)`
    );
  }

  return totalWritten;
}

export async function discoverScopesGroupedByPrimary(
  sdk: CogniteClient,
  viewConfigs: Array<{ view: ViewConfig; filters?: PipelineFilter[] }>,
  primaryScopeProperty?: string,
  secondaryScopeProperty?: string,
  onProgress?: ProgressCallback
): Promise<Record<string, Record<string, number>>> {
  const grouped: Record<string, Record<string, number>> = {};
  const groupBy: string[] = [];

  if (primaryScopeProperty) groupBy.push(primaryScopeProperty);
  if (secondaryScopeProperty) groupBy.push(secondaryScopeProperty);
  if (groupBy.length === 0) return grouped;

  for (const pair of viewConfigs) {
    const { view, filters } = pair;
    if (!view) continue;

    const filterObj = buildFilterWithInstanceSpace(view, filters);
    if (onProgress) {
      onProgress(
        `Aggregate request view=${view.schemaSpace}/${view.externalId}@${view.version}, instanceSpace=${view.instanceSpace || "(none)"}, groupBy=${groupBy.join(",")}`
      );
    }

    const response = await callWithRetries(
      () => sdk.instances.aggregate({
        view: {
          externalId: view.externalId,
          space: view.schemaSpace,
          type: "view",
          version: view.version,
        },
        aggregates: [{ count: {} }],
        groupBy,
        filter: filterObj,
        limit: CHUNK_SIZE,
      }),
      { onProgress, label: "Aggregate scopes" }
    );

    for (const item of response.items || []) {
      const group = item.group || {};
      const primaryRaw = primaryScopeProperty ? group[primaryScopeProperty] : GLOBAL_SCOPE;
      const secondaryRaw = secondaryScopeProperty ? group[secondaryScopeProperty] : NONE_SCOPE;

      const primaryKey = primaryRaw != null && String(primaryRaw).trim() !== ""
        ? String(primaryRaw).trim()
        : GLOBAL_SCOPE;
      const secondaryKey = secondaryRaw != null && String(secondaryRaw).trim() !== ""
        ? String(secondaryRaw).trim()
        : NONE_SCOPE;

      const countAggregate = item.aggregates?.find(
        (agg) => agg.aggregate === "count" && "value" in agg
      );
      const countValue = countAggregate && "value" in countAggregate ? countAggregate.value : 0;

      grouped[primaryKey] = grouped[primaryKey] || {};
      grouped[primaryKey][secondaryKey] = (grouped[primaryKey][secondaryKey] || 0) + Number(countValue || 0);
    }
  }

  return grouped;
}

export function buildScopePreviewMerged(
  fileGrouped: Record<string, Record<string, number>>,
  assetGrouped: Record<string, Record<string, number>>
) {
  const rows: Array<{
    patternScope: string;
    primaryScopeValue: string;
    secondaryScopeValue?: string;
    fileEntities: number;
    assetEntities: number;
    queryLabel: string;
  }> = [];

  const primaryKeys = new Set<string>([...Object.keys(fileGrouped), ...Object.keys(assetGrouped)]);

  for (const primary of primaryKeys) {
    const fileSecondary = Object.keys(fileGrouped[primary] || {});
    const assetSecondary = Object.keys(assetGrouped[primary] || {});
    const secondaryKeys = new Set<string>([...fileSecondary, ...assetSecondary]);

    if (secondaryKeys.size === 0) {
      secondaryKeys.add(NONE_SCOPE);
    }

    for (const secondary of secondaryKeys) {
      const fileCount = fileGrouped[primary]?.[secondary] || 0;
      const assetCount = assetGrouped[primary]?.[secondary] || 0;
      const hasSecondary = secondary && secondary !== NONE_SCOPE;
      const scopeKey = hasSecondary ? `${primary}_${secondary}` : primary;
      const queryLabel = `primary=${primary},secondary=${hasSecondary ? secondary : ""}`;

      rows.push({
        patternScope: scopeKey,
        primaryScopeValue: primary,
        secondaryScopeValue: hasSecondary ? secondary : undefined,
        fileEntities: fileCount,
        assetEntities: assetCount,
        queryLabel,
      });
    }
  }

  return rows;
}

export async function buildCachePreviewRows(
  sdk: CogniteClient,
  config: PipelineConfig,
  entries: Array<{ primaryScopeValue: string; secondaryScopeValue?: string }>,
  manualPatterns: PatternRecord[],
  viewPairs: Array<{ view: ViewConfig; filters?: PipelineFilter[]; entityType: "asset" | "file" }>,
  onProgress?: ProgressCallback,
  pipelineId?: string | null
): Promise<CacheRow[]> {
  const manualPatternsByScope = buildManualPatternsByScope(manualPatterns);
  const patternSampleGenerator = getPatternSampleGenerator(pipelineId);
  const assetScopeFilterBuilder = getScopeFilterBuilderForEntity(pipelineId, "asset");
  const fileScopeFilterBuilder = getScopeFilterBuilderForEntity(pipelineId, "file");
  const rows: CacheRow[] = [];

  const generatorMessage = patternSampleGenerator.getMessage();
  if (generatorMessage) {
    onProgress?.(generatorMessage);
  }

  const assetScopeFilterMessage = assetScopeFilterBuilder.getMessage();
  if (assetScopeFilterMessage) {
    onProgress?.(assetScopeFilterMessage);
  }

  const fileScopeFilterMessage = fileScopeFilterBuilder.getMessage();
  if (fileScopeFilterMessage && fileScopeFilterMessage !== assetScopeFilterMessage) {
    onProgress?.(fileScopeFilterMessage);
  }

  const assetPair = viewPairs.find((pair) => pair.entityType === "asset");
  const filePair = viewPairs.find((pair) => pair.entityType === "file");

  const total = entries.length || 1;
  let processed = 0;

  for (const entry of entries) {
    processed += 1;
    const primaryValue = entry.primaryScopeValue || GLOBAL_SCOPE;
    const secondaryValue = entry.secondaryScopeValue || NONE_SCOPE;
    const scopeKey = secondaryValue !== NONE_SCOPE ? `${primaryValue}_${secondaryValue}` : primaryValue;

    onProgress?.(`Building preview ${processed}/${total} (${scopeKey})`, Math.round((processed / total) * 100));

    const rawAssetEntities = assetPair?.view
      ? await fetchEntitiesForScope(
          sdk,
          assetPair.view,
          assetPair.filters,
          config,
          primaryValue,
          secondaryValue === NONE_SCOPE ? undefined : secondaryValue,
          "asset",
          assetScopeFilterBuilder
        )
      : [];
    const assetEntities = patternSampleGenerator.prepareEntitiesForCache(rawAssetEntities);

    const fileEntities = filePair?.view
      ? await fetchEntitiesForScope(
          sdk,
          filePair.view,
          filePair.filters,
          config,
          primaryValue,
          secondaryValue === NONE_SCOPE ? undefined : secondaryValue,
          "file",
          fileScopeFilterBuilder
        )
      : [];

    const assetPatternSamples = patternSampleGenerator.generateFromEntities(assetEntities);
    const filePatternSamples = patternSampleGenerator.generateFromEntities(fileEntities);
    const autoPatternSamples = [...assetPatternSamples, ...filePatternSamples];
    const manualPatternSamples = getManualPatternsForCacheKey(scopeKey, manualPatternsByScope);
    const combinedPatternSamples = mergePatternSamples(autoPatternSamples, manualPatternSamples);

    rows.push({
      key: scopeKey,
      columns: {
        AssetEntities: assetEntities,
        FileEntities: fileEntities,
        AssetPatternSamples: assetPatternSamples,
        FilePatternSamples: filePatternSamples,
        ManualPatternSamples: manualPatternSamples,
        CombinedPatternSamples: combinedPatternSamples,
        LastUpdateTimeUtcIso: new Date().toISOString(),
      },
    });
  }

  if (entries.length > 0) {
    onProgress?.("Preview rows ready.", 100);
  }

  return rows;
}

export function buildCachePreviewSummary(rows: CacheRow[]): CachePreviewSummary[] {
  return rows.map((row) => {
    const columns = row.columns || {};
    const assetEntities = Array.isArray(columns.AssetEntities) ? columns.AssetEntities.length : 0;
    const fileEntities = Array.isArray(columns.FileEntities) ? columns.FileEntities.length : 0;
    const assetPatternSamples = Array.isArray(columns.AssetPatternSamples) ? columns.AssetPatternSamples.length : 0;
    const filePatternSamples = Array.isArray(columns.FilePatternSamples) ? columns.FilePatternSamples.length : 0;
    const manualPatternSamples = Array.isArray(columns.ManualPatternSamples) ? columns.ManualPatternSamples.length : 0;
    const combinedPatternSamples = Array.isArray(columns.CombinedPatternSamples) ? columns.CombinedPatternSamples.length : 0;
    const lastUpdate = typeof columns.LastUpdateTimeUtcIso === "string" ? columns.LastUpdateTimeUtcIso : undefined;

    return {
      patternScope: row.key,
      assetEntities,
      fileEntities,
      assetPatternSamples,
      filePatternSamples,
      manualPatternSamples,
      combinedPatternSamples,
      lastUpdate,
    };
  });
}

export function normalizeAnnotationType(value?: string): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  const lower = trimmed.toLowerCase();
  if (trimmed === "File" || lower === "file" || lower === "diagrams.filelink") return "File";
  if (trimmed === "Asset" || lower === "asset" || lower === "diagrams.assetlink") return "Asset";
  return trimmed;
}

export function toAnnotationTypeApi(value?: string): string | undefined {
  if (!value) return undefined;
  if (value === "File") return "diagrams.FileLink";
  if (value === "Asset") return "diagrams.AssetLink";
  return value;
}

export function parseCsvToPreview(text: string, defaultScope?: string | null): PatternDraft[] {
  const rows = parseCsv(text);
  if (rows.length === 0) return [];
  const headers = rows[0].map((h) => h.trim());
  const dataRows = rows.slice(1).filter((r) => r.some((cell) => cell.trim() !== ""));

  const patternsCol = findColumn(headers, ["patterns", "pattern"]);
  const scopeCol = findColumn(headers, ["pattern_scope", "pattern scope", "scope", "key"]);

  const preview: PatternDraft[] = [];

  if (patternsCol) {
    const patternsIndex = headers.indexOf(patternsCol);
    const scopeIndex = scopeCol ? headers.indexOf(scopeCol) : -1;

    for (const row of dataRows) {
      const scopeValue = scopeIndex >= 0 ? row[scopeIndex]?.trim() : "";
      const scope = scopeValue || defaultScope || "";
      if (!scope) {
        throw new Error("CSV row missing scope and no default scope provided.");
      }

      const rawPatterns = row[patternsIndex] ?? "";
      if (!rawPatterns) continue;

      const patternList = parsePatternCell(rawPatterns);
      if (!patternList) continue;

      for (const pattern of patternList) {
        const sampleValue = extractValue(pattern, ["sample", "pattern", "value", "text"]);
        if (!sampleValue) continue;

        const annotationType = normalizeAnnotationType(
          extractValue(pattern, ["annotation_type", "annotationType", "entity_type", "type"]) || ""
        );
        const resourceType = extractValue(pattern, ["resource_type", "resourceType"]) || "";

        preview.push({
          sample: String(sampleValue).trim(),
          resourceType: String(resourceType || "").trim(),
          annotationType: annotationType || "",
          patternScope: scope,
        });
      }
    }

    return preview;
  }

  const sampleCol = findColumn(headers, ["sample", "pattern", "value", "text"]);
  const annotationCol = findColumn(headers, ["annotation_type", "annotation type", "entity_type", "type"]);
  const resourceCol = findColumn(headers, ["resource_type", "resource type", "resourceType"]);

  if (!sampleCol) {
    return [];
  }

  for (const row of dataRows) {
    const scopeValue = scopeCol ? row[headers.indexOf(scopeCol)]?.trim() : "";
    const scope = scopeValue || defaultScope || "";
    if (!scope) {
      throw new Error("CSV row missing scope and no default scope provided.");
    }

    const sampleValue = row[headers.indexOf(sampleCol)]?.trim();
    if (!sampleValue) continue;

    const annotationValue = annotationCol
      ? normalizeAnnotationType(row[headers.indexOf(annotationCol)]?.trim())
      : undefined;
    const resourceValue = resourceCol ? row[headers.indexOf(resourceCol)]?.trim() : "";

    preview.push({
      sample: sampleValue,
      resourceType: resourceValue || "",
      annotationType: annotationValue || "",
      patternScope: scope,
    });
  }

  return preview;
}

export function mergePatternDrafts(existing: PatternDraft[], incoming: PatternDraft[]): PatternDraft[] {
  const seen = new Set<string>();
  const result: PatternDraft[] = [];

  const add = (draft: PatternDraft) => {
    const key = `${draft.patternScope}::${draft.sample}::${draft.resourceType}::${draft.annotationType}`.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    result.push(draft);
  };

  existing.forEach(add);
  incoming.forEach(add);
  return result;
}

export function buildManualPatternsByScope(patterns: PatternRecord[]): Record<string, PatternSample[]> {
  const byScope: Record<string, PatternSample[]> = {};

  for (const pattern of patterns) {
    if (!pattern.patternScope) continue;
    if (!byScope[pattern.patternScope]) byScope[pattern.patternScope] = [];
    byScope[pattern.patternScope].push({
      sample: pattern.sample,
      resource_type: pattern.resourceType,
      annotation_type: toAnnotationTypeApi(pattern.annotationType) || pattern.annotationType,
    });
  }

  return byScope;
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let current = "";
  let inQuotes = false;
  let row: string[] = [];

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(current);
      current = "";
      continue;
    }

    if (char === "\n" && !inQuotes) {
      row.push(current);
      rows.push(row);
      row = [];
      current = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    current += char;
  }

  row.push(current);
  rows.push(row);

  return rows.filter((cells) => cells.some((cell) => cell.trim() !== ""));
}

function findColumn(headers: string[], candidates: string[]): string | undefined {
  const lowered = headers.map((header) => header.toLowerCase().trim());
  for (const candidate of candidates) {
    const index = lowered.indexOf(candidate.toLowerCase());
    if (index >= 0) return headers[index];
  }
  return undefined;
}

function parsePatternCell(value: string): Array<Record<string, string>> | null {
  try {
    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) return null;
    return parsed as Array<Record<string, string>>;
  } catch {
    return null;
  }
}

function extractValue(values: Record<string, string>, keys: string[]): string | undefined {
  for (const key of keys) {
    if (values[key]) return values[key];
  }
  return undefined;
}
