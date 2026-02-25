/**
 * Classic CDF model analysis: metadata field distribution.
 * Assets, time series, events, sequences, and files with count and sort by count descending.
 */

const CDF_FILTER_MAX_LEN = 64;
/** Documents aggregate allows longer filter values for type/author/source. */
const DOCUMENTS_FILTER_MAX_LEN = 512;

export type ResourceType = "assets" | "timeseries" | "events" | "sequences" | "files";

/** For filtering by dataset (id or externalId). */
export type DataSetIdEither = { id?: number; externalId?: string };

export interface AnalysisRow {
  count: number;
  text: string;
  /** For colored UI: filter key and value line (e.g. "Type: well\n") */
  filterKeyPart?: string;
  /** For colored UI: count line (e.g. "Count: 43\n") or empty */
  countPart?: string;
  /** For colored UI: metadata keys line (e.g. "Metadata keys: [a, b]\n\n") */
  metadataKeysPart?: string;
}

export interface AnalysisResult {
  resourceType: ResourceType;
  filterKey: string;
  rows: AnalysisRow[];
  error?: string;
}

interface CogniteClientLike {
  post: (path: string, body: Record<string, unknown>) => Promise<{ items?: unknown[] }>;
  get?: (path: string, params?: Record<string, unknown>) => Promise<{ items?: unknown[] }>;
}

/** Build CDF API path with project: e.g. /api/v1/projects/{project}/assets/aggregate */
function projectPath(project: string, resource: string): string {
  return `/api/v1/projects/${encodeURIComponent(project)}/${resource}/aggregate`;
}

/** Build CDF API path for a resource (no /list). E.g. /api/v1/projects/{project}/transformations */
function projectResourcePath(project: string, segment: string): string {
  return `/api/v1/projects/${encodeURIComponent(project)}/${segment}`;
}

/** Build CDF API list path for POST list. E.g. .../transformations/list */
function projectListPath(project: string, segment: string): string {
  return `/api/v1/projects/${encodeURIComponent(project)}/${segment}`;
}

/** Max limit for list endpoints (API cap is 1000). */
const LIST_LIMIT = 1000;

/** Parse list response: items array (API returns { items: [...] }; SDK may nest under .data or use .value/.result). */
function parseListResponse(res: unknown): unknown[] {
  if (res == null || typeof res !== "object") return [];
  const r = res as Record<string, unknown>;
  let items: unknown[] | undefined;
  if (Array.isArray(r.items)) items = r.items;
  if (items == null && r.data != null && typeof r.data === "object") {
    const d = r.data as Record<string, unknown>;
    if (Array.isArray(d)) return d;
    if (Array.isArray(d.items)) items = d.items;
    else if (d.data != null && typeof d.data === "object" && Array.isArray((d.data as Record<string, unknown>).items))
      items = (d.data as Record<string, unknown>).items as unknown[];
  }
  if (items != null) return items;
  for (const key of ["value", "result", "data"] as const) {
    const v = r[key];
    if (Array.isArray(v)) return v;
    if (v != null && typeof v === "object" && Array.isArray((v as Record<string, unknown>).items))
      return (v as Record<string, unknown>).items as unknown[];
  }
  if (Array.isArray(r)) return r;
  return [];
}

/** Get count via POST .../list only. Use for APIs that return 400 on GET (e.g. workflows). */
async function getListCountPostOnly(
  client: CogniteClientLike,
  project: string,
  listSegment: string,
  body?: Record<string, unknown>
): Promise<number> {
  const path = projectListPath(project, listSegment);
  const payload = { limit: LIST_LIMIT, ...body };
  const res = await client.post(path, payload);
  return parseListResponse(res).length;
}

/** Transformations API: no POST /list (404). Use GET on .../transformations/ with query params. */
async function getTransformationsCount(
  client: CogniteClientLike,
  project: string
): Promise<number> {
  if (typeof client.get !== "function") return 0;
  const path = projectResourcePath(project, "transformations/");
  const res = await client.get(path, { limit: LIST_LIMIT, includePublic: true });
  return parseListResponse(res).length;
}

/** Workflows API: POST .../workflows/list returns 404. Use GET on .../workflows/ with query params (same pattern as transformations). */
async function getWorkflowsCount(
  client: CogniteClientLike,
  project: string
): Promise<number> {
  if (typeof client.get !== "function") return 0;
  const path = projectResourcePath(project, "workflows/");
  const res = await client.get(path, { limit: LIST_LIMIT });
  return parseListResponse(res).length;
}

/** Documents API: filter must use DSL. Use filter with property ["sourceFile", "datasetId"] and and/or/in (no dataSetIds). */
function documentsDataSetFilter(dataSetIds: DataSetIdEither[] | undefined): Record<string, unknown> {
  if (!dataSetIds?.length) return {};
  const numericIds = dataSetIds.map((x) => x.id).filter((n): n is number => typeof n === "number" && Number.isFinite(n));
  if (numericIds.length === 0) return {};
  const inClause = { in: { property: ["sourceFile", "datasetId"], values: numericIds } };
  return {
    filter: {
      and: [{ and: [{ or: [inClause] }] }],
    },
  };
}

/** CDF aggregate endpoints use advancedFilter with property ["dataSetId"] and "in" for dataset scope. Use numeric ids. */
function dataSetFilterAggregate(dataSetIds: DataSetIdEither[] | undefined): Record<string, unknown> {
  if (!dataSetIds?.length) return {};
  const numericIds = dataSetIds.map((x) => x.id).filter((n): n is number => typeof n === "number" && Number.isFinite(n));
  if (!numericIds.length) return {};
  return {
    advancedFilter:
      numericIds.length === 1
        ? { equals: { property: ["dataSetId"], value: numericIds[0] } }
        : { in: { property: ["dataSetId"], values: numericIds } },
  };
}

function getPropertyPath(filterKey: string, resourceType: ResourceType): unknown[] {
  if (resourceType === "events") return ["type"];
  return ["metadata", filterKey];
}

/** Resource name for API path (files -> documents). */
function aggregateResource(resourceType: ResourceType): string {
  return resourceType === "files" ? "documents" : resourceType;
}

/** Total count for resource (used for primary filter key counts). Exported for deep analysis. */
export async function getTotalCount(
  client: CogniteClientLike,
  project: string,
  resourceType: ResourceType,
  dataSetIds: DataSetIdEither[] | undefined
): Promise<number> {
  const path = projectPath(project, aggregateResource(resourceType));
  const body: Record<string, unknown> =
    resourceType === "files"
      ? { aggregate: "count", ...documentsDataSetFilter(dataSetIds) }
      : { aggregate: "count", ...dataSetFilterAggregate(dataSetIds) };
  const res = await client.post(path, body);
  return parseCountResponse(res);
}

/** Parse aggregate count response from various shapes (API returns { items: [{ count: N }] }; SDK may nest under .data). */
function parseCountResponse(res: unknown): number {
  if (res == null || typeof res !== "object") return 0;
  const r = res as Record<string, unknown>;
  let items = r.items;
  if (!Array.isArray(items) && r.data != null && typeof r.data === "object") {
    const d = r.data as Record<string, unknown>;
    items = d.items ?? (d.data as Record<string, unknown> | undefined)?.items;
  }
  if (!Array.isArray(items) || items.length === 0) return 0;
  const first = items[0] as { count?: number };
  return typeof first?.count === "number" ? first.count : 0;
}

/** Counts per resource type for a single dataset (for dataset table). */
export interface DatasetResourceCounts {
  assets: number;
  timeseries: number;
  events: number;
  sequences: number;
  files: number;
}

/**
 * advancedFilter shape that matches CDF Data Explorer for dataSetId filtering.
 * Uses in + values array wrapped in and/or so the backend applies the filter.
 */
function dataSetIdAdvancedFilter(numericIds: number[]): Record<string, unknown> {
  if (numericIds.length === 0) return {};
  const inClause = { in: { property: ["dataSetId"], values: numericIds } };
  return {
    advancedFilter: {
      and: [{ and: [{ or: [inClause] }] }],
    },
  };
}

/**
 * Build request body for aggregate count scoped to one dataset.
 * Uses Data Explorer–style advancedFilter (and/or/in with values array). Files/documents use filter only.
 */
function countBodyForDataset(dataSetId: DataSetIdEither): Record<string, unknown> {
  const numericId = typeof dataSetId.id === "number" && Number.isFinite(dataSetId.id) ? dataSetId.id : null;
  const hasExternalId = dataSetId.externalId != null && String(dataSetId.externalId).trim() !== "";

  if (numericId != null) {
    return {
      ...dataSetIdAdvancedFilter([numericId]),
    };
  }
  if (hasExternalId) {
    return {};
  }
  return { aggregate: "count" };
}

/**
 * Documents aggregate: CDF uses filter (not advancedFilter) with property ["sourceFile", "datasetId"] and and/or/in DSL.
 */
function countBodyForDatasetDocuments(dataSetId: DataSetIdEither): Record<string, unknown> {
  const numericId = typeof dataSetId.id === "number" && Number.isFinite(dataSetId.id) ? dataSetId.id : null;
  if (numericId == null) return { aggregate: "count" };
  const inClause = { in: { property: ["sourceFile", "datasetId"], values: [numericId] } };
  return {
    filter: {
      and: [{ and: [{ or: [inClause] }] }],
    },
    aggregate: "count",
  };
}

/**
 * Aggregate count with no filter (for testing). Paces one request at a time.
 * Returns count for the given resource type.
 */
export async function getAggregateCountNoFilter(
  client: CogniteClientLike,
  project: string,
  resourceType: ResourceType
): Promise<number> {
  const path = projectPath(project, aggregateResource(resourceType));
  const res = await client.post(path, { aggregate: "count" });
  return parseCountResponse(res);
}

/** Counts from list endpoints (no aggregate): transformations, functions, workflows, raw tables. */
export interface GlobalExtendedCounts {
  transformations: number;
  functions: number;
  workflows: number;
  rawTables: number;
}

const RAW_DBS_LIST_CAP = 50;
const RAW_TABLES_PER_DB_LIMIT = 1000;

/**
 * Fetch global counts from list APIs (transformations, functions, workflows, raw tables).
 * Each resource is fetched in its own try/catch so one failure (e.g. workflows 400) doesn't zero out the rest.
 */
export async function getGlobalExtendedCounts(
  client: CogniteClientLike,
  project: string
): Promise<GlobalExtendedCounts> {
  let transformations = 0;
  let functions = 0;
  let workflows = 0;
  try {
    transformations = await getTransformationsCount(client, project);
  } catch {
    // keep 0
  }
  try {
    functions = await getListCountPostOnly(client, project, "functions/list");
  } catch {
    // keep 0
  }
  try {
    workflows = await getWorkflowsCount(client, project);
  } catch {
    // keep 0
  }

  let rawTables = 0;
  try {
    if (typeof client.get === "function") {
      const dbsPath = projectResourcePath(project, "raw/dbs");
      const dbsRes = await client.get(dbsPath, { limit: RAW_DBS_LIST_CAP });
      const dbs = parseListResponse(dbsRes) as Array<{ name?: string }>;
      const dbNames = dbs.map((db) => (db?.name != null ? String(db.name) : "")).filter(Boolean);
      for (const db of dbNames) {
        const tablesPath = projectResourcePath(project, `raw/dbs/${encodeURIComponent(db)}/tables`);
        const tablesRes = await client.get(tablesPath, { limit: RAW_TABLES_PER_DB_LIMIT });
        rawTables += parseListResponse(tablesRes).length;
      }
    } else {
      const dbsPath = projectListPath(project, "raw/dbs/list");
      const dbsRes = await client.post(dbsPath, { limit: RAW_DBS_LIST_CAP });
      const dbs = parseListResponse(dbsRes) as Array<{ name?: string }>;
      const dbNames = dbs.map((db) => (db?.name != null ? String(db.name) : "")).filter(Boolean);
      for (const db of dbNames) {
        const tablesPath = projectListPath(project, `raw/dbs/${encodeURIComponent(db)}/tables/list`);
        const tablesRes = await client.post(tablesPath, { limit: RAW_TABLES_PER_DB_LIMIT });
        rawTables += parseListResponse(tablesRes).length;
      }
    }
  } catch {
    rawTables = 0;
  }

  return { transformations, functions, workflows, rawTables };
}

/** Get resource counts for one dataset. Runs requests sequentially to respect aggregate concurrency limits. */
export async function getDatasetResourceCounts(
  client: CogniteClientLike,
  project: string,
  dataSetId: DataSetIdEither
): Promise<DatasetResourceCounts> {
  const pathAssets = projectPath(project, "assets");
  const pathTimeseries = projectPath(project, "timeseries");
  const pathEvents = projectPath(project, "events");
  const pathSequences = projectPath(project, "sequences");
  const pathDocuments = projectPath(project, "documents");

  const countBody = countBodyForDataset(dataSetId);
  const documentsBody = countBodyForDatasetDocuments(dataSetId);

  const assetsRes = await client.post(pathAssets, countBody);
  const timeseriesRes = await client.post(pathTimeseries, countBody);
  const eventsRes = await client.post(pathEvents, countBody);
  const sequencesRes = await client.post(pathSequences, countBody);
  const filesRes = await client.post(pathDocuments, documentsBody);

  return {
    assets: parseCountResponse(assetsRes),
    timeseries: parseCountResponse(timeseriesRes),
    events: parseCountResponse(eventsRes),
    sequences: parseCountResponse(sequencesRes),
    files: parseCountResponse(filesRes),
  };
}

/** CDF aggregate API uses advancedFilter (not filter) for equals/in DSL. Value can be string or boolean (e.g. for isStep/isString). */
function advancedFilterEquals(property: unknown[], value: string | boolean): Record<string, unknown> {
  return { advancedFilter: { equals: { property, value } } };
}

/** Documents aggregate API uses filter (not advancedFilter) for equals. */
function filterEquals(property: unknown[], value: string | boolean): Record<string, unknown> {
  return { filter: { equals: { property, value } } };
}

const VALUE_TOO_LONG_PREVIEW_LEN = 1024;

function valueTooLongMetaPart(value: string): string {
  const preview = value.slice(0, VALUE_TOO_LONG_PREVIEW_LEN);
  const suffix = value.length > VALUE_TOO_LONG_PREVIEW_LEN ? "..." : "";
  return `Metadata keys: (value too long to list; first ${VALUE_TOO_LONG_PREVIEW_LEN} chars: ${preview}${suffix})\n\n`;
}

/** uniqueValues items can have value/values (string, boolean, number, etc.); normalize to string. */
function itemValue(
  item: { value?: unknown; values?: unknown[]; count?: number }
): { value: string; count: number } | null {
  const raw = item.value ?? (Array.isArray(item.values) ? item.values[0] : undefined);
  if (raw == null) return null;
  const value =
    typeof raw === "string"
      ? raw
      : typeof raw === "number" || typeof raw === "boolean"
        ? String(raw)
        : typeof raw === "object"
          ? JSON.stringify(raw)
          : String(raw);
  return { value, count: item.count ?? 0 };
}

/** Time series: primary fields use top-level property; everything else (including "type") is metadata. */
function getTimeseriesPropertyPath(filterKey: string): string[] {
  const n = filterKey.trim().toLowerCase().replace(/\s+/g, " ");
  if (n === "is step" || n === "isstep") return ["isStep"];
  if (n === "is string" || n === "isstring") return ["isString"];
  if (n === "unit" || n === "units") return ["unit"];
  return ["metadata", filterKey.trim()];
}

/** Documents (Files) aggregate: type, labels, author, source, or metadata key under sourceFile. */
function getDocumentsPropertyPath(filterKey: string): string[] {
  const n = filterKey.trim().toLowerCase();
  if (n === "type") return ["type"];
  if (n === "labels") return ["labels"];
  if (n === "author") return ["author"];
  if (n === "source") return ["sourceFile", "source"];
  return ["sourceFile", "metadata", filterKey.trim()];
}

/** Extract metadata key names from uniqueProperties response (items have values[].property or legacy property). */
function uniquePropertiesKeys(
  items: Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>
): string[] {
  return items
    .flatMap((p) => {
      const prop = p.property ?? p.values?.[0]?.property;
      return prop?.length ? [prop.slice(-1)[0]!] : [];
    })
    .filter(Boolean);
}

/** Documents uniqueProperties returns items with values: ["keyName"] (string array), not property. */
function uniquePropertiesKeysDocuments(
  items: Array<{ values?: string[] | Array<{ property?: string[] }>; property?: string[] }>
): string[] {
  return items
    .flatMap((p) => {
      const first = p.values?.[0];
      if (typeof first === "string") return [first];
      const prop = p.property ?? (first as { property?: string[] } | undefined)?.property;
      return prop?.length ? [prop.slice(-1)[0]!] : [];
    })
    .filter(Boolean);
}

export async function runAssetAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  const resourceType = "assets";
  const path = projectPath(project, "assets");
  const results: AnalysisRow[] = [];
  const filterPart = dataSetFilterAggregate(dataSetIds);
  try {
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: getPropertyPath(filterKey, resourceType) }],
      ...filterPart,
    });
    const items = (uv.items || []) as Array<{ value?: string; values?: string[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      if (value.length > CDF_FILTER_MAX_LEN) {
        keys = [];
      } else {
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(["metadata", filterKey], value),
          ...filterPart,
        });
        keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : valueTooLongMetaPart(value);
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function runTimeseriesAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  const resourceType = "timeseries";
  const path = projectPath(project, "timeseries");
  const results: AnalysisRow[] = [];
  const propPath = getTimeseriesPropertyPath(filterKey);
  const filterPart = dataSetFilterAggregate(dataSetIds);
  try {
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: propPath }],
      ...filterPart,
    });
    const items = (uv.items || []) as Array<{ value?: string; values?: string[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      if (value.length > CDF_FILTER_MAX_LEN) {
        keys = [];
      } else {
        const filterValue: string | boolean =
          (propPath[0] === "isStep" || propPath[0] === "isString") && (value === "true" || value === "false")
            ? value === "true"
            : value;
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(propPath, filterValue),
          ...filterPart,
        });
        keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : valueTooLongMetaPart(value);
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function runEventsAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  const resourceType = "events";
  const path = projectPath(project, "events");
  const results: AnalysisRow[] = [];
  const CDF_MAX = CDF_FILTER_MAX_LEN;
  const filterPart = dataSetFilterAggregate(dataSetIds);
  try {
    if (filterKey === "metadata") {
      const allMeta = await client.post(path, {
        aggregate: "uniqueProperties",
        path: ["metadata"],
        ...filterPart,
      });
      const metaFields = [...new Set(uniquePropertiesKeys((allMeta.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>))].sort();
      for (const metaField of metaFields) {
        let sampleValue: string | null = null;
        try {
          const sv = await client.post(path, {
            aggregate: "uniqueValues",
            properties: [{ property: ["metadata", metaField] }],
            ...filterPart,
          });
          const vals = (sv.items || []) as Array<{ value?: string; values?: string[] }>;
          const first = vals.find((v) => {
            const x = v.value ?? v.values?.[0];
            return x != null && String(x).length <= CDF_MAX;
          });
          sampleValue = first ? (first.value ?? first.values?.[0] ?? null) : null;
        } catch {
          sampleValue = null;
        }
        if (sampleValue == null) {
          const t = `Metadata field: ${metaField}\n (skipped - no filterable value)\n\n`;
          results.push({
            count: 0,
            text: t,
            filterKeyPart: `Metadata field: ${metaField}\n`,
            countPart: "",
            metadataKeysPart: " (skipped - no filterable value)\n\n",
          });
          continue;
        }
        let count: number | null = null;
        try {
          const cr = await client.post(path, {
            aggregate: "count",
            ...advancedFilterEquals(["metadata", metaField], sampleValue),
            ...filterPart,
          });
          count = (cr as { items?: Array<{ count?: number }> }).items?.[0]?.count ?? null;
        } catch {
          count = null;
        }
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(["metadata", metaField], sampleValue),
          ...filterPart,
        });
        const keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
        const countStr = count != null ? `Count: ${count}\n` : "";
        const metaPart = `Metadata keys: [${keys.join(", ")}]\n\n`;
        results.push({
          count: count ?? 0,
          text: `Metadata field: ${metaField}\n${countStr}${metaPart}`,
          filterKeyPart: `Metadata field: ${metaField}\n`,
          countPart: countStr,
          metadataKeysPart: metaPart,
        });
      }
      results.sort((a, b) => b.count - a.count);
      return { resourceType, filterKey, rows: results };
    }
    const eventPropPath = filterKey === "type" ? ["type"] : ["metadata", filterKey];
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: eventPropPath }],
      ...filterPart,
    });
    const items = (uv.items || []) as Array<{ value?: string; values?: string[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      if (value.length > CDF_MAX) {
        keys = [];
      } else {
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(eventPropPath, value),
          ...filterPart,
        });
        keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : valueTooLongMetaPart(value);
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function runFileAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string
): Promise<AnalysisResult> {
  const resourceType = "files";
  const path = projectPath(project, "files");
  const results: AnalysisRow[] = [];
  try {
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: getPropertyPath(filterKey, resourceType) }],
    });
    const items = (uv.items || []) as Array<{ value?: string; values?: string[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      if (value.length > CDF_FILTER_MAX_LEN) {
        keys = [];
      } else {
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(["metadata", filterKey], value),
        });
        keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : valueTooLongMetaPart(value);
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function runSequencesAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  const resourceType = "sequences";
  const path = projectPath(project, "sequences");
  const results: AnalysisRow[] = [];
  const filterPart = dataSetFilterAggregate(dataSetIds);
  try {
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: ["metadata", filterKey] }],
      ...filterPart,
    });
    const items = (uv.items || []) as Array<{ value?: string; values?: string[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      if (value.length > CDF_FILTER_MAX_LEN) {
        keys = [];
      } else {
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          path: ["metadata"],
          ...advancedFilterEquals(["metadata", filterKey], value),
          ...filterPart,
        });
        keys = uniquePropertiesKeys((up.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }> }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : valueTooLongMetaPart(value);
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

/** Files use the documents aggregate API (type, labels, author, source, sourceFile.metadata). */
export async function runDocumentsAnalysis(
  client: CogniteClientLike,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  const resourceType = "files";
  const path = projectPath(project, "documents");
  const results: AnalysisRow[] = [];
  const propPath = getDocumentsPropertyPath(filterKey);
  const isLabels = propPath.length === 1 && propPath[0] === "labels";
  const maxLen = DOCUMENTS_FILTER_MAX_LEN;
  const filterPart = documentsDataSetFilter(dataSetIds);
  try {
    const uv = await client.post(path, {
      aggregate: "uniqueValues",
      properties: [{ property: propPath }],
      limit: 1000,
      ...filterPart,
    });
    const items = (uv.items || []) as Array<{ value?: unknown; values?: unknown[]; count?: number }>;
    for (const item of items) {
      const parsed = itemValue(item);
      if (!parsed) continue;
      const { value, count } = parsed;
      let keys: string[];
      let skippedLong = false;
      if (isLabels) {
        keys = [];
      } else if (value.length > maxLen) {
        keys = [];
        skippedLong = true;
      } else {
        const eqFilter = filterEquals(propPath, value);
        const datasetFilter = (filterPart.filter as object) ?? null;
        const mergedFilter =
          datasetFilter != null
            ? { and: [datasetFilter, (eqFilter.filter as object) ?? {}] }
            : (eqFilter.filter as object) ?? {};
        const up = await client.post(path, {
          aggregate: "uniqueProperties",
          properties: [{ property: ["sourceFile", "metadata"] }],
          limit: 1000,
          filter: mergedFilter,
        });
        keys = uniquePropertiesKeysDocuments((up.items || []) as Array<{ values?: string[] | Array<{ property?: string[] }>; property?: string[] }>);
      }
      const countStr = count ? `Count: ${count}\n` : "";
      const filterKeyLabel = filterKey.replace(/\b\w/g, (c) => c.toUpperCase());
      const metaPart = keys.length
        ? `Metadata keys: [${keys.join(", ")}]\n\n`
        : isLabels
          ? "Metadata keys: (not available for labels)\n\n"
          : skippedLong
            ? valueTooLongMetaPart(value)
            : "Metadata keys: (none returned)\n\n";
      results.push({
        count,
        text: `${filterKeyLabel}: ${value}\n${countStr}${metaPart}`,
        filterKeyPart: `${filterKeyLabel}: ${value}\n`,
        countPart: countStr,
        metadataKeysPart: metaPart,
      });
    }
    results.sort((a, b) => b.count - a.count);
    return { resourceType, filterKey, rows: results };
  } catch (e) {
    return { resourceType, filterKey, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function runAnalysis(
  client: CogniteClientLike,
  resourceType: ResourceType,
  filterKey: string,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<AnalysisResult> {
  switch (resourceType) {
    case "assets":
      return runAssetAnalysis(client, filterKey, project, dataSetIds);
    case "timeseries":
      return runTimeseriesAnalysis(client, filterKey, project, dataSetIds);
    case "events":
      return runEventsAnalysis(client, filterKey, project, dataSetIds);
    case "sequences":
      return runSequencesAnalysis(client, filterKey, project, dataSetIds);
    case "files":
      return runDocumentsAnalysis(client, filterKey, project, dataSetIds);
    default:
      return { resourceType, filterKey, rows: [], error: `Unknown resource type: ${resourceType}` };
  }
}

/** Minimal SDK type for listing files (list returns CursorAndAsyncIterator with autoPagingToArray). */
interface FilesListSdk {
  files: {
    list: (scope?: { limit?: number; filter?: unknown }) => {
      autoPagingToArray: (opts: { limit: number }) => Promise<Array<{ metadata?: Record<string, string> }>>;
    };
  };
}

/** Fetch metadata keys for Files by listing files and collecting keys from file.metadata (like the notebook). */
export async function getMetadataKeysListForFiles(
  sdk: FilesListSdk,
  limit = 10000
): Promise<{ key: string; count: number }[]> {
  const cap = Math.min(Math.max(1, limit), 100000);
  const listResult = sdk.files.list({ limit: cap });
  const files = await listResult.autoPagingToArray({ limit: cap });
  const keyCounts = new Map<string, number>();
  for (const file of files) {
    const meta = file.metadata ?? {};
    for (const key of Object.keys(meta)) {
      keyCounts.set(key, (keyCounts.get(key) ?? 0) + 1);
    }
  }
  return Array.from(keyCounts.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

/** Fetch all metadata keys with counts for a resource type (for user to choose filter key from). */
export async function getMetadataKeysList(
  client: CogniteClientLike,
  resourceType: ResourceType,
  project: string,
  dataSetIds?: DataSetIdEither[]
): Promise<{ key: string; count: number }[]> {
  const filterPart = resourceType === "files" ? documentsDataSetFilter(dataSetIds) : dataSetFilterAggregate(dataSetIds);
  if (resourceType === "files") {
    const path = projectPath(project, "documents");
    const res = await client.post(path, {
      aggregate: "uniqueProperties",
      properties: [{ property: ["sourceFile", "metadata"] }],
      limit: 1000,
      ...filterPart,
    });
    const items = (res.items || []) as Array<{ property?: string[]; values?: string[] | Array<{ property?: string[] }>; count?: number }>;
    const fromAgg = items
      .map((item) => {
        const first = item.values?.[0];
        const key =
          typeof first === "string"
            ? first
            : Array.isArray(item.property) && item.property.length
              ? item.property.slice(-1)[0]!
              : (first as { property?: string[] } | undefined)?.property?.length
                ? (first as { property: string[] }).property.slice(-1)[0]!
                : "";
        return key ? { key, count: item.count ?? 0 } : null;
      })
      .filter((x): x is { key: string; count: number } => x != null)
      .sort((a, b) => b.count - a.count);
    const totalCount = await getTotalCount(client, project, "files", dataSetIds);
    return [
      { key: "type", count: totalCount },
      { key: "labels", count: totalCount },
      { key: "author", count: totalCount },
      { key: "source", count: totalCount },
      ...fromAgg,
    ];
  }
  const path = projectPath(project, resourceType);
  const res = await client.post(path, { aggregate: "uniqueProperties", path: ["metadata"], ...filterPart });
  const items = (res.items || []) as Array<{ property?: string[]; values?: Array<{ property?: string[] }>; count?: number }>;
  const list = items
    .map((item) => {
      const prop = item.property ?? item.values?.[0]?.property;
      const key = prop?.length ? prop.slice(-1)[0]! : "";
      return key ? { key, count: item.count ?? 0 } : null;
    })
    .filter((x): x is { key: string; count: number } => x != null)
    .sort((a, b) => b.count - a.count);
  if (resourceType === "timeseries") {
    const totalCount = await getTotalCount(client, project, "timeseries", dataSetIds);
    return [
      { key: "is step", count: totalCount },
      { key: "is string", count: totalCount },
      { key: "unit", count: totalCount },
      ...list,
    ];
  }
  return list;
}
