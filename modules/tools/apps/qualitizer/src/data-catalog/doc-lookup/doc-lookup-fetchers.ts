import type { CogniteClient } from "@cognite/sdk";
import { cachedViewsRetrieve } from "@/shared/dms-catalog-cache";
import { cachedInstancesByIds } from "@/shared/instances-cache";
import { toTimestampLoose, getUserTimeZone } from "@/shared/time-utils";
import { withTransientRetries } from "@/shared/transient-http-retry";
import type {
  DocLookupNodeResult,
  DocLookupNodeSummary,
  DocLookupProgress,
  DocLookupResult,
  DocLookupViewDefinitionData,
  DocLookupViewRef,
  DocLookupViewRetrieveChunk,
  DocLookupViewSearchResult,
  DocLookupViewVersionData,
  PropertyValueChange,
} from "./doc-lookup-types";

const LIST_PAGE_SIZE = 100;
const MAX_NODES = 200;
const RETRIEVE_SOURCES_CHUNK = 10;

function hasRecordShape(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseListNode(item: Record<string, unknown>): DocLookupNodeSummary | null {
  const space = item.space;
  const externalId = item.externalId;
  if (typeof space !== "string" || typeof externalId !== "string") return null;

  let typeSpace: string | null = null;
  let typeExternalId: string | null = null;
  const type = item.type;
  if (hasRecordShape(type)) {
    if (typeof type.space === "string") typeSpace = type.space;
    if (typeof type.externalId === "string") typeExternalId = type.externalId;
  }

  return {
    space,
    externalId,
    instanceType: "node",
    typeSpace,
    typeExternalId,
    createdTime: typeof item.createdTime === "number" ? item.createdTime : null,
    lastUpdatedTime: typeof item.lastUpdatedTime === "number" ? item.lastUpdatedTime : null,
    version: typeof item.version === "number" ? item.version : null,
  };
}

function getInvolvedViewsArray(inspectItem: Record<string, unknown>): unknown[] {
  const direct = inspectItem.involvedViews;
  if (Array.isArray(direct)) return direct;

  const inspectionResults = inspectItem.inspectionResults;
  if (hasRecordShape(inspectionResults)) {
    const nested = inspectionResults.involvedViews;
    if (Array.isArray(nested)) return nested;
  }

  return [];
}

function parseInvolvedViews(inspectItem: unknown): DocLookupViewRef[] {
  if (!hasRecordShape(inspectItem)) return [];

  const views = getInvolvedViewsArray(inspectItem)
    .map((view): DocLookupViewRef | null => {
      if (!hasRecordShape(view)) return null;
      const space = view.space;
      const externalId = view.externalId;
      const version = view.version;
      if (typeof space !== "string" || typeof externalId !== "string" || typeof version !== "string") {
        return null;
      }
      return { space, externalId, version };
    })
    .filter((view): view is DocLookupViewRef => view !== null);

  return views.sort((a, b) =>
    `${a.space}/${a.externalId}/${a.version}`.localeCompare(`${b.space}/${b.externalId}/${b.version}`)
  );
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Request failed.";
}

function viewKey(view: DocLookupViewRef): string {
  return `${view.space}:${view.externalId}:${view.version}`;
}

function definitionKey(viewSpace: string, viewExternalId: string): string {
  return `${viewSpace}\x1f${viewExternalId}`;
}

function sortKeysDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeysDeep);
  if (hasRecordShape(value)) {
    return Object.keys(value)
      .sort()
      .reduce<Record<string, unknown>>((acc, key) => {
        acc[key] = sortKeysDeep(value[key]);
        return acc;
      }, {});
  }
  return value;
}

export function stablePropertyFingerprint(properties: Record<string, unknown>): string {
  return JSON.stringify(sortKeysDeep(properties));
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return hasRecordShape(value) && !Array.isArray(value);
}

function leafFingerprint(value: unknown): string {
  return JSON.stringify(sortKeysDeep(value));
}

function collectLeafPaths(value: unknown, prefix = ""): string[] {
  if (isPlainObject(value)) {
    return Object.keys(value)
      .sort()
      .flatMap((key) => {
        const path = prefix.length > 0 ? `${prefix}.${key}` : key;
        return collectLeafPaths(value[key], path);
      });
  }
  return prefix.length > 0 ? [prefix] : [];
}

function readLeafValue(root: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = root;
  for (const part of parts) {
    if (!isPlainObject(current)) return undefined;
    current = current[part];
  }
  return current;
}

export function diffPropertyValues(
  baseline: Record<string, unknown>,
  other: Record<string, unknown>
): PropertyValueChange[] {
  const paths = new Set([...collectLeafPaths(baseline), ...collectLeafPaths(other)]);
  const changes: PropertyValueChange[] = [];

  for (const path of [...paths].sort()) {
    const baselineValue = readLeafValue(baseline, path);
    const otherValue = readLeafValue(other, path);
    const baselineMissing = baselineValue === undefined;
    const otherMissing = otherValue === undefined;

    if (baselineMissing && !otherMissing) {
      changes.push({ path, kind: "added", baseline: undefined, value: otherValue });
      continue;
    }
    if (!baselineMissing && otherMissing) {
      changes.push({ path, kind: "removed", baseline: baselineValue, value: undefined });
      continue;
    }
    if (!baselineMissing && !otherMissing && leafFingerprint(baselineValue) !== leafFingerprint(otherValue)) {
      changes.push({ path, kind: "changed", baseline: baselineValue, value: otherValue });
    }
  }

  return changes;
}

function countUniqueStoredData(versionDataList: DocLookupViewVersionData[]): number {
  const fingerprints = new Set(
    versionDataList
      .filter((entry) => entry.retrieveError === null)
      .map((entry) => stablePropertyFingerprint(entry.properties))
  );
  return fingerprints.size;
}

function compareViewVersionStrings(a: string, b: string): number {
  const aSemver = /^v(\d+)$/i.exec(a);
  const bSemver = /^v(\d+)$/i.exec(b);
  if (aSemver && bSemver) {
    return Number(bSemver[1]) - Number(aSemver[1]);
  }
  return b.localeCompare(a);
}

function compareViewVersionsByLastSavedNewestFirst(
  a: DocLookupViewVersionData,
  b: DocLookupViewVersionData
): number {
  const aTime = a.viewLastUpdatedTime;
  const bTime = b.viewLastUpdatedTime;
  if (aTime !== null && bTime !== null && aTime !== bTime) {
    return bTime - aTime;
  }
  if (aTime !== null && bTime === null) return -1;
  if (aTime === null && bTime !== null) return 1;
  return (
    compareViewVersionStrings(a.view.version, b.view.version) ||
    a.view.version.localeCompare(b.view.version)
  );
}

function compareViewVersionsByLastSavedOldestFirst(
  a: DocLookupViewVersionData,
  b: DocLookupViewVersionData
): number {
  return -compareViewVersionsByLastSavedNewestFirst(a, b);
}

function sortViewVersionsChronologically(
  versionDataList: DocLookupViewVersionData[]
): DocLookupViewVersionData[] {
  return [...versionDataList].sort(compareViewVersionsByLastSavedOldestFirst);
}

function sortViewVersionsNewestFirst(
  versionDataList: DocLookupViewVersionData[]
): DocLookupViewVersionData[] {
  return [...versionDataList].sort(compareViewVersionsByLastSavedNewestFirst);
}

function enrichVersionDataWithDiffs(
  versionDataList: DocLookupViewVersionData[]
): DocLookupViewVersionData[] {
  const chronological = sortViewVersionsChronologically(versionDataList);
  const enrichedByVersion = new Map<string, DocLookupViewVersionData>();

  for (let index = 0; index < chronological.length; index += 1) {
    const entry = chronological[index];
    const previous = index > 0 ? chronological[index - 1] : undefined;

    if (entry.retrieveError !== null) {
      enrichedByVersion.set(entry.view.version, {
        ...entry,
        previousVersion: previous?.view.version ?? null,
        dataMatchesPrevious: false,
        changesFromPrevious: [],
      });
      continue;
    }

    if (previous === undefined || previous.retrieveError !== null) {
      enrichedByVersion.set(entry.view.version, {
        ...entry,
        previousVersion: null,
        dataMatchesPrevious: true,
        changesFromPrevious: [],
      });
      continue;
    }

    const dataMatchesPrevious =
      stablePropertyFingerprint(entry.properties) === stablePropertyFingerprint(previous.properties);

    enrichedByVersion.set(entry.view.version, {
      ...entry,
      previousVersion: previous.view.version,
      dataMatchesPrevious,
      changesFromPrevious: dataMatchesPrevious
        ? []
        : diffPropertyValues(previous.properties, entry.properties),
    });
  }

  return versionDataList.map(
    (entry) => enrichedByVersion.get(entry.view.version) ?? entry
  );
}

function extractPropertiesForView(
  item: Record<string, unknown>,
  view: DocLookupViewRef
): { properties: Record<string, unknown>; propertyKey: string | null } {
  if (!hasRecordShape(item.properties)) {
    return { properties: {}, propertyKey: null };
  }

  const spaceProperties = item.properties[view.space];
  if (!hasRecordShape(spaceProperties)) {
    return { properties: {}, propertyKey: null };
  }

  const exactKey = `${view.externalId}/${view.version}`;
  const exact = spaceProperties[exactKey];
  if (hasRecordShape(exact)) {
    return { properties: exact, propertyKey: exactKey };
  }

  for (const [key, value] of Object.entries(spaceProperties)) {
    if (key === exactKey || key.endsWith(`/${view.version}`)) {
      if (hasRecordShape(value)) {
        return { properties: value, propertyKey: key };
      }
    }
  }

  return { properties: {}, propertyKey: null };
}

type ViewVersionMetadata = {
  version: string;
  lastUpdatedTime: number | null;
  createdTime: number | null;
};

async function retrieveViewVersionMetadata(
  sdk: CogniteClient,
  versions: DocLookupViewRef[]
): Promise<{
  latestVersion: string;
  byVersion: Map<string, ViewVersionMetadata>;
}> {
  if (versions.length === 0) {
    return { latestVersion: "", byVersion: new Map() };
  }

  const fallbackLatest = [...versions].sort((a, b) =>
    compareViewVersionStrings(a.version, b.version)
  )[0].version;

  try {
    const response = (await cachedViewsRetrieve(
      sdk,
      versions.map((view) => ({
        space: view.space,
        externalId: view.externalId,
        version: view.version,
      }))
    )) as { items?: unknown[] };

    const metadata = (response.items ?? [])
      .map((item) => {
        if (!hasRecordShape(item)) return null;
        const version = item.version;
        if (typeof version !== "string") return null;
        const lastUpdatedTime = typeof item.lastUpdatedTime === "number" ? item.lastUpdatedTime : null;
        const createdTime = typeof item.createdTime === "number" ? item.createdTime : null;
        return { version, lastUpdatedTime, createdTime };
      })
      .filter((entry): entry is ViewVersionMetadata => entry !== null);

    const byVersion = new Map(metadata.map((entry) => [entry.version, entry]));
    const ranked = [...metadata].sort(
      (a, b) =>
        (b.lastUpdatedTime ?? 0) - (a.lastUpdatedTime ?? 0) ||
        (b.createdTime ?? 0) - (a.createdTime ?? 0)
    );

    return {
      latestVersion: ranked[0]?.version ?? fallbackLatest,
      byVersion,
    };
  } catch {
    return {
      latestVersion: fallbackLatest,
      byVersion: new Map(
        versions.map((view) => [
          view.version,
          { version: view.version, lastUpdatedTime: null, createdTime: null },
        ])
      ),
    };
  }
}

type ViewRetrieveResult = {
  properties: Record<string, unknown>;
  error: string | null;
  propertyKey: string | null;
};

type RetrieveBatchResult = {
  byView: Map<string, ViewRetrieveResult>;
  chunks: DocLookupViewRetrieveChunk[];
};

async function retrieveNodeViewPropertiesBatch(
  sdk: CogniteClient,
  node: DocLookupNodeSummary,
  views: DocLookupViewRef[]
): Promise<RetrieveBatchResult> {
  const byView = new Map<string, ViewRetrieveResult>();
  const chunks: DocLookupViewRetrieveChunk[] = [];

  for (let index = 0; index < views.length; index += RETRIEVE_SOURCES_CHUNK) {
    const chunk = views.slice(index, index + RETRIEVE_SOURCES_CHUNK);
    const request = {
      items: [
        {
          space: node.space,
          externalId: node.externalId,
          instanceType: node.instanceType,
        },
      ],
      sources: chunk.map((view) => ({
        source: {
          type: "view",
          space: view.space,
          externalId: view.externalId,
          version: view.version,
        },
      })),
    };

    try {
      const response = await withTransientRetries(() => cachedInstancesByIds(sdk, request));
      chunks.push({ sources: chunk, request, response });

      const item = response.items?.[0];
      if (!hasRecordShape(item)) {
        for (const view of chunk) {
          byView.set(viewKey(view), { properties: {}, error: "Instance not returned.", propertyKey: null });
        }
        continue;
      }

      for (const view of chunk) {
        const extracted = extractPropertiesForView(item, view);
        byView.set(viewKey(view), {
          properties: extracted.properties,
          error: null,
          propertyKey: extracted.propertyKey,
        });
      }
    } catch (error) {
      const message = toErrorMessage(error);
      chunks.push({ sources: chunk, request, response: { error: message } });
      for (const view of chunk) {
        byView.set(viewKey(view), { properties: {}, error: message, propertyKey: null });
      }
    }
  }

  return { byView, chunks };
}

function diagnosticsForDefinition(
  definitionViews: DocLookupViewRef[],
  chunks: DocLookupViewRetrieveChunk[]
): DocLookupViewRetrieveChunk[] {
  const definitionKeys = new Set(definitionViews.map((view) => viewKey(view)));
  return chunks.filter((chunk) => chunk.sources.some((view) => definitionKeys.has(viewKey(view))));
}

function buildViewDefinitionData(
  viewSpace: string,
  viewExternalId: string,
  versionDataList: DocLookupViewVersionData[],
  latestVersion: string
): DocLookupViewDefinitionData {
  const enrichedVersions = enrichVersionDataWithDiffs(versionDataList);
  const sortedVersions = sortViewVersionsNewestFirst(enrichedVersions);
  const successfulVersions = sortedVersions.filter((entry) => entry.retrieveError === null);
  const fingerprints = successfulVersions.map((entry) => stablePropertyFingerprint(entry.properties));
  const allVersionsIdentical =
    fingerprints.length <= 1 || fingerprints.every((fingerprint) => fingerprint === fingerprints[0]);
  const uniqueStoredDataCount = countUniqueStoredData(sortedVersions);

  const latestEntry =
    sortedVersions.find((entry) => entry.view.version === latestVersion) ?? sortedVersions[0];

  const displayVersions = allVersionsIdentical
    ? latestEntry !== undefined
      ? [latestEntry]
      : []
    : sortedVersions;

  return {
    viewSpace,
    viewExternalId,
    versions: sortedVersions,
    allVersionsIdentical,
    uniqueStoredDataCount,
    latestVersion,
    displayVersions,
    retrieveDiagnostics: [],
  };
}

async function buildViewDefinitionsForNode(
  sdk: CogniteClient,
  node: DocLookupNodeSummary,
  views: DocLookupViewRef[],
  onProgress?: (progress: DocLookupProgress) => void
): Promise<DocLookupViewDefinitionData[]> {
  if (views.length === 0) return [];

  const { byView: propertyByView, chunks: retrieveChunks } = await retrieveNodeViewPropertiesBatch(
    sdk,
    node,
    views
  );

  const byDefinition = new Map<string, DocLookupViewRef[]>();
  for (const view of views) {
    const key = definitionKey(view.space, view.externalId);
    const existing = byDefinition.get(key) ?? [];
    existing.push(view);
    byDefinition.set(key, existing);
  }

  const definitions: DocLookupViewDefinitionData[] = [];
  const entries = [...byDefinition.entries()].sort(([a], [b]) => a.localeCompare(b));

  for (let index = 0; index < entries.length; index += 1) {
    const [key, definitionViews] = entries[index];
    const [viewSpace, viewExternalId] = key.split("\x1f");
    onProgress?.({
      phase: "retrieve",
      current: index,
      total: entries.length,
      detail: `${viewSpace} / ${viewExternalId}`,
    });

    const { latestVersion, byVersion: viewMetadataByVersion } = await retrieveViewVersionMetadata(
      sdk,
      definitionViews
    );
    const versionDataList: DocLookupViewVersionData[] = definitionViews.map((view) => {
      const retrieved = propertyByView.get(viewKey(view));
      const metadata = viewMetadataByVersion.get(view.version);
      return {
        view,
        properties: retrieved?.properties ?? {},
        retrieveError: retrieved === undefined ? "Properties not retrieved." : retrieved.error,
        propertyKey: retrieved?.propertyKey ?? null,
        viewLastUpdatedTime: metadata?.lastUpdatedTime ?? null,
        previousVersion: null,
        dataMatchesPrevious: true,
        changesFromPrevious: [],
      };
    });

    const definition = buildViewDefinitionData(viewSpace, viewExternalId, versionDataList, latestVersion);
    definition.retrieveDiagnostics = diagnosticsForDefinition(definitionViews, retrieveChunks);
    definitions.push(definition);
  }

  onProgress?.({
    phase: "retrieve",
    current: entries.length,
    total: entries.length,
    detail: "Done",
  });

  return definitions;
}

async function listNodesByExternalId(
  sdk: CogniteClient,
  externalId: string,
  onProgress?: (progress: DocLookupProgress) => void
): Promise<{ nodes: Array<{ summary: DocLookupNodeSummary; rawListItem: unknown }>; truncated: boolean }> {
  const nodes: Array<{ summary: DocLookupNodeSummary; rawListItem: unknown }> = [];
  let cursor: string | undefined;
  let truncated = false;

  do {
    const response = await withTransientRetries(() =>
      sdk.instances.list({
        instanceType: "node",
        filter: {
          in: {
            property: ["node", "externalId"],
            values: [externalId],
          },
        },
        limit: LIST_PAGE_SIZE,
        cursor,
      })
    );

    for (const item of response.items) {
      if (!hasRecordShape(item)) continue;
      const parsed = parseListNode(item);
      if (parsed === null) continue;
      nodes.push({ summary: parsed, rawListItem: item });
      if (nodes.length >= MAX_NODES) {
        truncated = true;
        break;
      }
    }

    onProgress?.({
      phase: "list",
      current: nodes.length,
      total: truncated ? MAX_NODES : nodes.length || LIST_PAGE_SIZE,
      detail: `Found ${nodes.length} node${nodes.length === 1 ? "" : "s"}`,
    });

    if (truncated) break;
    cursor = response.nextCursor;
  } while (cursor !== undefined);

  nodes.sort((a, b) => a.summary.space.localeCompare(b.summary.space));
  return { nodes, truncated };
}

async function inspectNodeViews(
  sdk: CogniteClient,
  node: DocLookupNodeSummary
): Promise<{ views: DocLookupViewRef[]; error: string | null; rawInspectResponse: unknown | null }> {
  try {
    const response = await withTransientRetries(() =>
      (sdk.instances.inspect as (params: unknown) => Promise<unknown>)({
        inspectionOperations: {
          involvedViews: {
            allVersions: true,
          },
        },
        items: [
          {
            instanceType: "node",
            externalId: node.externalId,
            space: node.space,
          },
        ],
      })
    );

    const inspectItem = hasRecordShape(response) && Array.isArray(response.items) ? response.items[0] : null;
    return { views: parseInvolvedViews(inspectItem), error: null, rawInspectResponse: response };
  } catch (error) {
    return { views: [], error: toErrorMessage(error), rawInspectResponse: null };
  }
}

export async function searchNodesByExternalId(
  sdk: CogniteClient,
  externalId: string,
  options?: {
    onProgress?: (progress: DocLookupProgress) => void;
  }
): Promise<{ nodes: DocLookupNodeSummary[]; truncated: boolean }> {
  const { nodes, truncated } = await listNodesByExternalId(sdk, externalId, options?.onProgress);
  return { nodes: nodes.map((entry) => entry.summary), truncated };
}

export async function inspectInvolvedViewsForNode(
  sdk: CogniteClient,
  node: DocLookupNodeSummary
): Promise<{ views: DocLookupViewRef[]; error: string | null; rawInspectResponse: unknown | null }> {
  return inspectNodeViews(sdk, node);
}

export async function inspectInvolvedViewsForNodes(
  sdk: CogniteClient,
  nodes: Array<Pick<DocLookupNodeSummary, "space" | "externalId">>
): Promise<Map<string, DocLookupViewRef[]>> {
  const result = await inspectInvolvedViewsWithEvidence(sdk, nodes);
  return result.viewsByRef;
}

export type InvolvedViewsInspectionEvidence = {
  viewsByRef: Map<string, DocLookupViewRef[]>;
  inspectItemByRef: Map<string, unknown>;
  request: unknown;
  response: unknown;
};

export async function inspectInvolvedViewsWithEvidence(
  sdk: CogniteClient,
  nodes: Array<Pick<DocLookupNodeSummary, "space" | "externalId">>
): Promise<InvolvedViewsInspectionEvidence> {
  const uniqueRefs = new Map<string, Pick<DocLookupNodeSummary, "space" | "externalId">>();
  for (const node of nodes) {
    uniqueRefs.set(`${node.space}:${node.externalId}`, node);
  }

  const viewsByRef = new Map<string, DocLookupViewRef[]>();
  const inspectItemByRef = new Map<string, unknown>();
  const nodeList = Array.from(uniqueRefs.values());
  const batchRequests: unknown[] = [];
  const batchResponses: unknown[] = [];

  for (let index = 0; index < nodeList.length; index += 100) {
    const batch = nodeList.slice(index, index + 100);
    const request = {
      inspectionOperations: {
        involvedViews: {
          allVersions: true,
        },
      },
      items: batch.map((node) => ({
        instanceType: "node",
        externalId: node.externalId,
        space: node.space,
      })),
    };
    const response = await withTransientRetries(() =>
      (sdk.instances.inspect as (params: unknown) => Promise<unknown>)(request)
    );
    batchRequests.push(request);
    batchResponses.push(response);

    const items =
      hasRecordShape(response) && Array.isArray(response.items) ? response.items : [];

    for (let itemIndex = 0; itemIndex < batch.length; itemIndex += 1) {
      const node = batch[itemIndex];
      const key = `${node.space}:${node.externalId}`;
      const inspectItem = items[itemIndex] ?? null;
      inspectItemByRef.set(key, inspectItem);
      viewsByRef.set(key, parseInvolvedViews(inspectItem));
    }
  }

  return {
    viewsByRef,
    inspectItemByRef,
    request: batchRequests.length === 1 ? batchRequests[0] : batchRequests,
    response: batchResponses.length === 1 ? batchResponses[0] : batchResponses,
  };
}

export type NodeViewProperties = {
  view: DocLookupViewRef;
  properties: Record<string, unknown>;
  propertyKey: string | null;
  error: string | null;
};

export async function retrieveInvolvedViewPropertiesForNode(
  sdk: CogniteClient,
  node: DocLookupNodeSummary,
  views: DocLookupViewRef[]
): Promise<NodeViewProperties[]> {
  if (views.length === 0) return [];

  const { byView } = await retrieveNodeViewPropertiesBatch(sdk, node, views);
  return views.map((view) => {
    const retrieved = byView.get(viewKey(view));
    return {
      view,
      properties: retrieved?.properties ?? {},
      propertyKey: retrieved?.propertyKey ?? null,
      error: retrieved?.error ?? null,
    };
  });
}

export function mergeViewRefs(
  primary: DocLookupViewRef[],
  secondary: DocLookupViewRef[]
): DocLookupViewRef[] {
  const byKey = new Map<string, DocLookupViewRef>();
  for (const view of [...primary, ...secondary]) {
    byKey.set(viewKey(view), view);
  }
  return [...byKey.values()].sort((a, b) =>
    `${a.space}/${a.externalId}/${a.version}`.localeCompare(`${b.space}/${b.externalId}/${b.version}`)
  );
}

function pickLatestViewVersionEntry(entries: NodeViewProperties[]): NodeViewProperties {
  return [...entries].sort(
    (a, b) => compareViewVersionStrings(b.view.version, a.view.version)
  )[0];
}

export function collapseIdenticalViewVersionEntries(
  viewData: NodeViewProperties[]
): NodeViewProperties[] {
  const byDefinition = new Map<string, NodeViewProperties[]>();

  for (const entry of viewData) {
    const key = definitionKey(entry.view.space, entry.view.externalId);
    const group = byDefinition.get(key) ?? [];
    group.push(entry);
    byDefinition.set(key, group);
  }

  const collapsed: NodeViewProperties[] = [];

  for (const group of byDefinition.values()) {
    if (group.length <= 1) {
      collapsed.push(...group);
      continue;
    }

    const fingerprints = group.map((entry) => stablePropertyFingerprint(entry.properties));
    const allVersionsIdentical =
      fingerprints.length <= 1 ||
      fingerprints.every((fingerprint) => fingerprint === fingerprints[0]);

    if (allVersionsIdentical) {
      collapsed.push(pickLatestViewVersionEntry(group));
    } else {
      collapsed.push(...group);
    }
  }

  return collapsed.sort((a, b) =>
    `${a.view.space}/${a.view.externalId}/${a.view.version}`.localeCompare(
      `${b.view.space}/${b.view.externalId}/${b.view.version}`
    )
  );
}

export async function lookupExternalIdInDms(
  sdk: CogniteClient,
  externalId: string,
  options?: {
    onProgress?: (progress: DocLookupProgress) => void;
  }
): Promise<DocLookupResult> {
  const trimmed = externalId.trim();
  if (trimmed.length === 0) {
    return { queriedExternalId: "", nodes: [], listTruncated: false };
  }

  options?.onProgress?.({ phase: "list", current: 0, total: 0, detail: "Searching nodes by external ID" });

  const { nodes, truncated } = await listNodesByExternalId(sdk, trimmed, options?.onProgress);

  const results: DocLookupNodeResult[] = [];

  for (let index = 0; index < nodes.length; index += 1) {
    const { summary: node, rawListItem } = nodes[index];
    options?.onProgress?.({
      phase: "inspect",
      current: index,
      total: nodes.length,
      detail: `${node.space} / ${node.externalId}`,
    });

    const { views, error, rawInspectResponse } = await inspectNodeViews(sdk, node);
    const viewDefinitions = await buildViewDefinitionsForNode(sdk, node, views, options?.onProgress);

    results.push({
      node,
      views,
      viewDefinitions,
      inspectError: error,
      rawListItem,
      rawInspectResponse,
    });
  }

  options?.onProgress?.({
    phase: "inspect",
    current: nodes.length,
    total: nodes.length,
    detail: "Done",
  });

  return {
    queriedExternalId: trimmed,
    nodes: results,
    listTruncated: truncated,
  };
}

export function formatViewRef(view: DocLookupViewRef): string {
  return `${view.space} / ${view.externalId} : ${view.version}`;
}

function resolveDisplayTimeZone(): string | undefined {
  const timeZone = getUserTimeZone();
  if (timeZone === "local" || timeZone.length === 0) {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;
  }
  try {
    Intl.DateTimeFormat(undefined, { timeZone });
    return timeZone;
  } catch {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;
  }
}

function prettyTimestampOptions(timeZone: string | undefined): Intl.DateTimeFormatOptions {
  return {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
    ...(timeZone !== undefined ? { timeZone } : {}),
  };
}

const ISO_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/;

function looksLikeEpochMs(value: number): boolean {
  return value >= 1_000_000_000_000 && value <= 9_999_999_999_999;
}

function timestampPropertySegment(propertyKey: string | undefined): string | null {
  if (propertyKey === undefined || propertyKey.length === 0) return null;
  const segment = propertyKey.includes(".") ? propertyKey.split(".").pop() : propertyKey;
  if (segment === undefined || segment.length === 0) return null;
  return segment;
}

export function isTimestampPropertyKey(propertyKey: string | undefined): boolean {
  const segment = timestampPropertySegment(propertyKey);
  if (segment === null) return false;
  return (
    segment.endsWith("Time") ||
    segment.endsWith("Timestamp") ||
    segment.endsWith("Date") ||
    segment === "lastCalled"
  );
}

export function formatTimestamp(epochMs: number | null | undefined): string {
  if (epochMs === null || epochMs === undefined) return "—";
  const date = new Date(epochMs);
  if (Number.isNaN(date.getTime())) return "—";
  const timeZone = resolveDisplayTimeZone();
  const formatted = date.toLocaleString(undefined, prettyTimestampOptions(timeZone));
  return timeZone !== undefined ? `${formatted} (${timeZone})` : formatted;
}

export function tryFormatPropertyTimestamp(
  value: unknown,
  propertyKey?: string
): string | null {
  if (value === null || value === undefined) return null;

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return null;
    if (!isTimestampPropertyKey(propertyKey) && !ISO_TIMESTAMP_PATTERN.test(trimmed)) {
      return null;
    }
    const parsed = toTimestampLoose(trimmed);
    if (parsed === undefined) return null;
    return formatTimestamp(parsed);
  }

  if (typeof value === "number") {
    if (!isTimestampPropertyKey(propertyKey) && !looksLikeEpochMs(value)) return null;
    return formatTimestamp(value);
  }

  return null;
}

export function prettifyJsonPropertyLine(line: string): string {
  const match = /^(\s*)"([^"]+)":\s*("(?:[^"\\]|\\.)*")(,?)$/.exec(line);
  if (match === null) return line;

  const [, indent, key, quotedValue, trailingComma] = match;
  const rawValue = quotedValue.slice(1, -1).replace(/\\"/g, '"');
  const formatted = tryFormatPropertyTimestamp(rawValue, key);
  if (formatted === null) return line;

  return `${indent}"${key}": "${formatted}"${trailingComma}`;
}

export function buildDocLookupDiagnosticJson(result: DocLookupResult): string {
  return JSON.stringify(result, null, 2);
}

export function versionDataDiffersFromPrevious(entry: DocLookupViewVersionData): boolean {
  return (
    entry.retrieveError === null &&
    entry.previousVersion !== null &&
    !entry.dataMatchesPrevious
  );
}

export function viewVersionHasNoProperties(entry: DocLookupViewVersionData): boolean {
  return entry.retrieveError === null && Object.keys(entry.properties).length === 0;
}

export function isViewDefinitionPropertyEmpty(definition: DocLookupViewDefinitionData): boolean {
  return (
    definition.displayVersions.length > 0 &&
    definition.displayVersions.every(viewVersionHasNoProperties)
  );
}

export function formatPropertyValue(value: unknown, propertyKey?: string): string {
  if (value === undefined) return "—";
  const formattedTimestamp = tryFormatPropertyTimestamp(value, propertyKey);
  if (formattedTimestamp !== null) return formattedTimestamp;
  const assetPath = extractAssetPath(value);
  if (assetPath !== null) return formatAssetPathBreadcrumb(assetPath);
  const nodeRef = extractNodeRef(value);
  if (nodeRef !== null) return `${nodeRef.space} / ${nodeRef.externalId}`;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

export type NodeInstanceRef = {
  space: string;
  externalId: string;
};

export function extractNodeRef(value: unknown): NodeInstanceRef | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  if (typeof obj.space === "string" && typeof obj.externalId === "string") {
    return { space: obj.space, externalId: obj.externalId };
  }
  return null;
}

export function nodeRefsMatch(a: NodeInstanceRef, b: NodeInstanceRef): boolean {
  return a.space === b.space && a.externalId === b.externalId;
}

function extractNodeRefPath(path: unknown): NodeInstanceRef[] | null {
  if (!Array.isArray(path) || path.length === 0) return null;
  const refs: NodeInstanceRef[] = [];
  for (const item of path) {
    const ref = extractNodeRef(item);
    if (ref === null) return null;
    refs.push(ref);
  }
  return refs;
}

export function extractAssetPath(value: unknown): NodeInstanceRef[] | null {
  const directPath = extractNodeRefPath(value);
  if (directPath !== null) return directPath;
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  return extractNodeRefPath(obj.path ?? obj.Path);
}

export type AssetHierarchyRefs = {
  parent: NodeInstanceRef;
  root: NodeInstanceRef | null;
  path: NodeInstanceRef[];
  distanceToRoot: number | null;
};

export function extractAssetHierarchy(value: unknown): AssetHierarchyRefs | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  const parent = extractNodeRef(obj.parent);
  if (parent === null) return null;

  const path = extractNodeRefPath(obj.path ?? obj.Path) ?? [];
  const root = extractNodeRef(obj.root) ?? path[0] ?? null;

  let distanceToRoot: number | null = null;
  if (path.length > 0) {
    const parentIndex = path.findIndex(
      (ref) => ref.space === parent.space && ref.externalId === parent.externalId
    );
    distanceToRoot = parentIndex >= 0 ? parentIndex : null;
  }

  return { parent, root, path, distanceToRoot };
}

export type AssetHierarchyWarning =
  | { kind: "path_missing" }
  | { kind: "parent_missing" }
  | { kind: "root_missing" }
  | { kind: "parent_not_in_path"; parent: NodeInstanceRef }
  | { kind: "root_mismatch"; root: NodeInstanceRef; pathRoot: NodeInstanceRef };

export type CogniteAssetHierarchyAnalysis = {
  path: NodeInstanceRef[];
  root: NodeInstanceRef | null;
  parent: NodeInstanceRef | null;
  warnings: AssetHierarchyWarning[];
};

const COGNITE_ASSET_HIERARCHY_PROPERTY_KEYS = new Set(["path", "Path", "root", "parent"]);

export function isCogniteAssetHierarchyPropertyKey(key: string): boolean {
  return COGNITE_ASSET_HIERARCHY_PROPERTY_KEYS.has(key);
}

export function analyzeCogniteAssetHierarchy(
  properties: Record<string, unknown>
): CogniteAssetHierarchyAnalysis | null {
  const path = extractAssetPath(properties) ?? [];
  const parent = extractNodeRef(properties.parent);
  const root = extractNodeRef(properties.root);

  if (path.length === 0 && parent === null && root === null) return null;

  const warnings: AssetHierarchyWarning[] = [];

  if (path.length === 0) {
    warnings.push({ kind: "path_missing" });
  }
  if (path.length > 1 && parent === null) {
    warnings.push({ kind: "parent_missing" });
  }
  if (path.length > 0 && root === null) {
    warnings.push({ kind: "root_missing" });
  }
  if (parent !== null && path.length > 0) {
    const parentInPath = path.some((ref) => nodeRefsMatch(ref, parent));
    if (!parentInPath) {
      warnings.push({ kind: "parent_not_in_path", parent });
    }
  }
  if (root !== null && path.length > 0) {
    const pathRoot = path[0];
    if (pathRoot !== undefined && !nodeRefsMatch(pathRoot, root)) {
      warnings.push({ kind: "root_mismatch", root, pathRoot });
    }
  }

  return {
    path,
    root: root ?? path[0] ?? null,
    parent,
    warnings,
  };
}

export function formatAssetPathBreadcrumb(path: NodeInstanceRef[]): string {
  return path.map((ref) => ref.externalId).join(" → ");
}

export function hasAssetPathData(properties: Record<string, unknown>): boolean {
  if (extractAssetPath(properties) !== null) return true;
  for (const value of Object.values(properties)) {
    if (extractAssetPath(value) !== null) return true;
  }
  return false;
}

export function isCogniteAssetView(viewExternalId: string): boolean {
  return viewExternalId === "CogniteAsset";
}

export type CogniteDescribableData = {
  name: string | null;
  description: string | null;
  tags: string[];
  aliases: string[];
};

const COGNITE_DESCRIBABLE_PROPERTY_KEYS = new Set(["name", "description", "tags", "aliases"]);

function readStringProperty(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function readStringListProperty(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

export function isCogniteDescribablePropertyKey(key: string): boolean {
  return COGNITE_DESCRIBABLE_PROPERTY_KEYS.has(key.toLowerCase());
}

export function extractCogniteDescribable(
  properties: Record<string, unknown>
): CogniteDescribableData | null {
  const name = readStringProperty(properties.name ?? properties.Name);
  const description = readStringProperty(properties.description ?? properties.Description);
  const tags = readStringListProperty(properties.tags ?? properties.Tags);
  const aliases = readStringListProperty(properties.aliases ?? properties.Aliases);

  if (name === null && description === null && tags.length === 0 && aliases.length === 0) {
    return null;
  }

  return { name, description, tags, aliases };
}

export function hasCogniteDescribableData(properties: Record<string, unknown>): boolean {
  return extractCogniteDescribable(properties) !== null;
}

const SEARCH_REQUEST_KEY_ORDER = [
  "instanceType",
  "view",
  "query",
  "filter",
  "limit",
  "properties",
  "operator",
] as const;

const SEARCH_RESPONSE_KEY_ORDER = ["items", "nextCursor"] as const;

const SEARCH_VIEW_KEY_ORDER = ["space", "version", "externalId", "type"] as const;

const SEARCH_FILTER_CLAUSE_KEY_ORDER = [
  "property",
  "values",
  "value",
  "gt",
  "gte",
  "lt",
  "lte",
] as const;

const SEARCH_FILTER_GROUP_KEY_ORDER = ["and", "or", "not"] as const;

const SEARCH_FILTER_OP_KEY_ORDER = [
  "in",
  "equals",
  "prefix",
  "range",
  "exists",
  "containsAny",
  "containsAll",
  ...SEARCH_FILTER_GROUP_KEY_ORDER,
] as const;

function sortKeysWithOrder(
  value: Record<string, unknown>,
  keyOrder: readonly string[]
): Record<string, unknown> {
  const knownKeys: string[] = [];
  const unknownKeys: string[] = [];

  for (const key of Object.keys(value)) {
    if (keyOrder.includes(key)) {
      knownKeys.push(key);
    } else {
      unknownKeys.push(key);
    }
  }

  knownKeys.sort((a, b) => keyOrder.indexOf(a) - keyOrder.indexOf(b));
  unknownKeys.sort((a, b) => a.localeCompare(b));

  const keys = [...knownKeys, ...unknownKeys];

  return keys.reduce<Record<string, unknown>>((acc, key) => {
    acc[key] = sortSearchJsonKeys(value[key], key);
    return acc;
  }, {});
}

function sortObjectKeysWithFilterOpsFirst(value: Record<string, unknown>): Record<string, unknown> {
  const keys = Object.keys(value);
  const filterOpKeys = keys
    .filter((key) => SEARCH_FILTER_OP_KEY_ORDER.includes(key))
    .sort((a, b) => SEARCH_FILTER_OP_KEY_ORDER.indexOf(a) - SEARCH_FILTER_OP_KEY_ORDER.indexOf(b));
  const otherKeys = keys
    .filter((key) => !SEARCH_FILTER_OP_KEY_ORDER.includes(key))
    .sort((a, b) => a.localeCompare(b));

  return [...filterOpKeys, ...otherKeys].reduce<Record<string, unknown>>((acc, key) => {
    acc[key] = sortSearchJsonKeys(value[key], key);
    return acc;
  }, {});
}

function sortSearchJsonKeys(value: unknown, parentKey?: string): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sortSearchJsonKeys(item));
  }
  if (!hasRecordShape(value)) return value;

  if (parentKey === "view") {
    return sortKeysWithOrder(value, SEARCH_VIEW_KEY_ORDER);
  }

  if (parentKey === "in" || parentKey === "equals" || parentKey === "prefix" || parentKey === "range") {
    return sortKeysWithOrder(value, SEARCH_FILTER_CLAUSE_KEY_ORDER);
  }

  const keys = Object.keys(value);
  if (keys.length === 1) {
    const op = keys[0];
    if (op !== undefined && SEARCH_FILTER_OP_KEY_ORDER.includes(op)) {
      return { [op]: sortSearchJsonKeys(value[op], op) };
    }
  }

  if (keys.some((key) => SEARCH_FILTER_OP_KEY_ORDER.includes(key))) {
    return sortObjectKeysWithFilterOpsFirst(value);
  }

  if (parentKey === undefined && ("view" in value || "query" in value || "instanceType" in value)) {
    return sortKeysWithOrder(value, SEARCH_REQUEST_KEY_ORDER);
  }

  if (parentKey === undefined && "items" in value) {
    return sortKeysWithOrder(value, SEARCH_RESPONSE_KEY_ORDER);
  }

  if (parentKey === "filter") {
    return sortKeysWithOrder(value, SEARCH_FILTER_GROUP_KEY_ORDER);
  }

  return sortKeysWithOrder(value, []);
}

export function formatSearchJson(value: unknown): string {
  return JSON.stringify(sortSearchJsonKeys(value), null, 2);
}

export function buildViewVersionSearchRequest(
  node: DocLookupNodeSummary,
  view: DocLookupViewRef,
  query: string
): Record<string, unknown> {
  return {
    instanceType: "node",
    view: {
      type: "view",
      space: view.space,
      externalId: view.externalId,
      version: view.version,
    },
    query: query.trim(),
    filter: {
      and: [
        { equals: { property: ["node", "space"], value: node.space } },
        { equals: { property: ["node", "externalId"], value: node.externalId } },
      ],
    },
    limit: 10,
  };
}

export async function executeInstancesSearch(
  sdk: CogniteClient,
  request: Record<string, unknown>
): Promise<DocLookupViewSearchResult> {
  try {
    const response = await withTransientRetries(() => sdk.instances.search(request as never));
    return { request, response, error: null };
  } catch (error) {
    return { request, response: null, error: toErrorMessage(error) };
  }
}

export async function searchInstanceForView(
  sdk: CogniteClient,
  node: DocLookupNodeSummary,
  view: DocLookupViewRef,
  query: string
): Promise<DocLookupViewSearchResult> {
  const request = buildViewVersionSearchRequest(node, view, query);
  return executeInstancesSearch(sdk, request);
}
