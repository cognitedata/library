import { useQuery } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import type {
  AnnotationState,
  AnnotationRecord,
  PipelineConfig,
  ViewConfig,
  PatternRecord,
  AnnotationOverviewMetrics,
  GroupedCoverage,
} from "../utils/types";
import { DataProcessor } from "../utils/dataProcessor";
import { FIELD_NAMES, AnnotationStatus } from "../utils/constants";
import { isLocalMockMode } from "@/runtime/authMode";
import {
  localAnnotationStatesByPipeline,
  localAnnotationsByPipeline,
  getLocalAutomaticPatterns,
  getLocalManualPatterns,
} from "@/mocks/mockData";

const CHUNK_SIZE = 1000;
const ANNOTATIONS_CACHE_SCHEMA_VERSION = "v2-potential-enabled";
type AnnotationStatesLoadStage = "states" | "files";

/**
 * Helper to build ViewId from config
 */
function buildViewId(config: ViewConfig) {
  return {
    space: config.schemaSpace,
    externalId: config.externalId,
    version: config.version,
  };
}

/**
 * Fetch all rows from a Raw table
 */
async function fetchRawTableRows(
  sdk: CogniteClient,
  dbName: string,
  tableName: string,
  columns?: string[],
  onProgress?: (rowsRead: number) => void
): Promise<Record<string, unknown>[]> {
  try {
    const rows: Record<string, unknown>[] = [];
    const iterator = sdk.raw.listRows(dbName, tableName, {
      columns,
      limit: CHUNK_SIZE,
    });

    for await (const row of iterator) {
      rows.push({
        _key: row.key,
        ...row.columns,
      });
      if (rows.length % CHUNK_SIZE === 0) {
        onProgress?.(rows.length);
      }
    }

    if (rows.length % CHUNK_SIZE !== 0) {
      onProgress?.(rows.length);
    }

    return rows;
  } catch (error) {
    console.warn(`Failed to fetch Raw table ${dbName}/${tableName}:`, error);
    return [];
  }
}

/**
 * Fetch annotation states from DMS
 */
async function fetchAnnotationStates(
  sdk: CogniteClient,
  config: PipelineConfig,
  onProgress?: (stage: AnnotationStatesLoadStage) => void
): Promise<AnnotationState[]> {
  if (!config.annotationStateView) {
    return [];
  }

  const viewId = buildViewId(config.annotationStateView);
  const fileInstanceSpace = config.fileView?.instanceSpace;
  const stateSpaceFilter = fileInstanceSpace
    ? { equals: { property: ["node", "space"], value: fileInstanceSpace } }
    : undefined;
  const results: AnnotationState[] = [];
  const fileNodeIds: Array<{ space: string; externalId: string }> = [];

  // Fetch annotation state instances in chunks to avoid timeouts/truncation.
  let cursor: string | undefined;
  do {
    onProgress?.("states");
    const instances = await sdk.instances.list({
      instanceType: "node",
      sources: [{ source: { type: "view", ...viewId } }],
      limit: CHUNK_SIZE,
      cursor,
      filter: stateSpaceFilter,
    });

    for (const instance of instances.items) {
      const props = instance.properties?.[viewId.space]?.[
        `${viewId.externalId}/${viewId.version}`
      ] as Record<string, unknown> | undefined;

      if (!props) continue;

      const rawLinkedFile = props.linkedFile as
        | { externalId: string; space: string }
        | undefined;
      const linkedFile = rawLinkedFile
        ? {
            externalId: rawLinkedFile.externalId,
            space: rawLinkedFile.space || fileInstanceSpace || "",
          }
        : undefined;

      if (fileInstanceSpace && linkedFile?.space && linkedFile.space !== fileInstanceSpace) {
        continue;
      }

      const state: AnnotationState = {
        externalId: instance.externalId,
        space: instance.space,
        createdTime: new Date(instance.createdTime),
        lastUpdatedTime: new Date(instance.lastUpdatedTime),
        linkedFile,
        annotationStatus: props.annotationStatus as string | undefined,
        pageCount: props.pageCount as number | undefined,
        annotatedPageCount: props.annotatedPageCount as number | undefined,
        annotationMessage: props.annotationMessage as string | undefined,
        patternModeMessage: props.patternModeMessage as string | undefined,
        launchFunctionId: props.launchFunctionId as number | undefined,
        launchFunctionCallId: props.launchFunctionCallId as number | undefined,
        finalizeFunctionId: props.finalizeFunctionId as number | undefined,
        finalizeFunctionCallId: props.finalizeFunctionCallId as number | undefined,
        prepareFunctionId: props.prepareFunctionId as number | undefined,
        prepareFunctionCallId: props.prepareFunctionCallId as number | undefined,
        promoteFunctionId: props.promoteFunctionId as number | undefined,
        promoteFunctionCallId: props.promoteFunctionCallId as number | undefined,
      };

      results.push(state);

      if (linkedFile) {
        fileNodeIds.push({
          space: linkedFile.space || fileInstanceSpace || "",
          externalId: linkedFile.externalId,
        });
      }
    }

    cursor = instances.nextCursor;
  } while (cursor);

  // Fetch file metadata if we have file view config
  if (config.fileView && fileNodeIds.length > 0) {
    onProgress?.("files");
    const fileViewId = buildViewId(config.fileView);
    const instanceSpace = fileInstanceSpace || fileViewId.space;
    const uniqueFileIds = Array.from(
      new Map(
        fileNodeIds.map((f) => [`${instanceSpace}:${f.externalId}`, {
          space: instanceSpace,
          externalId: f.externalId,
        }])
      ).values()
    );

    try {
      const fileMap = new Map<string, Record<string, unknown>>();

      for (let i = 0; i < uniqueFileIds.length; i += CHUNK_SIZE) {
        const batch = uniqueFileIds.slice(i, i + CHUNK_SIZE);
        const fileInstances = await sdk.instances.retrieve({
          sources: [{ source: { type: "view", ...fileViewId } }],
          items: batch.map((f) => ({
            instanceType: "node" as const,
            space: instanceSpace,
            externalId: f.externalId,
          })),
        });

        for (const fileInstance of fileInstances.items) {
          const fileProps = fileInstance.properties?.[fileViewId.space]?.[
            `${fileViewId.externalId}/${fileViewId.version}`
          ] as Record<string, unknown> | undefined;

          if (fileProps) {
            fileMap.set(`${instanceSpace}:${fileInstance.externalId}`, fileProps);
          }
        }
      }

      // Enrich annotation states with file metadata
      for (const state of results) {
        if (state.linkedFile) {
          const key = `${instanceSpace}:${state.linkedFile.externalId}`;
          const fileProps = fileMap.get(key);
          if (fileProps) {
            state.fileName = fileProps.name as string | undefined;
            state.fileSourceId = fileProps.sourceId as string | undefined;
            state.fileMimeType = fileProps.mimeType as string | undefined;

            if (config.fileResourceProperty) {
              state.fileResourceType = fileProps[
                config.fileResourceProperty
              ] as string | undefined;
            }
            if (config.primaryScopeProperty) {
              state.filePrimaryScope = fileProps[
                config.primaryScopeProperty
              ] as string | undefined;
            }
            if (config.secondaryScopeProperty) {
              state.fileSecondaryScope = fileProps[
                config.secondaryScopeProperty
              ] as string | undefined;
            }
          }
        }
      }
    } catch (error) {
      console.warn("Failed to fetch file metadata:", error);
    }
  }

  return results;
}

export function extractFileExternalIdFromKey(
  key: string | undefined,
  tableKind: "pattern" | "tag"
): string | undefined {
  if (!key) return undefined;
  if (tableKind === "pattern") {
    return extractPatternFileExternalIdFromKey(key);
  }
  const first = key.split(":", 1)[0];
  return first || undefined;
}

/**
 * Extract file external ID from startNode (format: "space:externalId" or just "externalId")
 */
export function extractFileExternalId(startNode: string | undefined): string | undefined {
  if (!startNode) return undefined;
  // Handle "space:externalId" format
  if (startNode.includes(":")) {
    return startNode.split(":").pop();
  }
  return startNode;
}

function extractFileSpace(startNode: string | undefined): string | undefined {
  if (!startNode) return undefined;
  const separatorIndex = startNode.indexOf(":");
  if (separatorIndex <= 0) return undefined;
  return startNode.slice(0, separatorIndex);
}

export function buildAnnotationRecord(
  row: Record<string, unknown>,
  isFromPatternTable: boolean,
  fileExternalIdOverride?: string
): AnnotationRecord {
  const record = mapRowToAnnotationRecord(row, isFromPatternTable);
  const key = row._key as string | undefined;
  const fileExternalId =
    fileExternalIdOverride ||
    extractFileExternalId(record.startNode) ||
    (isFromPatternTable
      ? extractPatternFileExternalIdFromKey(key)
      : extractRawTagFileExternalIdFromKey(key));

  const startNodeText =
    record.startNodeText ||
    (isFromPatternTable ? extractPatternTagTextFromKey(key) : undefined);

  const enrichedRecord: AnnotationRecord = {
    ...record,
    fileExternalId,
    startNodeText,
  };

  return {
    ...enrichedRecord,
    normalizedStatus: DataProcessor.deriveNormalizedStatus(enrichedRecord),
  };
}

function calculateCoverageEntry(actualCount: number, potentialCount: number) {
  const totalPossible = actualCount + potentialCount;
  const coveragePct = totalPossible > 0 ? (actualCount / totalPossible) * 100 : 0;
  return {
    groupKey: "",
    coveragePct,
    actualCount,
    potentialCount,
    totalPossible,
  };
}

function toGroupedCoverage(
  aggregates: Map<string, { actualCount: number; potentialCount: number }>
): GroupedCoverage[] {
  const result: GroupedCoverage[] = [];
  for (const [groupKey, counts] of aggregates.entries()) {
    const coverage = calculateCoverageEntry(counts.actualCount, counts.potentialCount);
    result.push({ ...coverage, groupKey });
  }
  return result.sort((a, b) => b.coveragePct - a.coveragePct);
}

function incrementAggregate(
  map: Map<string, { actualCount: number; potentialCount: number }>,
  key: string,
  isActual: boolean
) {
  const entry = map.get(key) || { actualCount: 0, potentialCount: 0 };
  if (isActual) {
    entry.actualCount += 1;
  } else {
    entry.potentialCount += 1;
  }
  map.set(key, entry);
}

export function extractPatternFileExternalIdFromKey(key: string | undefined): string | undefined {
  if (!key) return undefined;

  if (key.startsWith("pattern:")) {
    const parts = key.split(":");
    return parts.length > 1 ? parts[1] : undefined;
  }

  if (key.startsWith("pattern_")) {
    const afterPrefix = key.slice("pattern_".length);
    const firstSeparator = afterPrefix.indexOf(":");
    if (firstSeparator <= 0) return undefined;
    return afterPrefix.slice(0, firstSeparator);
  }

  return undefined;
}

export function extractPatternTagTextFromKey(key: string | undefined): string | undefined {
  if (!key) return undefined;

  if (key.startsWith("pattern:")) {
    const parts = key.split(":");
    return parts.length > 2 ? parts[2] : undefined;
  }

  if (key.startsWith("pattern_")) {
    const afterPrefix = key.slice("pattern_".length);
    const parts = afterPrefix.split(":");
    return parts.length > 1 ? parts[1] : undefined;
  }

  return undefined;
}

export function extractRawTagFileExternalIdFromKey(key: string | undefined): string | undefined {
  if (!key) return undefined;
  const firstSeparator = key.indexOf(":");
  if (firstSeparator <= 0) return undefined;
  return key.slice(0, firstSeparator);
}

async function fetchAnnotationOverviewMetrics(
  sdk: CogniteClient,
  config: PipelineConfig
): Promise<AnnotationOverviewMetrics> {
  const rawDb = config.rawDb;
  if (!rawDb) {
    return {
      overallCoverage: DataProcessor.calculateCoverage([], []),
      coverageByTagResourceType: [],
      coverageByFileResourceType: [],
      coverageByPrimaryScope: [],
      coverageBySecondaryScope: [],
    };
  }

  const overallCounts = { actualCount: 0, potentialCount: 0 };
  const tagResourceTypeAgg = new Map<string, { actualCount: number; potentialCount: number }>();
  const byFileIdAgg = new Map<string, { actualCount: number; potentialCount: number }>();

  const processTable = async (
    tableName: string,
    kind: "asset" | "file" | "pattern"
  ) => {
    const iterator = sdk.raw.listRows(rawDb, tableName, {
      columns: SUMMARY_ANNOTATION_COLUMNS,
      limit: CHUNK_SIZE,
    });

    for await (const row of iterator) {
      const data = { _key: row.key, ...row.columns } as Record<string, unknown>;
      const record = buildAnnotationRecord(data, kind === "pattern");

      const hasEndNode = !!record.endNode && String(record.endNode).trim() !== "";
      if (kind === "pattern") {
        const isApprovedPattern = record.status === AnnotationStatus.APPROVED;
        if (isApprovedPattern && !hasEndNode) continue;
      } else if (!hasEndNode) {
        continue;
      }

      const parsedFromStartNode = extractFileExternalId(record.startNode);
      const parsedFromKey =
        kind === "pattern"
          ? extractPatternFileExternalIdFromKey(row.key)
          : extractRawTagFileExternalIdFromKey(row.key);
      const fileExternalId = parsedFromStartNode || parsedFromKey;
      if (!fileExternalId) continue;

      const isActual = kind === "pattern" ? record.status === AnnotationStatus.APPROVED : true;
      if (isActual) {
        overallCounts.actualCount += 1;
      } else {
        overallCounts.potentialCount += 1;
      }

      incrementAggregate(byFileIdAgg, fileExternalId, isActual);
      incrementAggregate(tagResourceTypeAgg, record.endNodeResourceType || "Unknown", isActual);
    }
  };

  if (config.rawTableAssetTags) {
    await processTable(config.rawTableAssetTags, "asset");
  }
  if (config.rawTableFileTags) {
    await processTable(config.rawTableFileTags, "file");
  }
  if (config.rawTablePatternTags) {
    await processTable(config.rawTablePatternTags, "pattern");
  }

  const byFileResourceTypeAgg = new Map<string, { actualCount: number; potentialCount: number }>();
  const byPrimaryScopeAgg = new Map<string, { actualCount: number; potentialCount: number }>();
  const bySecondaryScopeAgg = new Map<string, { actualCount: number; potentialCount: number }>();

  if (config.fileView && byFileIdAgg.size > 0) {
    try {
      const viewId = buildViewId(config.fileView);
      const instanceSpace = config.fileView.instanceSpace || viewId.space;
      const fileIds = Array.from(byFileIdAgg.keys());
      const filePropsMap = new Map<string, Record<string, unknown>>();

      for (let i = 0; i < fileIds.length; i += CHUNK_SIZE) {
        const batch = fileIds.slice(i, i + CHUNK_SIZE);
        const instances = await sdk.instances.retrieve({
          sources: [{ source: { type: "view", ...viewId } }],
          items: batch.map((externalId) => ({
            instanceType: "node" as const,
            space: instanceSpace,
            externalId,
          })),
        });

        for (const instance of instances.items) {
          const props = instance.properties?.[viewId.space]?.[
            `${viewId.externalId}/${viewId.version}`
          ] as Record<string, unknown> | undefined;
          if (props) {
            filePropsMap.set(instance.externalId, props);
          }
        }
      }

      for (const [fileId, counts] of byFileIdAgg.entries()) {
        const props = filePropsMap.get(fileId);
        const fileResourceType =
          config.fileResourceProperty && props
            ? String(props[config.fileResourceProperty] ?? "Unknown")
            : "Unknown";
        const primaryScope =
          config.primaryScopeProperty && props
            ? String(props[config.primaryScopeProperty] ?? "Unknown")
            : "Unknown";
        const secondaryScope =
          config.secondaryScopeProperty && props
            ? String(props[config.secondaryScopeProperty] ?? "Unknown")
            : "Unknown";

        incrementAggregate(byFileResourceTypeAgg, fileResourceType, true);
        incrementAggregate(byPrimaryScopeAgg, primaryScope, true);
        incrementAggregate(bySecondaryScopeAgg, secondaryScope, true);

        const byFileResourceTypeEntry = byFileResourceTypeAgg.get(fileResourceType);
        const byPrimaryScopeEntry = byPrimaryScopeAgg.get(primaryScope);
        const bySecondaryScopeEntry = bySecondaryScopeAgg.get(secondaryScope);

        if (byFileResourceTypeEntry) {
          byFileResourceTypeEntry.actualCount -= 1;
          byFileResourceTypeEntry.actualCount += counts.actualCount;
          byFileResourceTypeEntry.potentialCount += counts.potentialCount;
          byFileResourceTypeAgg.set(fileResourceType, byFileResourceTypeEntry);
        }

        if (byPrimaryScopeEntry) {
          byPrimaryScopeEntry.actualCount -= 1;
          byPrimaryScopeEntry.actualCount += counts.actualCount;
          byPrimaryScopeEntry.potentialCount += counts.potentialCount;
          byPrimaryScopeAgg.set(primaryScope, byPrimaryScopeEntry);
        }

        if (bySecondaryScopeEntry) {
          bySecondaryScopeEntry.actualCount -= 1;
          bySecondaryScopeEntry.actualCount += counts.actualCount;
          bySecondaryScopeEntry.potentialCount += counts.potentialCount;
          bySecondaryScopeAgg.set(secondaryScope, bySecondaryScopeEntry);
        }
      }
    } catch (error) {
      console.warn("Failed to enrich overview metrics with file metadata:", error);
    }
  }

  if (byFileResourceTypeAgg.size === 0) {
    for (const [fileId, counts] of byFileIdAgg.entries()) {
      incrementAggregate(byFileResourceTypeAgg, fileId, true);
      const entry = byFileResourceTypeAgg.get(fileId);
      if (entry) {
        entry.actualCount -= 1;
        entry.actualCount += counts.actualCount;
        entry.potentialCount += counts.potentialCount;
        byFileResourceTypeAgg.set(fileId, entry);
      }
    }
  }

  const overallCoverage = {
    ...calculateCoverageEntry(overallCounts.actualCount, overallCounts.potentialCount),
    groupKey: "overall",
  };

  return {
    overallCoverage: {
      coveragePct: overallCoverage.coveragePct,
      actualCount: overallCoverage.actualCount,
      potentialCount: overallCoverage.potentialCount,
      totalPossible: overallCoverage.totalPossible,
    },
    coverageByTagResourceType: toGroupedCoverage(tagResourceTypeAgg),
    coverageByFileResourceType: toGroupedCoverage(byFileResourceTypeAgg),
    coverageByPrimaryScope: toGroupedCoverage(byPrimaryScopeAgg),
    coverageBySecondaryScope: toGroupedCoverage(bySecondaryScopeAgg),
  };
}

async function enrichWithFileMetadata(
  sdk: CogniteClient,
  config: PipelineConfig,
  records: AnnotationRecord[]
) {
  if (!config.fileView || records.length === 0) return;

  const fileIds = new Set<string>();
  for (const r of records) {
    if (r.fileExternalId) {
      fileIds.add(r.fileExternalId);
    }
  }

  if (fileIds.size === 0) return;

  try {
    const viewId = buildViewId(config.fileView);
    const instanceSpace = config.fileView.instanceSpace || viewId.space;
    const fileIdArray = Array.from(fileIds);
    const batchSize = CHUNK_SIZE;
    const fileMetadataMap = new Map<string, Record<string, unknown>>();

    for (let i = 0; i < fileIdArray.length; i += batchSize) {
      const batch = fileIdArray.slice(i, i + batchSize);
      const instances = await sdk.instances.retrieve({
        sources: [{ source: { type: "view", ...viewId } }],
        items: batch.map((extId) => ({
          instanceType: "node" as const,
          space: instanceSpace,
          externalId: extId,
        })),
      });

      for (const instance of instances.items) {
        const props = instance.properties?.[viewId.space]?.[
          `${viewId.externalId}/${viewId.version}`
        ] as Record<string, unknown> | undefined;

        if (props) {
          fileMetadataMap.set(instance.externalId, props);
        }
      }
    }

    for (const record of records) {
      if (!record.fileExternalId) continue;
      const fileProps = fileMetadataMap.get(record.fileExternalId);
      if (!fileProps) continue;

      record.fileName = fileProps.name as string | undefined;
      record.fileSourceId = fileProps.sourceId as string | undefined;
      record.fileResourceType = config.fileResourceProperty
        ? (fileProps[config.fileResourceProperty] as string | undefined)
        : undefined;
      record.filePrimaryScope = config.primaryScopeProperty
        ? (fileProps[config.primaryScopeProperty] as string | undefined)
        : undefined;
      record.fileSecondaryScope = config.secondaryScopeProperty
        ? (fileProps[config.secondaryScopeProperty] as string | undefined)
        : undefined;
    }
  } catch (error) {
    console.warn("Failed to fetch file metadata for annotations:", error);
  }
}

export const FULL_ANNOTATION_COLUMNS = [
  FIELD_NAMES.EXTERNAL_ID,
  FIELD_NAMES.START_NODE_RESOURCE,
  FIELD_NAMES.END_NODE_RESOURCE,
  FIELD_NAMES.STATUS,
  FIELD_NAMES.START_NODE_TEXT,
  FIELD_NAMES.START_NODE,
  FIELD_NAMES.END_NODE_RESOURCE_TYPE,
  FIELD_NAMES.START_SOURCE_ID,
  FIELD_NAMES.END_NODE,
  FIELD_NAMES.END_NODE_SPACE,
  FIELD_NAMES.TAGS,
  // Bounding box columns for canvas visualization
  FIELD_NAMES.PAGE,
  FIELD_NAMES.CONFIDENCE,
  FIELD_NAMES.START_NODE_X_MIN,
  FIELD_NAMES.START_NODE_Y_MIN,
  FIELD_NAMES.START_NODE_X_MAX,
  FIELD_NAMES.START_NODE_Y_MAX,
];

export const SUMMARY_ANNOTATION_COLUMNS = [
  FIELD_NAMES.STATUS,
  FIELD_NAMES.START_NODE,
  FIELD_NAMES.END_NODE,
  FIELD_NAMES.END_NODE_RESOURCE_TYPE,
];

/**
 * Fetch annotations from Raw tables and enrich with file metadata
 */
async function fetchAnnotations(
  sdk: CogniteClient,
  config: PipelineConfig,
  columns: string[],
  onProgress?: (message: string) => void
): Promise<{ actual: AnnotationRecord[]; potential: AnnotationRecord[] }> {
  const rawDb = config.rawDb;
  if (!rawDb) {
    console.warn("No rawDb configured in pipeline config");
    return { actual: [], potential: [] };
  }

  const log = onProgress;
  log?.("Step: annotations fetch start");

  const fetchTable = async (
    tableName: string,
    tableKind: "asset" | "file" | "pattern"
  ) => {
    try {
      log?.(`Step: ${tableName} listRows start`);
      const rows = await fetchRawTableRows(
        sdk,
        rawDb,
        tableName,
        columns,
        (rowsRead) => log?.(`${tableName}: ${rowsRead.toLocaleString()} rows`)
      );
      log?.(`${tableName} read: ${rows.length.toLocaleString()} rows`);

      const actual: AnnotationRecord[] = [];
      const potential: AnnotationRecord[] = [];
      const isPattern = tableKind === "pattern";

      for (const row of rows) {
        const key = row._key as string | undefined;
        const record = buildAnnotationRecord(
          row,
          isPattern,
          extractFileExternalId(row[FIELD_NAMES.START_NODE] as string | undefined) ||
            (isPattern
              ? extractPatternFileExternalIdFromKey(key)
              : extractRawTagFileExternalIdFromKey(key))
        );

        if (isPattern) {
          if (record.status === AnnotationStatus.APPROVED) {
            if (record.endNode && String(record.endNode).trim() !== "") {
              actual.push(record);
            }
          } else {
            potential.push(record);
          }
        } else {
          actual.push(record);
        }
      }

      return { actual, potential };
    } catch (error) {
      console.warn(`Failed to fetch ${tableName}:`, error);
      return { actual: [], potential: [] };
    }
  };

  const tasks: Array<Promise<{ actual: AnnotationRecord[]; potential: AnnotationRecord[] }>> = [];
  if (config.rawTableAssetTags) {
    tasks.push(fetchTable(config.rawTableAssetTags, "asset"));
  }
  if (config.rawTableFileTags) {
    tasks.push(fetchTable(config.rawTableFileTags, "file"));
  }
  if (config.rawTablePatternTags) {
    tasks.push(fetchTable(config.rawTablePatternTags, "pattern"));
  }

  const results = await Promise.all(tasks);
  let actualRecords = results.flatMap((result) => result.actual);
  let potentialRecords = results.flatMap((result) => result.potential);

  // Filter out empty endNode only for actual records.
  actualRecords = actualRecords.filter(
    (r) => r.endNode && String(r.endNode).trim() !== ""
  );

  log?.("Step: metadata enrichment start");
  await enrichWithFileMetadata(sdk, config, [...actualRecords, ...potentialRecords]);
  log?.("Step: annotations fetch done");

  return { actual: actualRecords, potential: potentialRecords };
}

async function fetchAnnotationsForFiles(
  sdk: CogniteClient,
  config: PipelineConfig,
  fileExternalIds: string[]
): Promise<{ actual: AnnotationRecord[]; potential: AnnotationRecord[] }> {
  const rawDb = config.rawDb;
  if (!rawDb || fileExternalIds.length === 0) {
    return { actual: [], potential: [] };
  }

  const fileIdSet = new Set(fileExternalIds);
  const actualRecords: AnnotationRecord[] = [];
  const potentialRecords: AnnotationRecord[] = [];

  const appendFromTable = async (
    tableName: string,
    tableKind: "pattern" | "tag",
    isPattern: boolean
  ) => {
    const iterator = sdk.raw.listRows(rawDb, tableName, {
      columns: FULL_ANNOTATION_COLUMNS,
      limit: CHUNK_SIZE,
    });

    for await (const row of iterator) {
      const startNode = row.columns?.[FIELD_NAMES.START_NODE] as string | undefined;
      const fileSpace = extractFileSpace(startNode);
      if (config.fileView?.instanceSpace && fileSpace) {
        if (fileSpace !== config.fileView.instanceSpace) continue;
      }

      const fileExternalId = extractFileExternalIdFromKey(row.key, tableKind);
      if (!fileExternalId || !fileIdSet.has(fileExternalId)) continue;

      const record = buildAnnotationRecord(
        { _key: row.key, ...row.columns },
        isPattern,
        fileExternalId
      );

      if (isPattern) {
        if (record.status === AnnotationStatus.APPROVED) {
          if (!record.endNode || String(record.endNode).trim() === "") continue;
          actualRecords.push(record);
        } else {
          // Keep unresolved potential rows even without endNode.
          potentialRecords.push(record);
        }
      } else {
        if (!record.endNode || String(record.endNode).trim() === "") continue;
        actualRecords.push(record);
      }
    }
  };

  const tasks: Array<Promise<void>> = [];
  if (config.rawTableAssetTags) {
    tasks.push(appendFromTable(config.rawTableAssetTags, "tag", false));
  }
  if (config.rawTableFileTags) {
    tasks.push(appendFromTable(config.rawTableFileTags, "tag", false));
  }
  if (config.rawTablePatternTags) {
    tasks.push(appendFromTable(config.rawTablePatternTags, "pattern", true));
  }
  await Promise.all(tasks);

  await enrichWithFileMetadata(sdk, config, [...actualRecords, ...potentialRecords]);

  return { actual: actualRecords, potential: potentialRecords };
}

/**
 * Map raw row to annotation record
 */
function mapRowToAnnotationRecord(
  row: Record<string, unknown>,
  isFromPatternTable: boolean
): AnnotationRecord {
  // Extract bounding box if all coordinates are present
  const xMin = row[FIELD_NAMES.START_NODE_X_MIN] as number | undefined;
  const yMin = row[FIELD_NAMES.START_NODE_Y_MIN] as number | undefined;
  const xMax = row[FIELD_NAMES.START_NODE_X_MAX] as number | undefined;
  const yMax = row[FIELD_NAMES.START_NODE_Y_MAX] as number | undefined;

  const boundingBox =
    xMin != null && yMin != null && xMax != null && yMax != null
      ? { xMin, yMin, xMax, yMax }
      : undefined;

  return {
    externalId: row[FIELD_NAMES.EXTERNAL_ID] as string | undefined,
    startNode: row[FIELD_NAMES.START_NODE] as string | undefined,
    startNodeText: row[FIELD_NAMES.START_NODE_TEXT] as string | undefined,
    endNode: row[FIELD_NAMES.END_NODE] as string | undefined,
    endNodeResourceType: row[FIELD_NAMES.END_NODE_RESOURCE_TYPE] as string | undefined,
    status: row[FIELD_NAMES.STATUS] as string | undefined,
    tags: row[FIELD_NAMES.TAGS] as string | string[] | undefined,
    isFromPatternTable,
    page: row[FIELD_NAMES.PAGE] as number | undefined,
    confidence: row[FIELD_NAMES.CONFIDENCE] as number | undefined,
    boundingBox,
  };
}

/**
 * Fetch manual patterns from Raw table
 */
async function fetchManualPatterns(
  sdk: CogniteClient,
  config: PipelineConfig
): Promise<PatternRecord[]> {
  const rawDb = config.rawDb;
  const tableName = config.rawManualPatternsCatalog;

  if (!rawDb || !tableName) {
    return [];
  }

  try {
    const rows = await fetchRawTableRows(sdk, rawDb, tableName);
    const patterns: PatternRecord[] = [];

    for (const row of rows) {
      const patternScope = row._key as string;
      const patternsData = row.patterns as Array<{
        sample?: string | string[];
        resource_type?: string;
        annotation_type?: string;
      }> | undefined;

      if (!patternsData) continue;

      for (const pattern of patternsData) {
        const samples = Array.isArray(pattern.sample)
          ? pattern.sample
          : [pattern.sample];

        for (const sample of samples) {
          if (sample) {
            patterns.push({
              sample,
              resourceType: pattern.resource_type,
              annotationType: mapAnnotationType(pattern.annotation_type),
              patternScope,
              createdBy: "manual",
            });
          }
        }
      }
    }

    return patterns;
  } catch (error) {
    console.warn("Failed to fetch manual patterns:", error);
    return [];
  }
}

/**
 * Fetch automatic patterns from Raw table
 */
async function fetchAutomaticPatterns(
  sdk: CogniteClient,
  config: PipelineConfig
): Promise<PatternRecord[]> {
  const rawDb = config.rawDb;
  const tableName = config.rawTablePatternCache;

  if (!rawDb || !tableName) {
    return [];
  }

  try {
    const rows = await fetchRawTableRows(sdk, rawDb, tableName);
    const patterns: PatternRecord[] = [];

    for (const row of rows) {
      const patternScope = row._key as string;

      const filePatterns = row.FilePatternSamples as Array<{
        sample?: string | string[];
        resource_type?: string;
        annotation_type?: string;
      }> | undefined;

      const assetPatterns = row.AssetPatternSamples as Array<{
        sample?: string | string[];
        resource_type?: string;
        annotation_type?: string;
      }> | undefined;

      // Process file patterns
      if (filePatterns) {
        for (const pattern of filePatterns) {
          const samples = Array.isArray(pattern.sample)
            ? pattern.sample
            : [pattern.sample];

          for (const sample of samples) {
            if (sample) {
              patterns.push({
                sample,
                resourceType: pattern.resource_type,
                annotationType: mapAnnotationType(pattern.annotation_type),
                patternScope,
                entityType: "File",
                createdBy: "automatic",
              });
            }
          }
        }
      }

      // Process asset patterns
      if (assetPatterns) {
        for (const pattern of assetPatterns) {
          const samples = Array.isArray(pattern.sample)
            ? pattern.sample
            : [pattern.sample];

          for (const sample of samples) {
            if (sample) {
              patterns.push({
                sample,
                resourceType: pattern.resource_type,
                annotationType: mapAnnotationType(pattern.annotation_type),
                patternScope,
                entityType: "Asset",
                createdBy: "automatic",
              });
            }
          }
        }
      }
    }

    return patterns;
  } catch (error) {
    console.warn("Failed to fetch automatic patterns:", error);
    return [];
  }
}

/**
 * Map annotation type from API format to display format
 */
function mapAnnotationType(type: string | undefined): string | undefined {
  if (!type) return undefined;
  if (type === "diagrams.AssetLink") return "Asset";
  if (type === "diagrams.FileLink") return "File";
  return type;
}

/**
 * Hook to fetch annotation states
 */
export function useAnnotationStates(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null,
  options?: { enabled?: boolean; onProgress?: (stage: AnnotationStatesLoadStage) => void }
) {
  return useQuery({
    queryKey: [
      "annotationStates",
      isLocalMockMode ? "local" : config?.annotationStateView?.externalId,
      pipelineId,
    ],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(
          pipelineId ? localAnnotationStatesByPipeline[pipelineId] || [] : []
        );
      }
      if (!sdk || !config) return [];
      return fetchAnnotationStates(sdk, config, options?.onProgress);
    },
    enabled:
      options?.enabled !== false &&
      ((isLocalMockMode && !!pipelineId) || (!!sdk && !!config?.annotationStateView)),
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

/**
 * Hook to calculate KPIs from annotation states
 */
export function usePipelineKPIs(states: AnnotationState[]) {
  return DataProcessor.calculatePipelineKPIs(states);
}

/**
 * Hook to fetch annotations from Raw tables
 */
export function useAnnotations(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null,
  options?: { enabled?: boolean; onProgress?: (message: string) => void }
) {
  return useQuery({
    queryKey: [
      "annotations",
      ANNOTATIONS_CACHE_SCHEMA_VERSION,
      isLocalMockMode ? "local" : config?.rawDb,
      config?.rawTableAssetTags,
      config?.rawTableFileTags,
      config?.rawTablePatternTags,
      pipelineId,
    ],
    queryFn: async (): Promise<{
      actual: AnnotationRecord[];
      potential: AnnotationRecord[];
    }> => {
      if (isLocalMockMode) {
        return Promise.resolve(
          pipelineId ? localAnnotationsByPipeline[pipelineId] || { actual: [], potential: [] } : { actual: [], potential: [] }
        );
      }
      if (!sdk || !config) {
        return { actual: [], potential: [] };
      }
      return fetchAnnotations(sdk, config, FULL_ANNOTATION_COLUMNS, options?.onProgress);
    },
    enabled:
      options?.enabled !== false &&
      ((isLocalMockMode && !!pipelineId) || (!!sdk && !!config?.rawDb)),
    staleTime: 60 * 60 * 1000, // 1 hour
    gcTime: 60 * 60 * 1000, // 1 hour
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
}

/**
 * Hook to fetch annotations for selected files only
 */
export function useAnnotationsForFiles(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null,
  fileExternalIds: string[]
) {
  const stableFileIds = [...fileExternalIds].sort();
  return useQuery({
    queryKey: [
      "annotationsByFile",
      isLocalMockMode ? "local" : config?.rawDb,
      pipelineId,
      stableFileIds.join(","),
    ],
    queryFn: async (): Promise<{
      actual: AnnotationRecord[];
      potential: AnnotationRecord[];
    }> => {
      if (stableFileIds.length === 0) {
        return { actual: [], potential: [] };
      }
      if (isLocalMockMode) {
        const local = pipelineId
          ? localAnnotationsByPipeline[pipelineId] || { actual: [], potential: [] }
          : { actual: [], potential: [] };
        return {
          actual: local.actual.filter((r) => r.fileExternalId && stableFileIds.includes(r.fileExternalId)),
          potential: local.potential.filter((r) => r.fileExternalId && stableFileIds.includes(r.fileExternalId)),
        };
      }
      if (!sdk || !config) {
        return { actual: [], potential: [] };
      }
      return fetchAnnotationsForFiles(sdk, config, stableFileIds);
    },
    enabled:
      stableFileIds.length > 0 &&
      ((isLocalMockMode && !!pipelineId) || (!!sdk && !!config?.rawDb)),
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Hook to fetch annotation summary (lightweight columns)
 */
export function useAnnotationSummary(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null,
  options?: { enabled?: boolean }
) {
  return useQuery({
    queryKey: [
      "annotationSummary",
      ANNOTATIONS_CACHE_SCHEMA_VERSION,
      isLocalMockMode ? "local" : config?.rawDb,
      config?.rawTableAssetTags,
      config?.rawTableFileTags,
      config?.rawTablePatternTags,
      pipelineId,
    ],
    queryFn: async (): Promise<{
      actual: AnnotationRecord[];
      potential: AnnotationRecord[];
    }> => {
      if (isLocalMockMode) {
        return Promise.resolve(
          pipelineId ? localAnnotationsByPipeline[pipelineId] || { actual: [], potential: [] } : { actual: [], potential: [] }
        );
      }
      if (!sdk || !config) {
        return { actual: [], potential: [] };
      }
      return fetchAnnotations(sdk, config, SUMMARY_ANNOTATION_COLUMNS);
    },
    enabled:
      options?.enabled !== false &&
      ((isLocalMockMode && !!pipelineId) || (!!sdk && !!config?.rawDb)),
    staleTime: 5 * 60 * 1000,
  });
}

export function useAnnotationOverviewMetrics(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null,
  options?: { enabled?: boolean }
) {
  return useQuery({
    queryKey: [
      "annotationOverviewMetrics",
      isLocalMockMode ? "local" : config?.rawDb,
      config?.rawTableAssetTags,
      config?.rawTableFileTags,
      config?.rawTablePatternTags,
      pipelineId,
      config?.fileResourceProperty,
      config?.secondaryScopeProperty,
    ],
    queryFn: async (): Promise<AnnotationOverviewMetrics> => {
      if (isLocalMockMode) {
        const local = pipelineId
          ? localAnnotationsByPipeline[pipelineId] || { actual: [], potential: [] }
          : { actual: [], potential: [] };
        return {
          overallCoverage: DataProcessor.calculateCoverage(local.actual, local.potential),
          coverageByTagResourceType: DataProcessor.calculateGroupedCoverage(
            local.actual,
            local.potential,
            "endNodeResourceType"
          ),
          coverageByFileResourceType: DataProcessor.calculateGroupedCoverage(
            local.actual,
            local.potential,
            "fileResourceType"
          ),
          coverageByPrimaryScope: DataProcessor.calculateGroupedCoverage(
            local.actual,
            local.potential,
            "filePrimaryScope"
          ),
          coverageBySecondaryScope: DataProcessor.calculateGroupedCoverage(
            local.actual,
            local.potential,
            "fileSecondaryScope"
          ),
        };
      }

      if (!sdk || !config) {
        return {
          overallCoverage: DataProcessor.calculateCoverage([], []),
          coverageByTagResourceType: [],
          coverageByFileResourceType: [],
          coverageByPrimaryScope: [],
          coverageBySecondaryScope: [],
        };
      }

      return fetchAnnotationOverviewMetrics(sdk, config);
    },
    enabled:
      options?.enabled !== false &&
      ((isLocalMockMode && !!pipelineId) || (!!sdk && !!config?.rawDb)),
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Hook to fetch manual patterns
 */
export function useManualPatterns(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null
) {
  return useQuery({
    queryKey: [
      "manualPatterns",
      isLocalMockMode ? "local" : config?.rawDb,
      config?.rawManualPatternsCatalog,
      pipelineId,
    ],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(pipelineId ? getLocalManualPatterns(pipelineId) : []);
      }
      if (!sdk || !config) return [];
      return fetchManualPatterns(sdk, config);
    },
    enabled:
      (isLocalMockMode && !!pipelineId) ||
      (!!sdk && !!config?.rawDb && !!config?.rawManualPatternsCatalog),
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Hook to fetch automatic patterns
 */
export function useAutomaticPatterns(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  pipelineId: string | null
) {
  return useQuery({
    queryKey: [
      "automaticPatterns",
      isLocalMockMode ? "local" : config?.rawDb,
      config?.rawTablePatternCache,
      pipelineId,
    ],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(pipelineId ? getLocalAutomaticPatterns(pipelineId) : []);
      }
      if (!sdk || !config) return [];
      return fetchAutomaticPatterns(sdk, config);
    },
    enabled:
      (isLocalMockMode && !!pipelineId) ||
      (!!sdk && !!config?.rawDb && !!config?.rawTablePatternCache),
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Hook to get file metadata from DMS
 */
export function useFileMetadata(
  sdk: CogniteClient | null,
  config: PipelineConfig | null,
  fileIds: Array<{ space: string; externalId: string }>
) {
  return useQuery({
    queryKey: [
      "fileMetadata",
      config?.fileView?.externalId,
      fileIds.map((f) => f.externalId).join(","),
    ],
    queryFn: async () => {
      if (!sdk || !config?.fileView || fileIds.length === 0) return [];

      const viewId = buildViewId(config.fileView);
      const results: Array<{
        externalId: string;
        space: string;
        name?: string;
        sourceId?: string;
        mimeType?: string;
        resourceType?: string;
        secondaryScope?: string;
      }> = [];

      for (let i = 0; i < fileIds.length; i += CHUNK_SIZE) {
        const batch = fileIds.slice(i, i + CHUNK_SIZE);
        const instances = await sdk.instances.retrieve({
          sources: [{ source: { type: "view", ...viewId } }],
          items: batch.map((f) => ({
            instanceType: "node" as const,
            space: f.space,
            externalId: f.externalId,
          })),
        });

        for (const instance of instances.items) {
          const props = instance.properties?.[viewId.space]?.[
            `${viewId.externalId}/${viewId.version}`
          ] as Record<string, unknown> | undefined;

          results.push({
            externalId: instance.externalId,
            space: instance.space,
            name: props?.name as string | undefined,
            sourceId: props?.sourceId as string | undefined,
            mimeType: props?.mimeType as string | undefined,
            resourceType: config.fileResourceProperty
              ? (props?.[config.fileResourceProperty] as string | undefined)
              : undefined,
            secondaryScope: config.secondaryScopeProperty
              ? (props?.[config.secondaryScopeProperty] as string | undefined)
              : undefined,
          });
        }
      }

      return results;
    },
    enabled: !!sdk && !!config?.fileView && fileIds.length > 0,
    staleTime: 10 * 60 * 1000, // 10 minutes
  });
}
