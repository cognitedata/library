import type {
  AnnotationRecord,
  AnnotationState,
  PatternRecord,
  PipelineConfig,
  PipelineRun,
} from "@/shared/utils/types";
import { AnnotationStatus, CallerType } from "@/shared/utils/constants";

export interface LocalFileInfo {
  id: number;
  externalId: string;
  name: string;
  mimeType: string;
  previewUrl?: string;
  pageCount?: number;
}

const previewSvg =
  "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1400\" height=\"900\">" +
  "<rect width=\"1400\" height=\"900\" fill=\"#f8fafc\"/>" +
  "<rect x=\"80\" y=\"80\" width=\"1240\" height=\"740\" fill=\"#ffffff\" stroke=\"#cbd5f5\" stroke-width=\"4\"/>" +
  "<text x=\"120\" y=\"140\" font-family=\"Arial\" font-size=\"32\" fill=\"#1e293b\">Local File Preview</text>" +
  "<text x=\"120\" y=\"190\" font-family=\"Arial\" font-size=\"18\" fill=\"#64748b\">Placeholder canvas used in local mode</text>" +
  "<rect x=\"220\" y=\"260\" width=\"320\" height=\"120\" fill=\"#e2e8f0\" stroke=\"#94a3b8\"/>" +
  "<rect x=\"640\" y=\"340\" width=\"420\" height=\"180\" fill=\"#e2e8f0\" stroke=\"#94a3b8\"/>" +
  "<rect x=\"280\" y=\"520\" width=\"540\" height=\"180\" fill=\"#e2e8f0\" stroke=\"#94a3b8\"/>" +
  "</svg>";

const previewUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(previewSvg)}`;

const demoPipelineId = "file_annotation_demo";

export const localPipelines = [demoPipelineId];

export const localPipelineConfigById: Record<string, PipelineConfig> = {
  [demoPipelineId]: {
    annotationStateView: {
      schemaSpace: "file_annotation",
      externalId: "AnnotationState",
      version: "1",
      instanceSpace: "file_annotation",
    },
    fileView: {
      schemaSpace: "cdf_cdm",
      externalId: "CogniteFile",
      version: "v1",
      instanceSpace: "file_annotation",
    },
    fileResourceProperty: "resourceType",
    secondaryScopeProperty: "area",
    rawDb: "file_annotation_local",
    rawTablePatternTags: "pattern_tags",
    rawTableAssetTags: "asset_tags",
    rawTableFileTags: "file_tags",
    rawTablePatternCache: "pattern_cache",
    rawManualPatternsCatalog: "manual_patterns",
  },
};

const localFileInfos: LocalFileInfo[] = [
  {
    id: 101,
    externalId: "FILE-0001",
    name: "PID-001.pdf",
    mimeType: "application/pdf",
    previewUrl,
    pageCount: 1,
  },
  {
    id: 102,
    externalId: "FILE-0002",
    name: "PID-002.pdf",
    mimeType: "application/pdf",
    previewUrl,
    pageCount: 1,
  },
  {
    id: 103,
    externalId: "FILE-0003",
    name: "DATA-SHEET-003.pdf",
    mimeType: "application/pdf",
    previewUrl,
    pageCount: 1,
  },
];

const fileInfoByExternalId = new Map(
  localFileInfos.map((info) => [info.externalId, info])
);
const fileInfoById = new Map(localFileInfos.map((info) => [info.id, info]));

export function getLocalFileInfo(externalId: string, fileName?: string) {
  if (externalId && fileInfoByExternalId.has(externalId)) {
    return fileInfoByExternalId.get(externalId) || null;
  }
  if (fileName) {
    const match = localFileInfos.find((info) => info.name === fileName);
    return match || null;
  }
  return null;
}

export function getLocalFilePreview(fileId: number | null, _page = 1) {
  if (!fileId) return null;
  const info = fileInfoById.get(fileId);
  if (!info?.previewUrl) return null;
  return { url: info.previewUrl, type: "image" as const };
}

export function getLocalFilePageCount(fileId: number | null) {
  if (!fileId) return 1;
  return fileInfoById.get(fileId)?.pageCount || 1;
}

export const localAnnotationStatesByPipeline: Record<string, AnnotationState[]> = {
  [demoPipelineId]: [
    {
      externalId: "state-001",
      space: "file_annotation",
      createdTime: new Date("2025-02-18T08:10:00Z"),
      lastUpdatedTime: new Date("2025-02-18T08:25:00Z"),
      linkedFile: { externalId: "FILE-0001", space: "file_annotation" },
      annotationStatus: "Annotated",
      pageCount: 12,
      annotatedPageCount: 11,
      launchFunctionId: 3035717719542834,
      launchFunctionCallId: 1411406210929260,
      finalizeFunctionId: 3035717719542834,
      finalizeFunctionCallId: 1411406210929260,
      fileName: "PID-001.pdf",
      fileSourceId: "PID-001",
      fileMimeType: "application/pdf",
      fileResourceType: "P&ID",
      fileSecondaryScope: "North",
    },
    {
      externalId: "state-002",
      space: "file_annotation",
      createdTime: new Date("2025-02-18T09:05:00Z"),
      lastUpdatedTime: new Date("2025-02-18T09:18:00Z"),
      linkedFile: { externalId: "FILE-0002", space: "file_annotation" },
      annotationStatus: "Awaiting",
      pageCount: 8,
      annotatedPageCount: 0,
      fileName: "PID-002.pdf",
      fileSourceId: "PID-002",
      fileMimeType: "application/pdf",
      fileResourceType: "P&ID",
      fileSecondaryScope: "South",
    },
    {
      externalId: "state-003",
      space: "file_annotation",
      createdTime: new Date("2025-02-17T15:35:00Z"),
      lastUpdatedTime: new Date("2025-02-17T16:02:00Z"),
      linkedFile: { externalId: "FILE-0003", space: "file_annotation" },
      annotationStatus: "Failed",
      pageCount: 5,
      annotatedPageCount: 1,
      launchFunctionId: 3035717719542834,
      launchFunctionCallId: 1411406210929261,
      fileName: "DATA-SHEET-003.pdf",
      fileSourceId: "DS-003",
      fileMimeType: "application/pdf",
      fileResourceType: "Datasheet",
      fileSecondaryScope: "East",
    },
  ],
};

export const localAnnotationsByPipeline: Record<
  string,
  { actual: AnnotationRecord[]; potential: AnnotationRecord[] }
> = {
  [demoPipelineId]: {
    actual: [
      {
        externalId: "ann-001",
        startNode: "file_annotation:FILE-0001",
        startNodeText: "VALVE-100",
        endNode: "Asset:VALVE-100",
        endNodeResourceType: "Asset",
        status: AnnotationStatus.APPROVED,
        tags: ["PromotedManually"],
        fileExternalId: "FILE-0001",
        fileName: "PID-001.pdf",
        fileResourceType: "P&ID",
        fileSecondaryScope: "North",
        page: 1,
        confidence: 0.98,
        boundingBox: { xMin: 230, yMin: 260, xMax: 520, yMax: 360 },
      },
      {
        externalId: "ann-002",
        startNode: "file_annotation:FILE-0002",
        startNodeText: "PUMP-210",
        endNode: "Asset:PUMP-210",
        endNodeResourceType: "Asset",
        status: AnnotationStatus.APPROVED,
        tags: ["PromotedAuto"],
        fileExternalId: "FILE-0002",
        fileName: "PID-002.pdf",
        fileResourceType: "P&ID",
        fileSecondaryScope: "South",
        page: 1,
        confidence: 0.93,
        boundingBox: { xMin: 660, yMin: 340, xMax: 990, yMax: 460 },
      },
    ],
    potential: [
      {
        externalId: "ann-003",
        startNode: "file_annotation:FILE-0001",
        startNodeText: "LINE-120A",
        endNode: "Asset:LINE-120A",
        endNodeResourceType: "Asset",
        status: AnnotationStatus.SUGGESTED,
        tags: ["PatternFound"],
        fileExternalId: "FILE-0001",
        fileName: "PID-001.pdf",
        fileResourceType: "P&ID",
        fileSecondaryScope: "North",
        page: 1,
        confidence: 0.74,
        boundingBox: { xMin: 320, yMin: 540, xMax: 700, yMax: 610 },
      },
      {
        externalId: "ann-004",
        startNode: "file_annotation:FILE-0003",
        startNodeText: "TEMP-3A",
        endNode: "Asset:TEMP-3A",
        endNodeResourceType: "Asset",
        status: AnnotationStatus.SUGGESTED,
        tags: ["PatternFound"],
        fileExternalId: "FILE-0003",
        fileName: "DATA-SHEET-003.pdf",
        fileResourceType: "Datasheet",
        fileSecondaryScope: "East",
        page: 1,
        confidence: 0.66,
        boundingBox: { xMin: 720, yMin: 520, xMax: 980, yMax: 600 },
      },
    ],
  },
};

const manualPatternsByPipeline: Record<string, PatternRecord[]> = {
  [demoPipelineId]: [
    {
      sample: "VALVE-100",
      resourceType: "Valve",
      annotationType: "Asset",
      patternScope: "piping",
      createdBy: "manual",
    },
    {
      sample: "PUMP-210",
      resourceType: "Pump",
      annotationType: "Asset",
      patternScope: "piping",
      createdBy: "manual",
    },
  ],
};

const automaticPatternsByPipeline: Record<string, PatternRecord[]> = {
  [demoPipelineId]: [
    {
      sample: "LINE-120A",
      resourceType: "Pipeline",
      annotationType: "Asset",
      patternScope: "piping",
      entityType: "Asset",
      createdBy: "automatic",
    },
    {
      sample: "TEMP-3A",
      resourceType: "Instrument",
      annotationType: "Asset",
      patternScope: "instrumentation",
      entityType: "Asset",
      createdBy: "automatic",
    },
  ],
};

export function getLocalManualPatterns(pipelineId: string) {
  return manualPatternsByPipeline[pipelineId] || [];
}

export function setLocalManualPatterns(pipelineId: string, patterns: PatternRecord[]) {
  manualPatternsByPipeline[pipelineId] = patterns;
}

export function getLocalAutomaticPatterns(pipelineId: string) {
  return automaticPatternsByPipeline[pipelineId] || [];
}

export const localRunsByPipeline: Record<string, PipelineRun[]> = {
  [demoPipelineId]: [
    {
      id: "run-001",
      status: "success",
      message:
        "(caller:Launch, function_id:3035717719542834, call_id:1411406210929260) - total files processed: 12 - successful files: 11 - failed files: 1",
      createdTime: Date.parse("2025-02-18T08:25:00Z"),
      caller: CallerType.LAUNCH,
      functionId: "3035717719542834",
      callId: "1411406210929260",
      total: 12,
      success: 11,
      failed: 1,
    },
    {
      id: "run-002",
      status: "success",
      message:
        "(caller:Finalize, function_id:3035717719542834, call_id:1411406210929260) - total files processed: 11 - successful files: 11 - failed files: 0",
      createdTime: Date.parse("2025-02-18T08:28:00Z"),
      caller: CallerType.FINALIZE,
      functionId: "3035717719542834",
      callId: "1411406210929260",
      total: 11,
      success: 11,
      failed: 0,
    },
    {
      id: "run-003",
      status: "failure",
      message:
        "(caller:Prepare, function_id:3035717719542834, call_id:1411406210929261) - total files processed: 4 - successful files: 1 - failed files: 3",
      createdTime: Date.parse("2025-02-17T16:00:00Z"),
      caller: CallerType.PREPARE,
      functionId: "3035717719542834",
      callId: "1411406210929261",
      total: 4,
      success: 1,
      failed: 3,
    },
  ],
};

const localFunctionLogsByKey: Record<string, string> = {
  "3035717719542834:1411406210929260":
    "[08:25:02] Start processing\n[08:25:09] Processing file NodeId(space='file_annotation', external_id='FILE-0001')\n[08:25:18] Processing file NodeId(space='file_annotation', external_id='FILE-0002')\n[08:25:30] Completed\n",
  "3035717719542834:1411406210929261":
    "[16:00:02] Start processing\n[16:00:19] Processing file NodeId(space='file_annotation', external_id='FILE-0003')\n[16:00:25] Error: OCR timeout\n",
};

export function getLocalFunctionLogs(functionId: string | number, callId: string | number) {
  return localFunctionLogsByKey[`${functionId}:${callId}`] || "";
}
