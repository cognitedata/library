import type { AnnotationRecord } from "@/shared/utils/types";

export interface FileInfo {
  fileExternalId: string;
  fileName?: string;
}

export function buildFileInfoByExternalId(records: AnnotationRecord[]) {
  const map = new Map<string, FileInfo>();

  for (const record of records) {
    if (!record.fileExternalId) continue;

    if (!map.has(record.fileExternalId)) {
      map.set(record.fileExternalId, {
        fileExternalId: record.fileExternalId,
        fileName: record.fileName,
      });
      continue;
    }

    const existing = map.get(record.fileExternalId);
    if (existing && !existing.fileName && record.fileName) {
      map.set(record.fileExternalId, {
        fileExternalId: record.fileExternalId,
        fileName: record.fileName,
      });
    }
  }

  return map;
}

export function resolveFileInfo(
  fileInfoByExternalId: Map<string, FileInfo>,
  fileExternalId: string
): FileInfo {
  return (
    fileInfoByExternalId.get(fileExternalId) ?? {
      fileExternalId,
      fileName: undefined,
    }
  );
}
