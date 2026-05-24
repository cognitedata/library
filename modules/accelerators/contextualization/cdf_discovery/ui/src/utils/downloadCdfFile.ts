import type { MessageKey } from "../i18n/types";
import { headFileDownload, downloadFileBlob } from "../api";
import {
  downloadableFileRefFromRow,
  fileSizeFromRow,
} from "./downloadableFileFromRow";

export const LARGE_FILE_THRESHOLD_BYTES = 10 * 1024 * 1024;

type Translator = (key: MessageKey, vars?: Record<string, string | number>) => string;

function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  try {
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.rel = "noopener";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  } finally {
    URL.revokeObjectURL(url);
  }
}

export function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"] as const;
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const digits = unitIndex === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${units[unitIndex]}`;
}

async function confirmDownload(
  t: Translator,
  sizeBytes: number | undefined
): Promise<boolean> {
  if (sizeBytes != null && sizeBytes > LARGE_FILE_THRESHOLD_BYTES) {
    return window.confirm(
      t("sql.downloadFileConfirmLarge", { size: formatFileSize(sizeBytes) })
    );
  }
  if (sizeBytes == null) {
    return window.confirm(t("sql.downloadFileConfirmUnknown"));
  }
  return true;
}

export async function downloadCdfFileWithConfirm(
  row: Record<string, unknown>,
  t: Translator,
  opts?: { signal?: AbortSignal }
): Promise<void> {
  const ref = downloadableFileRefFromRow(row);
  if (!ref) {
    throw new Error(t("sql.downloadFileDisabled"));
  }

  let sizeBytes = fileSizeFromRow(row);
  let filename = ref.name?.trim() || ref.external_id?.trim() || "download";

  if (sizeBytes == null) {
    const head = await headFileDownload(ref, { signal: opts?.signal });
    sizeBytes = head.sizeBytes;
    filename = head.filename || filename;
  }

  const proceed = await confirmDownload(t, sizeBytes);
  if (!proceed) return;

  const blob = await downloadFileBlob(ref, { signal: opts?.signal });
  downloadBlob(blob, filename);
}
