import type { FileContentFormat, FileContentRef } from "../types/explorerNodes";

type Row = Record<string, unknown>;

function field(row: Row, keys: string[]): unknown {
  for (const key of keys) {
    if (key in row && row[key] != null && row[key] !== "") {
      return row[key];
    }
  }
  return undefined;
}

function asString(value: unknown): string {
  if (value == null) return "";
  return String(value).trim();
}

function asBoolean(value: unknown): boolean | null {
  if (value == null || value === "") return null;
  if (typeof value === "boolean") return value;
  const s = String(value).trim().toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  return null;
}

function asFileId(value: unknown): number | undefined {
  if (value == null || value === "") return undefined;
  const n = typeof value === "number" ? value : Number(String(value).trim());
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : undefined;
}

export function detectFileContentFormat(row: Row): FileContentFormat | null {
  const mime = asString(field(row, ["mimeType", "mime_type", "mimetype"])).toLowerCase();
  const name = asString(field(row, ["name", "fileName", "filename"])).toLowerCase();

  if (mime.includes("parquet") || name.endsWith(".parquet")) {
    return "parquet";
  }
  if (
    mime === "text/csv" ||
    mime === "application/csv" ||
    mime === "text/comma-separated-values" ||
    name.endsWith(".csv")
  ) {
    return "csv";
  }
  if (
    mime.includes("json") ||
    mime.includes("ndjson") ||
    name.endsWith(".json") ||
    name.endsWith(".jsonl") ||
    name.endsWith(".ndjson")
  ) {
    return "json";
  }
  return null;
}

export function isQueryableFileRow(row: Row | null | undefined): row is Row {
  if (!row) return false;
  const format = detectFileContentFormat(row);
  if (!format) return false;
  const uploaded = asBoolean(field(row, ["isUploaded", "is_uploaded", "uploaded"]));
  if (uploaded === false) return false;
  const fileId = asFileId(field(row, ["id", "fileId", "file_id"]));
  const externalId = asString(field(row, ["externalId", "external_id"]));
  return fileId != null || externalId.length > 0;
}

export function fileContentRefFromRow(row: Row): FileContentRef | null {
  if (!isQueryableFileRow(row)) return null;
  const format = detectFileContentFormat(row);
  if (!format) return null;

  const file_id = asFileId(field(row, ["id", "fileId", "file_id"]));
  const external_id = asString(field(row, ["externalId", "external_id"]));
  const name = asString(field(row, ["name", "fileName", "filename"]));

  return {
    ...(file_id != null ? { file_id } : {}),
    ...(external_id ? { external_id } : {}),
    ...(name ? { name } : {}),
    format,
  };
}

export function fileContentTabLabel(ref: FileContentRef): string {
  const base = ref.name?.trim() || ref.external_id?.trim() || (ref.file_id != null ? String(ref.file_id) : "File");
  const suffix = ref.format.toUpperCase();
  return `${base} (${suffix})`;
}
