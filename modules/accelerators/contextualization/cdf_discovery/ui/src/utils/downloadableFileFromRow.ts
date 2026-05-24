type Row = Record<string, unknown>;

export type DownloadableFileRef = {
  file_id?: number;
  external_id?: string;
  instance_space?: string;
  name?: string;
};

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

function asByteSize(value: unknown): number | undefined {
  if (value == null || value === "") return undefined;
  const n = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isFinite(n) || n < 0) return undefined;
  return Math.trunc(n);
}

export function isDownloadableFileRow(row: Row | null | undefined): row is Row {
  if (!row) return false;
  const uploaded = asBoolean(field(row, ["isUploaded", "is_uploaded", "uploaded"]));
  if (uploaded === false) return false;
  const fileId = asFileId(field(row, ["id", "fileId", "file_id"]));
  const externalId = asString(field(row, ["externalId", "external_id"]));
  return fileId != null || externalId.length > 0;
}

export function downloadableFileRefFromRow(row: Row): DownloadableFileRef | null {
  if (!isDownloadableFileRow(row)) return null;
  const instance_space = asString(field(row, ["space", "instanceSpace", "instance_space"]));
  const external_id = asString(field(row, ["externalId", "external_id"]));
  const file_id = asFileId(field(row, ["id", "fileId", "file_id"]));
  const name = asString(field(row, ["name", "fileName", "filename"]));

  if (instance_space && external_id) {
    return {
      instance_space,
      external_id,
      ...(name ? { name } : {}),
    };
  }

  return {
    ...(file_id != null ? { file_id } : {}),
    ...(external_id ? { external_id } : {}),
    ...(name ? { name } : {}),
  };
}

export function fileSizeFromRow(row: Row): number | undefined {
  return asByteSize(field(row, ["size", "fileSize", "file_size", "contentLength", "content_length"]));
}
