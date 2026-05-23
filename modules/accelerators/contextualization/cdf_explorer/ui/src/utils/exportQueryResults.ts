import type { GridRow } from "../types/explorerNodes";

export type QueryExportFormat = "json" | "yaml" | "csv" | "excel" | "parquet";

export const QUERY_EXPORT_FORMATS: QueryExportFormat[] = [
  "json",
  "yaml",
  "csv",
  "excel",
  "parquet",
];

const MIME: Record<QueryExportFormat, string> = {
  json: "application/json",
  yaml: "application/x-yaml",
  csv: "text/csv;charset=utf-8",
  excel: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  parquet: "application/octet-stream",
};

const EXT: Record<QueryExportFormat, string> = {
  json: "json",
  yaml: "yaml",
  csv: "csv",
  excel: "xlsx",
  parquet: "parquet",
};

type ParquetBasicType = "BOOLEAN" | "INT32" | "INT64" | "DOUBLE" | "STRING";

function slugifyExportBaseName(label: string): string {
  const slug = label
    .trim()
    .replace(/[^\w\-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 60);
  return slug || "query_results";
}

export function exportFilename(label: string, format: QueryExportFormat): string {
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  return `${slugifyExportBaseName(label)}_${ts}.${EXT[format]}`;
}

function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  try {
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.rel = "noopener";
    anchor.click();
  } finally {
    URL.revokeObjectURL(url);
  }
}

function orderedRows(columns: string[], items: GridRow[]): GridRow[] {
  return items.map((row) => {
    const out: GridRow = {};
    for (const col of columns) {
      out[col] = row[col] ?? null;
    }
    return out;
  });
}

function csvEscape(value: string): string {
  if (/[",\r\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function cellToScalarString(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function toCsv(columns: string[], items: GridRow[]): string {
  const header = columns.map(csvEscape).join(",");
  const lines = items.map((row) =>
    columns.map((col) => csvEscape(cellToScalarString(row[col]))).join(",")
  );
  return [header, ...lines].join("\r\n");
}

function toJson(columns: string[], items: GridRow[]): string {
  return JSON.stringify(orderedRows(columns, items), null, 2);
}

async function toYaml(columns: string[], items: GridRow[]): Promise<string> {
  const { dump } = await import("js-yaml");
  return dump(orderedRows(columns, items), { lineWidth: 120, noRefs: true });
}

async function toExcel(columns: string[], items: GridRow[]): Promise<ArrayBuffer> {
  const XLSX = await import("xlsx");
  const rows = orderedRows(columns, items);
  const sheet = XLSX.utils.json_to_sheet(rows, { header: columns });
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, sheet, "Results");
  return XLSX.write(workbook, { bookType: "xlsx", type: "array" }) as ArrayBuffer;
}

function inferParquetType(values: unknown[]): ParquetBasicType {
  const nonNull = values.filter((v) => v != null);
  if (nonNull.length === 0) return "STRING";
  if (nonNull.every((v) => typeof v === "boolean")) return "BOOLEAN";
  if (
    nonNull.every(
      (v) => typeof v === "number" && Number.isInteger(v) && Math.abs(v) <= 2147483647
    )
  ) {
    return "INT32";
  }
  if (nonNull.every((v) => typeof v === "number" && Number.isInteger(v))) return "INT64";
  if (nonNull.every((v) => typeof v === "number")) return "DOUBLE";
  if (nonNull.every((v) => typeof v === "string")) return "STRING";
  return "STRING";
}

function toParquetCell(value: unknown, type: ParquetBasicType): unknown {
  if (value == null) return null;
  if (type === "STRING") {
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
    return String(value);
  }
  if (type === "BOOLEAN") return Boolean(value);
  if (type === "INT32" || type === "INT64" || type === "DOUBLE") return Number(value);
  return String(value);
}

async function toParquet(columns: string[], items: GridRow[]): Promise<ArrayBuffer> {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const columnData = columns.map((name) => {
    const rawValues = items.map((row) => row[name]);
    const type = inferParquetType(rawValues);
    return {
      name,
      type,
      nullable: true,
      data: rawValues.map((v) => toParquetCell(v, type)),
    };
  });
  return parquetWriteBuffer({ columnData });
}

export async function exportQueryResults(
  format: QueryExportFormat,
  columns: string[],
  items: GridRow[],
  label: string
): Promise<void> {
  if (columns.length === 0 || items.length === 0) {
    throw new Error("No rows to export");
  }

  const filename = exportFilename(label, format);
  switch (format) {
    case "json": {
      downloadBlob(new Blob([toJson(columns, items)], { type: MIME.json }), filename);
      return;
    }
    case "yaml": {
      downloadBlob(new Blob([await toYaml(columns, items)], { type: MIME.yaml }), filename);
      return;
    }
    case "csv": {
      downloadBlob(new Blob([toCsv(columns, items)], { type: MIME.csv }), filename);
      return;
    }
    case "excel": {
      downloadBlob(new Blob([await toExcel(columns, items)], { type: MIME.excel }), filename);
      return;
    }
    case "parquet": {
      downloadBlob(new Blob([await toParquet(columns, items)], { type: MIME.parquet }), filename);
      return;
    }
    default: {
      const _exhaustive: never = format;
      throw new Error(`Unsupported export format: ${_exhaustive}`);
    }
  }
}
