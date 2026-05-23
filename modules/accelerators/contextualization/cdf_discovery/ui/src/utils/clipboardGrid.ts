import type { GridRow } from "../types/discoveryNodes";
import { formatGridCell } from "./gridFormat";

function csvEscape(value: string): string {
  if (/[",\r\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function gridRowsToTsv(columns: string[], rows: GridRow[]): string {
  const header = columns.map(csvEscape).join("\t");
  const lines = rows.map((row) =>
    columns.map((col) => csvEscape(formatGridCell(row[col]))).join("\t")
  );
  return [header, ...lines].join("\r\n");
}

export async function copyTextToClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    document.execCommand("copy");
  } finally {
    document.body.removeChild(textarea);
  }
}
