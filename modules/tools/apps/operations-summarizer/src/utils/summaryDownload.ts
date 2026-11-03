/**
 * Summary download utilities.
 * Provides functions for downloading summaries as Markdown and PDF.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getSummarySpace } from "@/config/dataTypes";
import type { SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import {
  getOperationSummaryExternalIdPrefix,
  getTestReportExternalIdPrefix,
} from "@/utils/summaryListQuery";
import {
  formatDateRangeDisplay,
  formatDateTimeShort,
  markdownToHtml,
} from "./formatting";
import { extractFileProperties } from "./nodeProperties";
import { stripSummaryContentTypeBitmapLine } from "./summaryListQuery";

/**
 * Generate human-readable display name from system identifier.
 * Handles legacy reports with technical names like "ops_summary_2024-01-01_2024-01-31_..."
 */
function generateDisplayNameFromId(identifier: string): string {
  const match = identifier.match(
    /^(ops_summary|test_report)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_/,
  );
  if (!match) return identifier;
  const [, type, startDateStr, endDateStr] = match;
  const startDate = new Date(startDateStr);
  const endDate = new Date(endDateStr);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return identifier;
  }
  const label = type === "ops_summary" ? "Operations summary" : "Test report";
  return `${label}, ${formatDateRangeDisplay(startDate, endDate)}`;
}

/**
 * Extract content from a summary instance.
 * Uses shared extractFileProperties for consistent property extraction.
 */
function normalizeSummaryName(
  summary: SummaryInstance,
  rawName?: string,
): string {
  let name = rawName || summary.externalId || "Unnamed Summary";

  const looksLikeSystemId =
    name.startsWith(getOperationSummaryExternalIdPrefix()) ||
    name.startsWith(getTestReportExternalIdPrefix());

  if (looksLikeSystemId) {
    name = generateDisplayNameFromId(name);
  }

  return name.endsWith(".pdf") ? name.slice(0, -4) : name;
}

export function extractSummaryContent(summary: SummaryInstance): {
  name: string;
  content: string;
  description: string;
  mimeType: string;
} {
  const fileProps = extractFileProperties(
    summary,
    VIEW_IDS.SUMMARY_OUTPUT_FILE,
  );

  let content = (fileProps?.content as string) || "";

  let description = stripSummaryContentTypeBitmapLine(
    (fileProps?.description as string) || "",
  );

  // If description carries the markdown payload (e.g. test reports store their full
  // report in description; sourceContext holds only JSON metrics in that format),
  // prefer it before falling back to sourceContext.
  if (
    !content &&
    description &&
    (description.startsWith("#") ||
      description.includes("\n##") ||
      description.includes("**"))
  ) {
    content = description;
    description = "";
  }

  // Fall back to sourceContext only when description didn't yield content
  // (e.g. operation summary PDFs that store their markdown text in sourceContext).
  if (!content) {
    content =
      (fileProps?.sourceContext as string) ||
      (fileProps?.source as string) ||
      "";
  }

  return {
    name: normalizeSummaryName(summary, fileProps?.name as string | undefined),
    content,
    description,
    mimeType: (fileProps?.mimeType as string) || "",
  };
}

/**
 * Download content as a Markdown file
 */
export function downloadAsMarkdown(content: string, filename: string): void {
  const blob = new Blob([content], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);

  const link = document.createElement("a");
  link.href = url;
  link.download = filename.endsWith(".md") ? filename : `${filename}.md`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);

  URL.revokeObjectURL(url);
}

/**
 * Generate a simple PDF from Markdown content using browser print
 */
function downloadAsPdf(markdown: string, filename: string): void {
  const html = markdownToHtml(markdown, { printFriendly: true });

  const printWindow = window.open("", "_blank");
  if (!printWindow) {
    console.error("Failed to open print window");
    return;
  }

  let printed = false;
  const doPrint = () => {
    if (printed) return;
    printed = true;
    printWindow.print();
    printWindow.onafterprint = () => printWindow.close();
  };

  printWindow.onload = doPrint;

  printWindow.document.write(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>${escapeHtml(filename)}</title>
      <style>
        body {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
          max-width: 800px;
          margin: 0 auto;
          padding: 40px;
          line-height: 1.6;
          color: #333;
        }
        h1, h2, h3 { color: #1a1a2e; margin-top: 1.5em; }
        h1 { font-size: 24px; border-bottom: 2px solid #4a90a4; padding-bottom: 10px; }
        h2 { font-size: 20px; }
        h3 { font-size: 16px; }
        table { border-collapse: collapse; width: 100%; margin: 1em 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #f4f4f4; }
        li { margin: 0.5em 0; }
        code { background: #f4f4f4; padding: 2px 4px; border-radius: 2px; }
        pre { background: #f4f4f4; padding: 15px; border-radius: 4px; overflow-x: auto; }
        @media print {
          body { max-width: none; padding: 20px; }
        }
      </style>
    </head>
    <body>
      ${html}
    </body>
    </html>
  `);
  printWindow.document.close();

  // Fallback: if onload does not fire (e.g. document.write in some browsers), print after a short delay
  setTimeout(doPrint, 500);
}

/** Escape for safe use in HTML attribute/text to avoid breaking the document. */
function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function getSummaryDownloadPayload(
  summary: SummaryInstance,
  contentOverride?: string,
) {
  const { name, content: extractedContent } = extractSummaryContent(summary);
  return {
    filename: name.replace(/\.[^/.]+$/, ""),
    content: contentOverride || extractedContent,
  };
}

export function downloadSummaryAsMarkdown(
  summary: SummaryInstance,
  contentOverride?: string,
): void {
  const { filename, content } = getSummaryDownloadPayload(
    summary,
    contentOverride,
  );
  downloadAsMarkdown(content, filename);
}

export function downloadSummaryAsPdf(
  summary: SummaryInstance,
  contentOverride?: string,
): void {
  const { filename, content } = getSummaryDownloadPayload(
    summary,
    contentOverride,
  );
  downloadAsPdf(content, filename);
}

/**
 * Get a formatted display date from a summary
 */
export function getSummaryDisplayDate(summary: SummaryInstance): string {
  const createdTime =
    typeof summary.createdTime === "number"
      ? new Date(summary.createdTime)
      : new Date();
  return formatDateTimeShort(createdTime);
}

/**
 * Get a truncated preview of summary content
 */
/**
 * Prefer signed download URL for PDF file nodes; otherwise invoke fallback (e.g. robust download).
 */
export async function downloadOperationSummaryPdfWithSignedUrlFirst(
  client: CogniteClient,
  summary: SummaryInstance,
  options: { fallback: (s: SummaryInstance) => Promise<void> },
): Promise<void> {
  const { name, mimeType } = extractSummaryContent(summary);
  const isActualPdf =
    name?.toLowerCase().endsWith(".pdf") || mimeType === "application/pdf";

  if (isActualPdf) {
    try {
      const space = summary.space || getSummarySpace();
      const [result] = await withCdfConcurrency(() =>
        client.files.getDownloadUrls([
          { instanceId: { space, externalId: summary.externalId } },
        ]),
      );

      if (result?.downloadUrl) {
        const link = document.createElement("a");
        link.href = result.downloadUrl;
        link.download = name || `${summary.externalId}.pdf`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        return;
      }
    } catch (error) {
      console.error("Error downloading PDF via direct URL:", error);
    }
  }

  await options.fallback(summary);
}

export function getSummaryPreview(
  summary: SummaryInstance,
  maxLength = 200,
): string {
  const { content } = extractSummaryContent(summary);

  // Remove markdown formatting for preview
  let preview = content
    .replace(/^#{1,6}\s+/gm, "") // Remove headers
    .replace(/\*\*|__|`/g, "") // Remove bold/code markers
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1") // Convert links to text
    .replace(/\n+/g, " ") // Convert newlines to spaces
    .trim();

  if (preview.length > maxLength) {
    preview = `${preview.substring(0, maxLength).trim()}...`;
  }

  return preview;
}
