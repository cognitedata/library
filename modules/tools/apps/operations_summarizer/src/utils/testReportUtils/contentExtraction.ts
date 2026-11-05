/**
 * Period, data type, and volume extraction from test report content.
 */

import { getTestReportExternalIdPrefix } from "@/utils/summaryListQuery";

/**
 * Extract total data volume from test report content.
 */
export function extractDataVolumeFromContent(content: string): number {
  const totalDataMatch = content.match(/Total\s*Data:\s*(\d+)/i);
  if (totalDataMatch) return Number.parseInt(totalDataMatch[1], 10);

  const patterns = [
    /\*\*?Assets?\*\*?:?\s*(\d+)/gi,
    /\*\*?Files?\*\*?:?\s*(\d+)/gi,
    /\*\*?Equipment\*\*?:?\s*(\d+)/gi,
    /\*\*?Time[_\s]?Series\*\*?:?\s*(\d+)/gi,
    /\*\*?Work[_\s]?Orders?\*\*?:?\s*(\d+)/gi,
    /(\d+)\s*work\s*orders?/gi,
    /(\d+)\s*assets?/gi,
    /(\d+)\s*files?/gi,
    /(\d+)\s*time\s*series/gi,
    /(\d+)\s*equipment/gi,
  ];

  const foundValues = new Set<number>();

  for (const pattern of patterns) {
    const matches = content.matchAll(pattern);
    for (const match of matches) {
      foundValues.add(Number.parseInt(match[1], 10));
    }
  }

  let total = 0;
  for (const value of foundValues) {
    total += value;
  }
  return total;
}

/**
 * Extract test period from content or external ID.
 */
export function extractPeriodFromContent(content: string): string {
  const dateRangeMatch = content.match(
    /date\s*range\s*\**:?\**\s*(\d{4}-\d{2}-\d{2})\s*(?:to|—|-)\s*(\d{4}-\d{2}-\d{2})/i,
  );
  if (dateRangeMatch) return `${dateRangeMatch[1]} — ${dateRangeMatch[2]}`;

  const prefix = getTestReportExternalIdPrefix().replace(
    /[.*+?^${}()|[\]\\]/g,
    "\\$&",
  );
  const idMatch = content.match(
    new RegExp(`${prefix}(\\d{4}-\\d{2}-\\d{2})_(\\d{4}-\\d{2}-\\d{2})`, "i"),
  );
  if (idMatch) return `${idMatch[1]} — ${idMatch[2]}`;

  return "";
}

/**
 * Extract mentioned data types from content.
 */
export function extractDataTypesFromContent(content: string): string[] {
  const dataTypes: string[] = [];

  if (/assets?\s*[:(]/i.test(content) || /\d+\s*assets?/i.test(content)) {
    dataTypes.push("Assets");
  }
  if (/files?\s*[:(]/i.test(content) || /\d+\s*files?/i.test(content)) {
    dataTypes.push("Files");
  }
  if (/equipment\s*[:(]/i.test(content) || /\d+\s*equipment/i.test(content)) {
    dataTypes.push("Equipment");
  }
  if (
    /time\s*series\s*[:(]/i.test(content) ||
    /\d+\s*time\s*series/i.test(content)
  ) {
    dataTypes.push("Time Series");
  }
  if (
    /work\s*orders?\s*[:(]/i.test(content) ||
    /\d+\s*work\s*orders?/i.test(content)
  ) {
    dataTypes.push("Work Orders");
  }

  return dataTypes;
}
