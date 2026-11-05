import { getOperationSummaryExternalIdPrefix } from "@/utils/summaryListQuery";

export function parseDatesFromExternalId(
  externalId: string,
): { start: string; end: string } | null {
  const prefix = escapeRegExp(getOperationSummaryExternalIdPrefix());
  const match = externalId.match(
    new RegExp(`${prefix}(\\d{4}-\\d{2}-\\d{2})_(\\d{4}-\\d{2}-\\d{2})`),
  );
  if (match) return { start: match[1], end: match[2] };
  return null;
}

export function formatDateForInput(date: Date): string {
  return date.toISOString().split("T")[0];
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
