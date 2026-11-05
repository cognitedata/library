/**
 * Barrel re-exports for formatting utilities.
 * Prefer importing from focused modules (sanitize, markdown-format, dates, etc.) in new code.
 */

export type { DateRange } from "./dates";
export {
  calendarIsoDateSpanDays,
  extractDateRangeFromContent,
  formatDateDisplay,
  formatDateRangeDisplay,
  formatDateShort,
  formatDateTimeShort,
  formatIsoDate,
  formatTimestamp,
  isCalendarIsoDateString,
  parseCalendarIsoToUtcDate,
  toCalendarIsoDate,
} from "./dates";
export { generateFilename } from "./filenames";
export type { MarkdownToHtmlOptions } from "./markdown-format";
export { markdownToHtml, stripHtmlTags } from "./markdown-format";
export type { PaginationResult } from "./pagination";
export { paginateItems } from "./pagination";
export { sanitizeHtml } from "./sanitize";
export {
  calculateDataAccuracyScore,
  extractDataCounts,
  extractSimilarityScore,
  getScoreCategory,
  getScoreDisplay,
  normalizeSummaryTextForComparison,
} from "./scores";
