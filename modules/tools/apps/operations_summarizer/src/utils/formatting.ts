/**
 * Barrel re-exports for formatting utilities.
 * Prefer importing from focused modules (markdown-format, dates, etc.) in new code.
 */

export {
	extractDateRangeFromContent,
	formatDateDisplay,
	formatDateRangeDisplay,
	formatDateShort,
	formatDateTimeShort,
	formatIsoDate,
	formatTimestamp,
} from "./dates";
export { generateFilename } from "./filenames";

export { markdownToHtml, stripHtmlTags } from "./markdown-format";

export { paginateItems } from "./pagination";
export {
	extractDataCounts,
	extractSimilarityScore,
	normalizeSummaryTextForComparison,
} from "./scores";
