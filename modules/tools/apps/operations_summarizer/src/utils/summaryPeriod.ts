/**
 * Summary period helpers: the list filters and displays the period a summary covers,
 * not the day it was generated.
 */

import { endOfDay, isValid, parse, startOfDay } from "date-fns";
import type { SummaryInstance } from "@/types/operations";

export interface SummaryPeriod {
	start: Date;
	end: Date;
}

/** Saved summaries and test reports embed the period as `<start>_<end>` in the external id. */
const PERIOD_IN_EXTERNAL_ID_RE = /(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/;

/** Parse YYYY-MM-DD as local midnight (not UTC, unlike `new Date("YYYY-MM-DD")`). */
export function parseLocalCalendarDate(ymd: string): Date | null {
	const date = parse(ymd, "yyyy-MM-dd", new Date());
	return isValid(date) ? date : null;
}

/**
 * Period a summary covers. The list query only loads name/description/mimeType, so the
 * period comes from the external id; rows without one fall back to the created day.
 */
export function getSummaryPeriod(summary: SummaryInstance): SummaryPeriod {
	const match = summary.externalId.match(PERIOD_IN_EXTERNAL_ID_RE);
	const start = match ? parseLocalCalendarDate(match[1]) : null;
	const end = match ? parseLocalCalendarDate(match[2]) : null;
	if (start && end) return { start, end };

	const created = startOfDay(new Date(summary.createdTime));
	return { start: created, end: created };
}

/** True when the period and the range share at least one local day (both ends inclusive). */
export function periodOverlaps(
	period: SummaryPeriod,
	range: SummaryPeriod
): boolean {
	return (
		startOfDay(period.start) <= endOfDay(range.end) &&
		endOfDay(period.end) >= startOfDay(range.start)
	);
}
