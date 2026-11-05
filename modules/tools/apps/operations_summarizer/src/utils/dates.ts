/**
 * Date formatting and extraction for Operations Summarizer.
 */

import {
  differenceInCalendarDays,
  format,
  isSameDay,
  isSameMonth,
  isSameYear,
  isValid,
  parseISO,
} from "date-fns";

export interface DateRange {
  start: Date;
  end: Date;
}

const CALENDAR_ISO_RE = /^\d{4}-\d{2}-\d{2}$/;

/** Calendar day as YYYY-MM-DD from a Date (UTC date part; matches HTML date inputs). */
export function toCalendarIsoDate(date: Date): string {
  return format(date, "yyyy-MM-dd");
}

/**
 * Parse a calendar YYYY-MM-DD string to UTC midnight for deterministic range math
 * and CDF queries (matches previous Date("YYYY-MM-DD") UTC semantics).
 */
export function parseCalendarIsoToUtcDate(isoYmd: string): Date {
  if (!CALENDAR_ISO_RE.test(isoYmd)) return new Date(Number.NaN);

  const date = parseISO(`${isoYmd}T00:00:00.000Z`);
  return isValid(date) && date.toISOString().startsWith(`${isoYmd}T`)
    ? date
    : new Date(Number.NaN);
}

/** True when `value` is a non-empty YYYY-MM-DD that parses to a valid calendar day. */
export function isCalendarIsoDateString(value: string): boolean {
  return isValid(parseCalendarIsoToUtcDate(value));
}

/** Whole-day span between two calendar ISO strings (inclusive-oriented; non-negative). */
export function calendarIsoDateSpanDays(
  startIso: string,
  endIso: string,
): number {
  const start = parseCalendarIsoToUtcDate(startIso);
  const end = parseCalendarIsoToUtcDate(endIso);

  if (!isValid(start) || !isValid(end)) return 0;
  return Math.max(0, differenceInCalendarDays(end, start));
}

/**
 * Format ISO date string to YYYY-MM-DD
 */
export function formatIsoDate(dateStr: string | null | undefined): string {
  if (!dateStr) return "None";
  const date = parseISO(dateStr);
  return isValid(date) ? format(date, "yyyy-MM-dd") : "Invalid Date";
}

/**
 * Format date to human-readable string (long month name: "January 15, 2024").
 * Accepts a Date or a calendar YYYY-MM-DD string (local calendar day for strings).
 */
export function formatDateDisplay(date: Date | string): string {
  const value = typeof date === "string" ? parseISO(date) : date;
  return isValid(value) ? format(value, "MMMM d, yyyy") : "Invalid date";
}

/**
 * Format date with abbreviated month name ("Jan 15, 2024")
 */
export function formatDateShort(date: Date): string {
  return format(date, "MMM d, yyyy");
}

/**
 * Format date and time with abbreviated month name ("Jan 15, 2024, 02:30 PM")
 */
export function formatDateTimeShort(date: Date): string {
  return format(date, "MMM d, yyyy, hh:mm a");
}

/**
 * Format timestamp to datetime string.
 * Accepts Date or ISO string only; for numeric timestamps (ms or s), convert to Date at the call site.
 */
export function formatTimestamp(timestamp: Date | string): string {
  const date = typeof timestamp === "string" ? parseISO(timestamp) : timestamp;
  return isValid(date) ? format(date, "MM/dd/yyyy, hh:mm:ss a") : "Unknown";
}

/**
 * Format date range for human-readable display.
 */
export function formatDateRangeDisplay(
  startDate: Date | string,
  endDate: Date | string,
): string {
  const start = typeof startDate === "string" ? parseISO(startDate) : startDate;
  const end = typeof endDate === "string" ? parseISO(endDate) : endDate;

  if (!isValid(start) || !isValid(end)) return "Invalid date range";

  if (isSameDay(start, end)) {
    return format(start, "MMM d, yyyy");
  }

  if (isSameMonth(start, end) && isSameYear(start, end)) {
    return `${format(start, "MMM")} ${format(start, "d")}–${format(end, "d")}, ${format(start, "yyyy")}`;
  }

  const startFormatted = isSameYear(start, end)
    ? format(start, "MMM d")
    : format(start, "MMM d, yyyy");

  return `${startFormatted} – ${format(end, "MMM d, yyyy")}`;
}

/**
 * Extract date range from summary content.
 * Looks for patterns like "2024-01-01 to 2024-01-31" in the text.
 */
export function extractDateRangeFromContent(content: string): DateRange | null {
  const isoPattern = /(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/i;
  const isoMatch = content.match(isoPattern);
  if (isoMatch) {
    const start = new Date(isoMatch[1]);
    const end = new Date(isoMatch[2]);
    if (isValid(start) && isValid(end)) {
      return { start, end };
    }
  }

  const longPattern =
    /(\w+\s+\d{1,2},?\s+\d{4})\s*[-–—]\s*(\w+\s+\d{1,2},?\s+\d{4})/i;
  const longMatch = content.match(longPattern);
  if (longMatch) {
    const start = new Date(longMatch[1]);
    const end = new Date(longMatch[2]);
    if (isValid(start) && isValid(end)) {
      return { start, end };
    }
  }

  const periodPattern =
    /period[:\s]+(\d{4}-\d{2}-\d{2})\s*[-–—]\s*(\d{4}-\d{2}-\d{2})/i;
  const periodMatch = content.match(periodPattern);
  if (periodMatch) {
    const start = new Date(periodMatch[1]);
    const end = new Date(periodMatch[2]);
    if (isValid(start) && isValid(end)) {
      return { start, end };
    }
  }

  return null;
}
