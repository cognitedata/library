/**
 * Date and Time Utilities
 * Utilities for date formatting and manipulation
 */

/**
 * Returns the current time as an ISO 8601 string with millisecond precision.
 * @example nowIsoMs() // '2024-01-15T10:30:00.123Z'
 */
export function nowIsoMs(): string {
	return new Date().toISOString();
}

/**
 * Safely parses a date string and returns a valid Date object or null.
 * Validates that the parsed date is actually valid before returning it.
 * @param input - The date string to parse
 * @returns Valid Date object or null if parsing fails
 */
export function safeParseDate(input: string | null | undefined): Date | null {
	if (!input || typeof input !== "string" || input.trim() === "") {
		return null;
	}

	try {
		const date = new Date(input.trim());

		// Check if the date is valid - new Date() doesn't throw on invalid strings
		if (Number.isNaN(date.getTime())) {
			return null;
		}

		return date;
	} catch {
		return null;
	}
}

/**
 * Safely converts a date string to ISO format for CDF queries.
 * Returns null if the date string is invalid to prevent runtime errors.
 * @param input - The date string to convert
 * @returns ISO string or null if parsing fails
 */
export function safeToISOString(
	input: string | null | undefined
): string | null {
	const date = safeParseDate(input);
	return date ? date.toISOString() : null;
}

/**
 * Safely creates an end-of-day date for date range queries.
 * Sets time to 23:59:59.999 UTC and converts to ISO format.
 * @param input - The date string to convert
 * @returns ISO string for end of day or null if parsing fails
 */
export function safeEndOfDayISOString(
	input: string | null | undefined
): string | null {
	const date = safeParseDate(input);
	if (!date) return null;

	try {
		// Set to end of day in UTC to avoid timezone issues
		date.setUTCHours(23, 59, 59, 999);
		return date.toISOString();
	} catch {
		return null;
	}
}

/**
 * Format an ISO date string for display (e.g. "Jan 15, 2024").
 * Returns `null` when the input is falsy, and falls back to the raw string on
 * parse failure so invalid values are still visible rather than silently lost.
 */
export function formatDate(iso?: string): string | null {
	if (!iso) return null;

	try {
		const date = new Date(iso);

		// Check if the date is valid - new Date() doesn't throw on invalid strings
		if (Number.isNaN(date.getTime())) {
			return iso; // Return original string so invalid values remain visible
		}

		return date.toLocaleDateString(undefined, {
			year: "numeric",
			month: "short",
			day: "numeric",
		});
	} catch {
		// Fallback for any unexpected errors
		return iso;
	}
}
