/**
 * Formatting Utilities
 * Shared utilities for formatting data for display
 */

/**
 * Map property names to display-friendly column titles
 */
export function getColumnTitle(prop: string): string {
	const displayNames: Record<string, string> = {
		sourceUnit: "Unit",
		mimeType: "MIME Type",
		externalId: "External ID",
	};
	return displayNames[prop] || prop;
}

/**
 * Format property values for display, handling objects and arrays
 *
 * @param value - The value to format (can be any type)
 * @returns A formatted string representation of the value
 *
 * @example
 * ```ts
 * formatPropertyValue('test') // 'test'
 * formatPropertyValue(123) // '123'
 * formatPropertyValue({ name: 'Item' }) // 'Item'
 * formatPropertyValue([1, 2, 3]) // '1, 2, 3'
 * formatPropertyValue(null) // '-'
 * ```
 */
export function formatPropertyValue(value: unknown): string {
	if (value == null) return "-";
	if (typeof value === "string") return value || "-";
	if (typeof value === "number" || typeof value === "boolean")
		return String(value);

	if (typeof value === "object") {
		const obj = value as Record<string, unknown>;

		// Handle arrays
		if (Array.isArray(value)) {
			if (value.length === 0) return "-";
			return (
				value
					.map((v) => formatPropertyValue(v))
					.filter((v) => v !== "-")
					.join(", ") || "-"
			);
		}

		// Try common property names for objects (like unit objects)
		if ("name" in obj && obj.name) return String(obj.name);
		if ("label" in obj && obj.label) return String(obj.label);
		if ("externalId" in obj && obj.externalId) return String(obj.externalId);
		if ("value" in obj && obj.value != null) return String(obj.value);

		// Fall back to showing key-value pairs
		const entries = Object.entries(obj).filter(([, v]) => v != null);
		if (entries.length === 0) return "-";
		if (entries.length === 1) return formatPropertyValue(entries[0][1]);
		return entries
			.map(([k, v]) => `${k}: ${formatPropertyValue(v)}`)
			.join(", ");
	}

	return String(value);
}
