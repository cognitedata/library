/**
 * Cursor pagination helpers for DMS reads with hard caps on total items.
 */

export interface FetchPageResult<T> {
	items: T[];
	nextCursor?: string;
}

/**
 * Read `nextCursor` from a DMS response without asserting SDK types.
 * Search and list payloads both include this field at runtime.
 */
export function readOptionalNextCursor(response: unknown): string | undefined {
	if (typeof response !== "object" || response === null) return undefined;
	if (!("nextCursor" in response)) return undefined;
	const cursor = response.nextCursor;
	return typeof cursor === "string" && cursor.length > 0 ? cursor : undefined;
}

/**
 * Repeatedly fetch pages until the cursor is exhausted or maxTotalItems is reached.
 * When a page would exceed the cap, only the leading slice is kept.
 */
export async function fetchPagesWithItemCap<T>(options: {
	maxTotalItems: number;
	/** Shown in console.warn when results are truncated (e.g. "getAssetResourcesGeneric:file"). */
	warnContext: string;
	fetchPage: (cursor: string | undefined) => Promise<FetchPageResult<T>>;
}): Promise<{ items: T[]; truncated: boolean }> {
	const items: T[] = [];
	let cursor: string | undefined;
	let truncated = false;

	const warnTruncated = () => {
		console.warn(
			`[dms] ${options.warnContext}: truncated at ${options.maxTotalItems} items (more data available)`
		);
	};

	while (true) {
		const page = await options.fetchPage(cursor);
		const batch = page.items ?? [];
		const room = options.maxTotalItems - items.length;

		if (room <= 0) {
			truncated = true;
			break;
		}

		if (batch.length <= room) {
			items.push(...batch);
			cursor = page.nextCursor;
			if (items.length >= options.maxTotalItems && cursor) {
				truncated = true;
				warnTruncated();
				break;
			}
			if (!cursor) break;
		} else {
			items.push(...batch.slice(0, room));
			truncated = true;
			if (page.nextCursor) warnTruncated();
			break;
		}
	}

	return { items, truncated };
}
