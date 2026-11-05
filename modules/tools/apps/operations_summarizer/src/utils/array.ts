/**
 * Array helpers for sorting and selecting items by createdTime.
 */

/** Item with optional createdTime (number or Date). */
export interface WithCreatedTime {
  createdTime?: number | Date;
}

function toEpochMs(value: number | Date | undefined): number {
  if (value === undefined) return 0;
  return typeof value === "number" ? value : value.getTime();
}

/**
 * Returns a new array sorted by createdTime descending (newest first).
 */
export function sortByCreatedTimeDesc<T extends WithCreatedTime>(
  items: T[],
): T[] {
  return [...items].sort(
    (a, b) => toEpochMs(b.createdTime) - toEpochMs(a.createdTime),
  );
}

/**
 * Returns the latest item by createdTime (descending). Returns null if the array is empty.
 * Single-pass O(n); does not allocate a copy of the array.
 */
export function getLatestByCreatedTime<T extends WithCreatedTime>(
  items: T[],
): T | null {
  if (items.length === 0) return null;
  return items.reduce((latest, current) =>
    toEpochMs(current.createdTime) > toEpochMs(latest.createdTime)
      ? current
      : latest,
  );
}
