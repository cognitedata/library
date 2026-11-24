import type { TimeseriesDataPoint } from "@dune-canvas/canvas-lib";

/** Format a millisecond timestamp for the crosshair tooltip */
export function formatTimestamp(ms: number): string {
	const d = new Date(ms);
	const pad = (n: number) => String(n).padStart(2, "0");
	const date = `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${d.getFullYear()}`;
	const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
	// Timezone offset
	const offset = -d.getTimezoneOffset();
	const sign = offset >= 0 ? "+" : "-";
	const tzH = pad(Math.floor(Math.abs(offset) / 60));
	const tzM = pad(Math.abs(offset) % 60);
	const tz = `GMT(${sign}${tzH}:${tzM})`;
	return `${date}, ${time} ${tz}`;
}

/**
 * Compute aggregate stats for a set of data points.
 * When the data was fetched with server-side aggregation (min/max/count per bucket),
 * uses those values for accurate stats across the full dataset.
 * Falls back to client-side computation for raw datapoints.
 */
export function computeStats(points: TimeseriesDataPoint[]) {
	if (points.length === 0) return null;

	// Check if data carries server-side aggregates
	const hasAggregates = points[0].min !== undefined;

	if (hasAggregates) {
		let min = Infinity;
		let max = -Infinity;
		let totalCount = 0;
		let weightedSum = 0;
		for (const p of points) {
			if (p.min != null && p.min < min) min = p.min;
			if (p.max != null && p.max > max) max = p.max;
			const bucketCount = p.count ?? 1;
			totalCount += bucketCount;
			weightedSum += p.y * bucketCount; // y = bucket average
		}
		return {
			min: min.toFixed(3),
			max: max.toFixed(3),
			average: totalCount > 0 ? (weightedSum / totalCount).toFixed(3) : "0.000",
			count: totalCount,
		};
	}

	// Fallback: client-side computation for raw datapoints
	let min = Infinity;
	let max = -Infinity;
	let sum = 0;
	for (const p of points) {
		if (p.y < min) min = p.y;
		if (p.y > max) max = p.y;
		sum += p.y;
	}
	return {
		min: min.toFixed(3),
		max: max.toFixed(3),
		average: (sum / points.length).toFixed(3),
		count: points.length,
	};
}
