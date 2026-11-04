/**
 * Downsampling algorithms for time series data
 *
 * These algorithms reduce the number of data points while preserving
 * the visual characteristics of the data.
 */

// Point type is available in ./types if needed for future extensions

// ============================================================================
// Types
// ============================================================================

export interface DataPoint {
	x: number;
	y: number;
}

// ============================================================================
// LTTB Algorithm (Largest Triangle Three Buckets)
// ============================================================================

/**
 * Largest Triangle Three Buckets (LTTB) downsampling algorithm.
 *
 * This algorithm is specifically designed for time series visualization.
 * It preserves the visual shape of the data by selecting points that
 * form the largest triangle with adjacent bucket points.
 *
 * Reference: https://skemman.is/bitstream/1946/15343/3/SS_MSthesis.pdf
 *
 * @param data - Array of data points sorted by x value
 * @param targetPoints - Desired number of output points
 * @returns Downsampled array of points
 */
export function lttbDownsample<T extends DataPoint>(
	data: T[],
	targetPoints: number
): T[] {
	const dataLength = data.length;

	// If we don't need to downsample, return the original data
	if (targetPoints >= dataLength || targetPoints <= 2) {
		return targetPoints <= 2 ? data.slice(0, Math.max(2, targetPoints)) : data;
	}

	const sampled: T[] = [];

	// Bucket size (excluding first and last points which are always included)
	const bucketSize = (dataLength - 2) / (targetPoints - 2);

	// Always include the first point
	sampled.push(data[0]);

	let prevSelectedIndex = 0;

	for (let i = 0; i < targetPoints - 2; i++) {
		// Calculate the range for this bucket
		const bucketStart = Math.floor((i + 0) * bucketSize) + 1;
		const bucketEnd = Math.floor((i + 1) * bucketSize) + 1;

		// Calculate the range for the next bucket (for averaging)
		const nextBucketStart = Math.floor((i + 1) * bucketSize) + 1;
		const nextBucketEnd = Math.min(
			Math.floor((i + 2) * bucketSize) + 1,
			dataLength - 1
		);

		// Calculate average point in the next bucket
		let avgX = 0;
		let avgY = 0;
		const nextBucketLength = nextBucketEnd - nextBucketStart;

		for (let j = nextBucketStart; j < nextBucketEnd; j++) {
			avgX += data[j].x;
			avgY += data[j].y;
		}

		if (nextBucketLength > 0) {
			avgX /= nextBucketLength;
			avgY /= nextBucketLength;
		} else {
			// Fallback to last point if next bucket is empty
			avgX = data[dataLength - 1].x;
			avgY = data[dataLength - 1].y;
		}

		// Point from the previous selected bucket
		const prevPoint = data[prevSelectedIndex];

		// Find the point in the current bucket that forms the largest triangle
		let maxArea = -1;
		let maxAreaIndex = bucketStart;

		for (let j = bucketStart; j < bucketEnd && j < dataLength - 1; j++) {
			// Calculate triangle area using the cross product formula
			// Area = 0.5 * |x1(y2 - y3) + x2(y3 - y1) + x3(y1 - y2)|
			const area = Math.abs(
				(prevPoint.x - avgX) * (data[j].y - prevPoint.y) -
					(prevPoint.x - data[j].x) * (avgY - prevPoint.y)
			);

			if (area > maxArea) {
				maxArea = area;
				maxAreaIndex = j;
			}
		}

		sampled.push(data[maxAreaIndex]);
		prevSelectedIndex = maxAreaIndex;
	}

	// Always include the last point
	sampled.push(data[dataLength - 1]);

	return sampled;
}

// ============================================================================
// Min/Max Bucketing
// ============================================================================

/**
 * Min/Max bucketing downsampling algorithm.
 *
 * This algorithm preserves peaks and valleys by keeping both the minimum
 * and maximum values in each bucket. Good for preserving extreme values.
 *
 * @param data - Array of data points sorted by x value
 * @param targetPoints - Desired number of output points (will output up to 2x this)
 * @returns Downsampled array of points
 */
export function minMaxDownsample<T extends DataPoint>(
	data: T[],
	targetPoints: number
): T[] {
	const dataLength = data.length;

	if (dataLength <= targetPoints) {
		return data;
	}

	const bucketCount = Math.ceil(targetPoints / 2);
	const bucketSize = Math.ceil(dataLength / bucketCount);
	const result: T[] = [];

	for (let i = 0; i < dataLength; i += bucketSize) {
		const bucketEnd = Math.min(i + bucketSize, dataLength);
		let minPoint = data[i];
		let maxPoint = data[i];

		for (let j = i; j < bucketEnd; j++) {
			if (data[j].y < minPoint.y) minPoint = data[j];
			if (data[j].y > maxPoint.y) maxPoint = data[j];
		}

		// Add points in x-order
		if (minPoint.x < maxPoint.x) {
			result.push(minPoint, maxPoint);
		} else if (minPoint.x > maxPoint.x) {
			result.push(maxPoint, minPoint);
		} else {
			// Same point
			result.push(minPoint);
		}
	}

	return result;
}

// ============================================================================
// Adaptive Downsampling
// ============================================================================

/**
 * Adaptive downsampling that combines LTTB with min/max preservation.
 *
 * Uses LTTB as the primary algorithm but ensures that extreme values
 * (significant peaks and valleys) are not lost.
 *
 * @param data - Array of data points sorted by x value
 * @param targetPoints - Desired number of output points
 * @param extremeThreshold - Threshold for considering a point "extreme" (0-1, default 0.9)
 * @returns Downsampled array of points
 */
export function adaptiveDownsample<T extends DataPoint>(
	data: T[],
	targetPoints: number,
	extremeThreshold: number = 0.9
): T[] {
	const dataLength = data.length;

	if (dataLength <= targetPoints) {
		return data;
	}

	// Find global min/max for normalization
	let globalMin = Infinity;
	let globalMax = -Infinity;

	for (const point of data) {
		if (point.y < globalMin) globalMin = point.y;
		if (point.y > globalMax) globalMax = point.y;
	}

	const range = globalMax - globalMin || 1;
	const extremeMinThreshold = globalMin + range * (1 - extremeThreshold);
	const extremeMaxThreshold = globalMax - range * (1 - extremeThreshold);

	// Find extreme points
	const extremeIndices = new Set<number>();
	extremeIndices.add(0); // Always include first
	extremeIndices.add(dataLength - 1); // Always include last

	for (let i = 1; i < dataLength - 1; i++) {
		const y = data[i].y;
		if (y <= extremeMinThreshold || y >= extremeMaxThreshold) {
			extremeIndices.add(i);
		}
	}

	// If we have more extreme points than target, just use LTTB
	if (extremeIndices.size >= targetPoints * 0.5) {
		return lttbDownsample(data, targetPoints);
	}

	// Use LTTB for the remaining budget
	const lttbBudget = targetPoints - extremeIndices.size;
	const lttbResult = lttbDownsample(data, Math.max(2, lttbBudget));

	// Merge extreme points with LTTB result
	const resultSet = new Set<number>();

	for (const idx of extremeIndices) {
		resultSet.add(idx);
	}

	for (const point of lttbResult) {
		const idx = data.indexOf(point);
		if (idx !== -1) {
			resultSet.add(idx);
		}
	}

	// Convert to sorted array
	const sortedIndices = Array.from(resultSet).sort((a, b) => a - b);

	return sortedIndices.map((idx) => data[idx]);
}

// ============================================================================
// Mode-Based Downsampling
// ============================================================================

export type DownsampleMode = "lttb" | "minmax" | "adaptive";

/**
 * Downsample data using the specified algorithm.
 *
 * @param data - Array of data points sorted by x value
 * @param targetPoints - Desired number of output points
 * @param mode - Downsampling algorithm to use
 * @returns Downsampled array of points
 */
export function downsample<T extends DataPoint>(
	data: T[],
	targetPoints: number,
	mode: DownsampleMode = "lttb"
): T[] {
	switch (mode) {
		case "minmax":
			return minMaxDownsample(data, targetPoints);
		case "adaptive":
			return adaptiveDownsample(data, targetPoints);
		default:
			return lttbDownsample(data, targetPoints);
	}
}
