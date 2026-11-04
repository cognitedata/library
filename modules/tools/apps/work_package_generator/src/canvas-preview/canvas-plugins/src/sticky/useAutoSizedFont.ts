/**
 * Auto-sizing text hook
 * Calculates the optimal font size to fit text within a container
 */

import type { Size } from "@dune-canvas/canvas-lib";
import { useMemo } from "react";

// ============================================================================
// Constants
// ============================================================================

/** Maximum font size as a ratio of the smaller dimension */
const MAX_RELATIVE_FONT_SIZE = 0.3;

/** Minimum font size as a ratio of the smaller dimension */
const MIN_RELATIVE_FONT_SIZE = 0.02;

/** Binary search accuracy for font size */
const FONT_SIZE_SEARCH_ACCURACY = 0.5;

/** Default font size fallback */
const DEFAULT_FONT_SIZE = 14;

/** Maximum cache size for font calculations */
const MAX_CACHE_SIZE = 1000;

// ============================================================================
// Utilities
// ============================================================================

/**
 * Binary search to find the threshold value
 */
function binarySearch(
	min: number,
	max: number,
	isOverflow: (value: number) => boolean,
	accuracy: number
): number {
	while (max - min > accuracy) {
		const mid = (min + max) / 2;
		if (isOverflow(mid)) {
			max = mid;
		} else {
			min = mid;
		}
	}
	return min;
}

/**
 * Cache for font size calculations
 */
const fontSizeCache = new Map<string, number>();

/**
 * Find the maximum font size that fits the text within the given dimensions
 */
export function findMaxFontSizeWithoutOverflow(
	text: string,
	width: number,
	height: number,
	padding: number,
	options?: {
		fontFamily?: string;
		lineHeight?: number;
	}
): number {
	const fontFamily = options?.fontFamily ?? "sans-serif";
	const lineHeight = options?.lineHeight ?? 1.5;

	const cacheKey = `${text}-${width}-${height}-${padding}-${fontFamily}-${lineHeight}`;
	const cached = fontSizeCache.get(cacheKey);
	if (cached !== undefined) {
		return cached;
	}

	const minDimension = Math.min(width, height);
	const maxFontSize = MAX_RELATIVE_FONT_SIZE * minDimension;
	const minFontSize = MIN_RELATIVE_FONT_SIZE * minDimension;

	if (text.length === 0) {
		fontSizeCache.set(cacheKey, maxFontSize);
		return maxFontSize;
	}

	const availableWidth = width - padding * 2;
	const availableHeight = height - padding * 2;

	if (availableWidth <= 0 || availableHeight <= 0) {
		return minFontSize;
	}

	// Create a temporary div to measure the text
	const node = document.createElement("div");
	node.style.position = "absolute";
	node.style.visibility = "hidden";
	node.style.width = `${availableWidth}px`;
	node.style.height = `${availableHeight}px`;
	node.style.fontFamily = fontFamily;
	node.style.lineHeight = String(lineHeight);
	node.style.whiteSpace = "pre-wrap";
	node.style.wordBreak = "break-word";
	node.style.overflow = "hidden";
	node.innerText = text;

	document.body.appendChild(node);

	// Binary search for the maximum font size
	const foundFontSize = binarySearch(
		minFontSize,
		maxFontSize,
		(fontSize) => {
			node.style.fontSize = `${fontSize}px`;
			return (
				node.scrollWidth > Math.ceil(availableWidth) ||
				node.scrollHeight > Math.ceil(availableHeight)
			);
		},
		FONT_SIZE_SEARCH_ACCURACY
	);

	node.remove();

	// Cache the result (limit cache size)
	if (fontSizeCache.size > MAX_CACHE_SIZE) {
		const firstKey = fontSizeCache.keys().next().value;
		if (firstKey) fontSizeCache.delete(firstKey);
	}
	fontSizeCache.set(cacheKey, foundFontSize);

	return foundFontSize;
}

// ============================================================================
// Hook
// ============================================================================

export interface UseAutoSizedFontOptions {
	/** The text to measure */
	text: string;
	/** Container size */
	size: Size;
	/** Padding around the text */
	padding: number;
	/** Whether auto-sizing is enabled */
	enabled?: boolean;
	/** Default font size when disabled or no text */
	defaultFontSize?: number;
	/** Font family for measurement */
	fontFamily?: string;
	/** Line height for measurement */
	lineHeight?: number;
}

/**
 * Hook to calculate auto-sized font that fits within a container
 *
 * @example
 * ```tsx
 * const fontSize = useAutoSizedFont({
 *   text: content,
 *   size: { width: 200, height: 200 },
 *   padding: 12,
 * });
 * ```
 */
export function useAutoSizedFont({
	text,
	size,
	padding,
	enabled = true,
	defaultFontSize = DEFAULT_FONT_SIZE,
	fontFamily = "sans-serif",
	lineHeight = 1.5,
}: UseAutoSizedFontOptions): number {
	return useMemo(() => {
		if (!enabled || !text) {
			return defaultFontSize;
		}
		return findMaxFontSizeWithoutOverflow(
			text,
			size.width,
			size.height,
			padding,
			{
				fontFamily,
				lineHeight,
			}
		);
	}, [
		text,
		size.width,
		size.height,
		padding,
		enabled,
		defaultFontSize,
		fontFamily,
		lineHeight,
	]);
}

export default useAutoSizedFont;
