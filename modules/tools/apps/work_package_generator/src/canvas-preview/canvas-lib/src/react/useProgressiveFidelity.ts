/**
 * Progressive Fidelity Hook
 *
 * Provides components with reactive access to viewport interaction state
 * so they can render at reduced fidelity during pan/zoom operations.
 *
 * Usage:
 * ```tsx
 * const { fidelity, isInteracting, shouldReduceFidelity, dataPointMultiplier } = useProgressiveFidelity();
 *
 * // Use shouldReduceFidelity for simple on/off decisions
 * if (shouldReduceFidelity) {
 *   return <SimplifiedChart data={data} />;
 * }
 *
 * // Or use fidelity level for more granular control
 * const pointCount = fidelity === 'high' ? 1000 : fidelity === 'medium' ? 500 : 100;
 * ```
 */

import { useCallback, useMemo } from "react";

import type { DownsampleMode } from "../core/downsampling";
import {
	type FidelityLevel,
	type FidelitySettings,
	fidelityLevel,
	fidelitySettings,
	isInteracting,
	isPanning,
	isZooming,
	qualityMultiplier,
	setDownsampleMode,
	setForceHighFidelity,
	setMaxDataPoints,
	setQualityBias,
	stableScale,
	viewportScale,
} from "../core/viewportSignals";

// ============================================================================
// Types
// ============================================================================

export interface ProgressiveFidelityState {
	/**
	 * Current fidelity level: 'low', 'medium', or 'high'
	 */
	fidelity: FidelityLevel;

	/**
	 * Whether any viewport interaction (pan/zoom) is happening
	 */
	isInteracting: boolean;

	/**
	 * Whether the user is actively zooming
	 */
	isZooming: boolean;

	/**
	 * Whether the user is actively panning
	 */
	isPanning: boolean;

	/**
	 * Convenience boolean - true when fidelity should be reduced
	 */
	shouldReduceFidelity: boolean;

	/**
	 * Current zoom scale (updates during zoom)
	 */
	currentScale: number;

	/**
	 * Stable zoom scale (updates only after zoom gesture ends)
	 */
	stableScale: number;

	/**
	 * Suggested multiplier for data point counts based on zoom level and user settings.
	 * Higher zoom = more detail needed.
	 * Adjusted by user's quality bias setting.
	 * Capped at maxScale to prevent excessive resource usage.
	 */
	dataPointMultiplier: number;

	/**
	 * Suggested render detail level (0-1) based on fidelity.
	 * 0 = minimum detail, 1 = maximum detail
	 */
	renderDetail: number;

	/**
	 * User-configured fidelity settings
	 */
	settings: FidelitySettings;

	/**
	 * Maximum data points to render (from user settings)
	 */
	maxDataPoints: number;

	/**
	 * Preferred downsampling algorithm (from user settings)
	 */
	downsampleMode: DownsampleMode;

	/**
	 * Functions to update fidelity settings
	 */
	setQualityBias: (bias: number) => void;
	setForceHighFidelity: (force: boolean) => void;
	setMaxDataPoints: (max: number) => void;
	setDownsampleMode: (mode: DownsampleMode) => void;
}

export interface UseProgressiveFidelityOptions {
	/**
	 * Maximum zoom multiplier for data point scaling.
	 * Prevents excessive memory usage at high zoom levels.
	 * Default: 4
	 */
	maxZoomMultiplier?: number;

	/**
	 * Whether to use stable scale (waits for zoom to finish) or current scale.
	 * Default: true (uses stable scale to prevent re-renders during zoom)
	 */
	useStableScale?: boolean;
}

// ============================================================================
// Hook
// ============================================================================

/**
 * Hook for progressive fidelity rendering.
 * Components can use this to adapt their rendering based on viewport state and user settings.
 */
export function useProgressiveFidelity(
	options: UseProgressiveFidelityOptions = {}
): ProgressiveFidelityState {
	const { maxZoomMultiplier = 4, useStableScale = true } = options;

	// Subscribe to signals
	const fidelity = fidelityLevel.value;
	const settings = fidelitySettings.value;
	const quality = qualityMultiplier.value;
	const interacting = isInteracting.value;
	const zooming = isZooming.value;
	const panning = isPanning.value;
	const currentScale = viewportScale.value;
	const stable = stableScale.value;

	// Calculate derived values
	const shouldReduceFidelity = fidelity !== "high" || interacting;

	// Use stable scale for data point calculations to avoid re-computing during zoom
	const effectiveScale = useStableScale ? stable : currentScale;

	// Data point multiplier: combines zoom level with user quality preference
	// quality ranges from 0.5 (bias=-1) to 2.0 (bias=+1)
	const dataPointMultiplier = Math.min(
		effectiveScale * quality,
		maxZoomMultiplier * 2
	);

	// Render detail: 0-1 scale based on fidelity, adjusted by quality bias
	const baseRenderDetail =
		fidelity === "high" ? 1 : fidelity === "medium" ? 0.5 : 0.25;
	const renderDetail = Math.min(1, baseRenderDetail * quality);

	// Memoized setter callbacks
	const handleSetQualityBias = useCallback(
		(bias: number) => setQualityBias(bias),
		[]
	);
	const handleSetForceHighFidelity = useCallback(
		(force: boolean) => setForceHighFidelity(force),
		[]
	);
	const handleSetMaxDataPoints = useCallback(
		(max: number) => setMaxDataPoints(max),
		[]
	);
	const handleSetDownsampleMode = useCallback(
		(mode: DownsampleMode) => setDownsampleMode(mode),
		[]
	);

	return useMemo(
		() => ({
			fidelity,
			isInteracting: interacting,
			isZooming: zooming,
			isPanning: panning,
			shouldReduceFidelity,
			currentScale,
			stableScale: stable,
			dataPointMultiplier,
			renderDetail,
			settings,
			maxDataPoints: settings.maxDataPoints,
			downsampleMode: settings.downsampleMode,
			setQualityBias: handleSetQualityBias,
			setForceHighFidelity: handleSetForceHighFidelity,
			setMaxDataPoints: handleSetMaxDataPoints,
			setDownsampleMode: handleSetDownsampleMode,
		}),
		[
			fidelity,
			interacting,
			zooming,
			panning,
			shouldReduceFidelity,
			currentScale,
			stable,
			dataPointMultiplier,
			renderDetail,
			settings,
			handleSetQualityBias,
			handleSetForceHighFidelity,
			handleSetMaxDataPoints,
			handleSetDownsampleMode,
		]
	);
}
