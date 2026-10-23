/**
 * Viewport Signals
 *
 * Reactive signals for viewport state that components can subscribe to.
 * Components only re-render when signals they subscribe to change.
 * Components that don't care about zoom simply don't subscribe.
 */

import {
	computed,
	type ReadonlySignal,
	type Signal,
	signal,
} from "@preact/signals-react";

import type { Bounds, Size, ViewportState } from "./types";

// ============================================================================
// Types
// ============================================================================

/**
 * Fidelity levels for progressive rendering.
 * Components can render differently based on interaction state.
 */
export type FidelityLevel = "low" | "medium" | "high";

/**
 * Visibility state for nodes relative to viewport.
 */
export type VisibilityZone =
	| "visible" // In viewport - fully render
	| "near" // Just outside viewport - keep mounted but simplified
	| "far" // Far outside viewport - unmount entirely
	| "culled"; // Outside virtualization bounds - don't create DOM element

/**
 * User-adjustable fidelity settings.
 * Allows users to trade off quality vs performance.
 */
export interface FidelitySettings {
	/**
	 * Quality bias from -1 (performance) to +1 (quality).
	 * 0 = balanced (default)
	 * -1 = maximum performance, minimum quality
	 * +1 = maximum quality, may impact performance
	 */
	qualityBias: number;

	/**
	 * Override automatic fidelity reduction during interactions.
	 * When true, always render at high fidelity regardless of pan/zoom.
	 */
	forceHighFidelity: boolean;

	/**
	 * Maximum data points to render per chart (safety limit).
	 * Prevents excessive memory/CPU usage for very large datasets.
	 */
	maxDataPoints: number;

	/**
	 * Downsampling algorithm preference.
	 * - 'lttb': Best visual preservation (default)
	 * - 'minmax': Preserves peaks/valleys
	 * - 'adaptive': LTTB + extreme value preservation
	 */
	downsampleMode: "lttb" | "minmax" | "adaptive";
}

// ============================================================================
// Core Signals
// ============================================================================

/**
 * Current viewport scale (zoom level).
 * Updates on every zoom change.
 */
export const viewportScale: Signal<number> = signal(1);

/**
 * Whether the user is actively zooming.
 * True during zoom gestures, false when zoom has stabilized.
 */
export const isZooming: Signal<boolean> = signal(false);

/**
 * Whether the user is actively panning.
 * True during pan gestures, false when pan has stopped.
 */
export const isPanning: Signal<boolean> = signal(false);

/**
 * Whether any interaction (zoom, pan, drag) is happening.
 * Useful for components that want to reduce fidelity during any interaction.
 */
export const isInteracting: ReadonlySignal<boolean> = computed(
	() => isZooming.value || isPanning.value
);

/**
 * Stable viewport scale - only updates when zoom has stabilized.
 * Use this for expensive operations like re-rendering at higher quality.
 */
export const stableScale: Signal<number> = signal(1);

/**
 * Current viewport bounds in canvas coordinates.
 * Updated whenever viewport changes (pan/zoom).
 */
export const viewportBounds: Signal<Bounds> = signal({
	x: 0,
	y: 0,
	width: 1000,
	height: 800,
});

/**
 * Extended viewport bounds for virtualization (includes buffer zone).
 * Nodes in this zone should be mounted but may render at reduced fidelity.
 */
export const virtualBounds: Signal<Bounds> = signal({
	x: -500,
	y: -400,
	width: 2000,
	height: 1600,
});

/**
 * User-adjustable fidelity settings.
 * Can be modified via setFidelitySettings() or individual setter functions.
 */
export const fidelitySettings: Signal<FidelitySettings> = signal({
	qualityBias: 0,
	forceHighFidelity: false,
	maxDataPoints: 10000,
	downsampleMode: "lttb",
});

/**
 * Current fidelity level based on interaction state and user settings.
 * - 'low': During rapid interactions (fast pan/zoom) - unless forceHighFidelity
 * - 'medium': During slow interactions or with negative quality bias
 * - 'high': When idle or with forceHighFidelity enabled
 */
export const fidelityLevel: ReadonlySignal<FidelityLevel> = computed(() => {
	const settings = fidelitySettings.value;

	// If force high fidelity is enabled, always return high
	if (settings.forceHighFidelity) {
		return "high";
	}

	// During interactions, reduce fidelity
	if (isZooming.value || isPanning.value) {
		// With high quality bias, use medium during interactions instead of low
		if (settings.qualityBias > 0.5) {
			return "medium";
		}
		return "low";
	}

	// When idle, use quality bias to determine level
	// Very negative bias (-0.5 to -1) = medium fidelity even when idle
	if (settings.qualityBias < -0.5) {
		return "medium";
	}

	return "high";
});

/**
 * Computed quality multiplier based on user settings.
 * Range: 0.25 (min quality) to 2.0 (max quality)
 * Affects data point counts and render quality.
 */
export const qualityMultiplier: ReadonlySignal<number> = computed(() => {
	const bias = fidelitySettings.value.qualityBias;
	// Map -1..+1 to 0.25..2.0 (exponential for more granularity at high end)
	return 2 ** bias;
});

// ============================================================================
// Internal State
// ============================================================================

let zoomTimeout: ReturnType<typeof setTimeout> | null = null;
let panTimeout: ReturnType<typeof setTimeout> | null = null;
const ZOOM_STABILIZE_DELAY = 200; // ms
const PAN_STABILIZE_DELAY = 100; // ms

// Virtualization buffer multiplier (extends viewport bounds by this factor)
const VIRTUAL_BUFFER_MULTIPLIER = 1.5;

// ============================================================================
// Update Functions (called by Canvas)
// ============================================================================

/**
 * Update the viewport scale.
 * Called by Canvas when zoom changes.
 * Automatically tracks zoom stability with debouncing.
 */
export function updateScale(newScale: number): void {
	viewportScale.value = newScale;
	isZooming.value = true;

	// Clear existing timeout
	if (zoomTimeout) {
		clearTimeout(zoomTimeout);
	}

	// Set new timeout - when it fires, zoom has stabilized
	zoomTimeout = setTimeout(() => {
		isZooming.value = false;
		stableScale.value = newScale;
		zoomTimeout = null;
	}, ZOOM_STABILIZE_DELAY);
}

/**
 * Update panning state.
 * Called by Canvas during pan operations.
 */
export function updatePanning(panning: boolean): void {
	if (panning) {
		isPanning.value = true;

		// Clear existing timeout
		if (panTimeout) {
			clearTimeout(panTimeout);
			panTimeout = null;
		}
	} else {
		// Debounce the end of panning
		if (panTimeout) {
			clearTimeout(panTimeout);
		}
		panTimeout = setTimeout(() => {
			isPanning.value = false;
			panTimeout = null;
		}, PAN_STABILIZE_DELAY);
	}
}

/**
 * Update viewport bounds.
 * Called by Canvas when viewport changes.
 *
 * @param viewport - Current viewport state
 * @param containerSize - Size of the canvas container in screen pixels
 */
export function updateViewportBounds(
	viewport: ViewportState,
	containerSize: Size
): void {
	// Calculate visible bounds in canvas coordinates
	const canvasBounds: Bounds = {
		x: -viewport.x / viewport.scale,
		y: -viewport.y / viewport.scale,
		width: containerSize.width / viewport.scale,
		height: containerSize.height / viewport.scale,
	};

	viewportBounds.value = canvasBounds;

	// Calculate extended virtual bounds with buffer
	const bufferX = canvasBounds.width * VIRTUAL_BUFFER_MULTIPLIER;
	const bufferY = canvasBounds.height * VIRTUAL_BUFFER_MULTIPLIER;

	virtualBounds.value = {
		x: canvasBounds.x - bufferX,
		y: canvasBounds.y - bufferY,
		width: canvasBounds.width + bufferX * 2,
		height: canvasBounds.height + bufferY * 2,
	};
}

// ============================================================================
// Fidelity Settings Functions
// ============================================================================

/**
 * Update fidelity settings.
 * Partial updates are supported - only provided fields are updated.
 */
export function setFidelitySettings(settings: Partial<FidelitySettings>): void {
	fidelitySettings.value = {
		...fidelitySettings.value,
		...settings,
	};
}

/**
 * Set the quality bias.
 * @param bias - Value from -1 (performance) to +1 (quality)
 */
export function setQualityBias(bias: number): void {
	const clampedBias = Math.max(-1, Math.min(1, bias));
	fidelitySettings.value = {
		...fidelitySettings.value,
		qualityBias: clampedBias,
	};
}

/**
 * Toggle force high fidelity mode.
 * When enabled, always renders at high quality regardless of interactions.
 */
export function setForceHighFidelity(force: boolean): void {
	fidelitySettings.value = {
		...fidelitySettings.value,
		forceHighFidelity: force,
	};
}

/**
 * Set the maximum data points limit.
 * @param max - Maximum number of data points per chart
 */
export function setMaxDataPoints(max: number): void {
	fidelitySettings.value = {
		...fidelitySettings.value,
		maxDataPoints: Math.max(100, max),
	};
}

/**
 * Set the downsampling algorithm mode.
 */
export function setDownsampleMode(
	mode: FidelitySettings["downsampleMode"]
): void {
	fidelitySettings.value = {
		...fidelitySettings.value,
		downsampleMode: mode,
	};
}

// ============================================================================
// Convenience Hooks
// ============================================================================
