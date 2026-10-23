/**
 * Canvas Background Component
 * Renders a dot grid background that responds correctly to pan and zoom
 *
 * Uses SVG pattern elements (like React Flow) instead of CSS background-image
 * to avoid creating new data URLs on every pan/zoom.
 */

import type React from "react";
import { memo, useId } from "react";

import type { ViewportState } from "../core";

// ============================================================================
// Types
// ============================================================================

export interface BackgroundProps {
	viewport: ViewportState;
	/** Gap between dots in canvas units */
	gap?: number;
	/** Dot radius in screen pixels */
	dotRadius?: number;
	/** Dot color */
	color?: string;
	/** Background color */
	bgColor?: string;
	className?: string;
	style?: React.CSSProperties;
}

// ============================================================================
// Constants
// ============================================================================

// Default gap between dots in canvas coordinates
const DEFAULT_GAP = 50;

// Min/max screen pixel spacing to keep dots visible but not overwhelming
const MIN_SCREEN_SPACING = 15; // Don't go denser than this (zoom out)
const MAX_SCREEN_SPACING = 100; // Don't go sparser than this (zoom in)

// Default dot radius in screen pixels
const DEFAULT_DOT_RADIUS = 1.5;

// Default colors
const DEFAULT_DOT_COLOR = "#c0c0c0";
const DEFAULT_BG_COLOR = "#fafafa";

/**
 * Calculate grid spacing that stays within reasonable bounds
 * When zoomed out too far, double the spacing (show fewer dots)
 * When zoomed in too far, halve the spacing (show more dots)
 */
function getAdjustedSpacing(baseSpacing: number, scale: number): number {
	let spacing = baseSpacing * scale;

	// If too dense (zoomed out), double the spacing
	while (spacing < MIN_SCREEN_SPACING) {
		spacing *= 2;
	}
	// If too sparse (zoomed in), halve the spacing
	while (spacing > MAX_SCREEN_SPACING) {
		spacing /= 2;
	}

	return spacing;
}

// ============================================================================
// Component
// ============================================================================

function BackgroundComponent({
	viewport,
	gap = DEFAULT_GAP,
	dotRadius = DEFAULT_DOT_RADIUS,
	color = DEFAULT_DOT_COLOR,
	bgColor = DEFAULT_BG_COLOR,
	className,
	style,
}: BackgroundProps) {
	const patternId = useId();
	const { x, y, scale } = viewport;

	// Get adjusted spacing that stays within reasonable bounds
	const adjustedGap = getAdjustedSpacing(gap, scale);

	// Keep dot radius fixed in screen pixels (don't scale with zoom)
	const fixedRadius = dotRadius;

	// Pattern position follows the viewport pan
	const patternX = x % adjustedGap;
	const patternY = y % adjustedGap;

	return (
		<svg
			className={className}
			style={{
				position: "absolute",
				inset: 0,
				width: "100%",
				height: "100%",
				pointerEvents: "none",
				...style,
			}}
			data-canvas-background
		>
			{/* Background fill */}
			<rect x="0" y="0" width="100%" height="100%" fill={bgColor} />

			{/* Dot pattern definition */}
			<pattern
				id={patternId}
				x={patternX}
				y={patternY}
				width={adjustedGap}
				height={adjustedGap}
				patternUnits="userSpaceOnUse"
			>
				<circle
					cx={fixedRadius}
					cy={fixedRadius}
					r={fixedRadius}
					fill={color}
				/>
			</pattern>

			{/* Fill the entire area with the pattern */}
			<rect
				x="0"
				y="0"
				width="100%"
				height="100%"
				fill={`url(#${patternId})`}
			/>
		</svg>
	);
}

BackgroundComponent.displayName = "Background";

export const Background = memo(BackgroundComponent);
