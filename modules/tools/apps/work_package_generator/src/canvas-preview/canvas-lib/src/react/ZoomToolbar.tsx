/**
 * ZoomToolbar Component
 * Provides zoom controls at the bottom right of the canvas
 */

import type React from "react";

/**
 * Predefined zoom levels
 */
export const ZOOM_LEVELS = [
	0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0,
] as const;

export interface ZoomToolbarProps {
	/** Current zoom scale (1 = 100%) */
	scale: number;
	/** Callback when zoom changes */
	onZoomChange: (scale: number) => void;
	/** Callback to zoom to fit all content */
	onZoomToFit: () => void;
}

/**
 * Zoom button component
 */
const ZoomButton: React.FC<{
	onClick: () => void;
	disabled?: boolean;
	title: string;
	children: React.ReactNode;
}> = ({ onClick, disabled, title, children }) => (
	<button
		onClick={onClick}
		disabled={disabled}
		title={title}
		style={{
			display: "flex",
			alignItems: "center",
			justifyContent: "center",
			width: 32,
			height: 32,
			padding: 0,
			border: "none",
			borderRadius: 4,
			backgroundColor: "transparent",
			color: disabled ? "#ccc" : "#333",
			cursor: disabled ? "default" : "pointer",
			transition: "background-color 0.15s",
		}}
		onMouseEnter={(e) => {
			if (!disabled) {
				e.currentTarget.style.backgroundColor = "rgba(0, 0, 0, 0.05)";
			}
		}}
		onMouseLeave={(e) => {
			e.currentTarget.style.backgroundColor = "transparent";
		}}
	>
		{children}
	</button>
);

/**
 * Zoom out icon (minus)
 */
const ZoomOutIcon: React.FC<{ size?: number }> = ({ size = 16 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
		strokeLinecap="round"
		strokeLinejoin="round"
	>
		<circle cx="11" cy="11" r="8" />
		<line x1="21" y1="21" x2="16.65" y2="16.65" />
		<line x1="8" y1="11" x2="14" y2="11" />
	</svg>
);

/**
 * Zoom in icon (plus)
 */
const ZoomInIcon: React.FC<{ size?: number }> = ({ size = 16 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
		strokeLinecap="round"
		strokeLinejoin="round"
	>
		<circle cx="11" cy="11" r="8" />
		<line x1="21" y1="21" x2="16.65" y2="16.65" />
		<line x1="11" y1="8" x2="11" y2="14" />
		<line x1="8" y1="11" x2="14" y2="11" />
	</svg>
);

/**
 * Zoom to fit icon (arrows pointing outward to corners)
 */
const ZoomToFitIcon: React.FC<{ size?: number }> = ({ size = 16 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
		strokeLinecap="round"
		strokeLinejoin="round"
	>
		{/* Top-left arrow */}
		<polyline points="4 14 4 4 14 4" />
		<line x1="4" y1="4" x2="9" y2="9" />
		{/* Bottom-right arrow */}
		<polyline points="20 10 20 20 10 20" />
		<line x1="20" y1="20" x2="15" y2="15" />
	</svg>
);

/**
 * ZoomToolbar component
 * Positioned at the bottom right of the canvas
 */
export const ZoomToolbar: React.FC<ZoomToolbarProps> = ({
	scale,
	onZoomChange,
	onZoomToFit,
}) => {
	const zoomPercentage = Math.round(scale * 100);

	// Find the next zoom level up
	const handleZoomIn = () => {
		const nextLevel = ZOOM_LEVELS.find((level) => level > scale + 0.001);
		if (nextLevel !== undefined) {
			onZoomChange(nextLevel);
		}
	};

	// Find the next zoom level down
	const handleZoomOut = () => {
		// Reverse the array to find the first level below current scale
		const nextLevel = [...ZOOM_LEVELS]
			.reverse()
			.find((level) => level < scale - 0.001);
		if (nextLevel !== undefined) {
			onZoomChange(nextLevel);
		}
	};

	const minZoom = ZOOM_LEVELS[0];
	const maxZoom = ZOOM_LEVELS[ZOOM_LEVELS.length - 1];
	const canZoomOut = scale > minZoom + 0.001;
	const canZoomIn = scale < maxZoom - 0.001;

	return (
		<div
			style={{
				position: "absolute",
				bottom: 16,
				right: 16,
				display: "flex",
				alignItems: "center",
				gap: 2,
				padding: "4px 8px",
				backgroundColor: "#fff",
				borderRadius: 8,
				boxShadow: "0 2px 8px rgba(0, 0, 0, 0.15)",
				zIndex: 100,
			}}
		>
			{/* Zoom out button */}
			<ZoomButton
				onClick={handleZoomOut}
				disabled={!canZoomOut}
				title="Zoom out"
			>
				<ZoomOutIcon size={16} />
			</ZoomButton>

			{/* Zoom percentage */}
			<span
				style={{
					minWidth: 45,
					textAlign: "center",
					fontSize: 13,
					fontWeight: 500,
					color: "#333",
					userSelect: "none",
				}}
			>
				{zoomPercentage}%
			</span>

			{/* Zoom in button */}
			<ZoomButton onClick={handleZoomIn} disabled={!canZoomIn} title="Zoom in">
				<ZoomInIcon size={16} />
			</ZoomButton>

			{/* Divider */}
			<div
				style={{
					width: 1,
					height: 20,
					backgroundColor: "#e0e0e0",
					margin: "0 4px",
				}}
			/>

			{/* Zoom to fit button */}
			<ZoomButton onClick={onZoomToFit} title="Zoom to fit (fit all content)">
				<ZoomToFitIcon size={16} />
			</ZoomButton>
		</div>
	);
};
