/**
 * NodeToolbar component
 * Displays a floating toolbar above a selected node
 *
 * The toolbar is positioned in canvas space but rendered at fixed scale
 * so it remains readable regardless of zoom level.
 */

import type React from "react";
import { useEffect, useRef, useState } from "react";
import { getAbsolutePosition } from "../core/transforms";
import type { CanvasNode, Size, ViewportState } from "../core/types";

// ============================================================================
// Common Toolbar Button Component
// ============================================================================

export interface ToolbarButtonProps {
	onClick: () => void;
	title: string;
	children: React.ReactNode;
	disabled?: boolean;
}

export const ToolbarButton: React.FC<ToolbarButtonProps> = ({
	onClick,
	title,
	children,
	disabled = false,
}) => {
	return (
		<button
			onClick={onClick}
			title={title}
			disabled={disabled}
			style={{
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				width: 28,
				height: 28,
				padding: 0,
				border: "none",
				borderRadius: 4,
				backgroundColor: "transparent",
				color: disabled ? "#999" : "#333",
				cursor: disabled ? "not-allowed" : "pointer",
				transition: "background-color 0.15s ease",
			}}
			onMouseEnter={(e) => {
				if (!disabled) {
					e.currentTarget.style.backgroundColor = "#f0f0f0";
				}
			}}
			onMouseLeave={(e) => {
				e.currentTarget.style.backgroundColor = "transparent";
			}}
		>
			{children}
		</button>
	);
};

// ============================================================================
// Common Toolbar Divider
// ============================================================================

export const ToolbarDivider: React.FC = () => (
	<div
		style={{
			width: 1,
			height: 20,
			backgroundColor: "#e0e0e0",
			margin: "0 4px",
		}}
	/>
);

// ============================================================================
// Selection Toolbar Container
// ============================================================================

export interface SelectionToolbarContainerProps {
	selectedNodes: CanvasNode[];
	nodes: CanvasNode[];
	viewport: ViewportState;
	containerSize: Size;
	children: React.ReactNode;
}

/**
 * Container for the selection toolbar
 * Positions the toolbar above the bounding box of all selected nodes
 * Rendered in screen space (outside the canvas transform)
 */
export const SelectionToolbarContainer: React.FC<
	SelectionToolbarContainerProps
> = ({ selectedNodes, nodes, viewport, containerSize, children }) => {
	const toolbarRef = useRef<HTMLDivElement>(null);
	const [toolbarSize, setToolbarSize] = useState<Size>({ width: 0, height: 0 });

	// Measure toolbar size
	useEffect(() => {
		if (toolbarRef.current) {
			const rect = toolbarRef.current.getBoundingClientRect();
			setToolbarSize({ width: rect.width, height: rect.height });
		}
	}, [children]);

	// Don't render if no nodes selected or no toolbar content
	if (selectedNodes.length === 0 || !children) {
		return null;
	}

	// Calculate bounding box of all selected nodes in canvas coordinates
	let minX = Infinity;
	let minY = Infinity;
	let maxX = -Infinity;
	let maxY = -Infinity;

	for (const node of selectedNodes) {
		const { x, y } = getAbsolutePosition(node, nodes);
		const w = node.size.width;
		const h = node.size.height;

		minX = Math.min(minX, x);
		minY = Math.min(minY, y);
		maxX = Math.max(maxX, x + w);
		maxY = Math.max(maxY, y + h);
	}

	// Convert canvas coordinates to screen coordinates
	const screenMinX = minX * viewport.scale + viewport.x;
	const screenMinY = minY * viewport.scale + viewport.y;
	const screenMaxX = maxX * viewport.scale + viewport.x;

	// Position toolbar at center-top of selection bounding box
	const centerX = (screenMinX + screenMaxX) / 2;
	const gap = 12; // Gap between selection and toolbar in pixels
	const topY = screenMinY - gap - toolbarSize.height;

	// Clamp to keep toolbar visible within container
	const clampedX = Math.max(
		toolbarSize.width / 2 + 8,
		Math.min(centerX, containerSize.width - toolbarSize.width / 2 - 8)
	);
	const clampedY = Math.max(8, topY);

	return (
		<div
			ref={toolbarRef}
			style={{
				position: "absolute",
				left: clampedX,
				top: clampedY,
				transform: "translateX(-50%)",
				zIndex: 1000,
				pointerEvents: "auto",
			}}
			onMouseDown={(e) => e.stopPropagation()}
			onClick={(e) => e.stopPropagation()}
		>
			<div
				style={{
					display: "flex",
					alignItems: "center",
					gap: 4,
					padding: "6px 8px",
					backgroundColor: "#ffffff",
					borderRadius: 8,
					boxShadow:
						"0 2px 8px rgba(0, 0, 0, 0.15), 0 0 1px rgba(0, 0, 0, 0.1)",
					border: "1px solid rgba(0, 0, 0, 0.08)",
				}}
			>
				{children}
			</div>
		</div>
	);
};
