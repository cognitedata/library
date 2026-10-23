/**
 * Shape Node Component
 * Renders basic shapes (rectangle, ellipse, line)
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
	Point,
} from "@dune-canvas/canvas-lib";
import type React from "react";

// ============================================================================
// Types
// ============================================================================

export interface ShapeNodeData {
	shape: "rectangle" | "ellipse" | "line";
	fill?: string;
	stroke?: string;
	strokeWidth?: number;
	points?: Point[]; // For line shapes
}

// ============================================================================
// Component
// ============================================================================

export const ShapeNode: React.FC<NodeComponentProps<ShapeNodeData>> = ({
	data,
	size,
}) => {
	const {
		shape,
		fill = "transparent",
		stroke = "#000000",
		strokeWidth = 2,
		points,
	} = data;

	const renderShape = () => {
		switch (shape) {
			case "rectangle":
				return (
					<rect
						x={strokeWidth / 2}
						y={strokeWidth / 2}
						width={size.width - strokeWidth}
						height={size.height - strokeWidth}
						fill={fill}
						stroke={stroke}
						strokeWidth={strokeWidth}
					/>
				);

			case "ellipse":
				return (
					<ellipse
						cx={size.width / 2}
						cy={size.height / 2}
						rx={(size.width - strokeWidth) / 2}
						ry={(size.height - strokeWidth) / 2}
						fill={fill}
						stroke={stroke}
						strokeWidth={strokeWidth}
					/>
				);

			case "line":
				if (points && points.length >= 2) {
					return (
						<line
							x1={points[0].x}
							y1={points[0].y}
							x2={points[1].x}
							y2={points[1].y}
							stroke={stroke}
							strokeWidth={strokeWidth}
							strokeLinecap="round"
						/>
					);
				}
				return null;

			default:
				return null;
		}
	};

	return (
		<div
			data-shape={shape}
			style={{
				width: "100%",
				height: "100%",
				pointerEvents: "none",
			}}
		>
			<svg
				width="100%"
				height="100%"
				viewBox={`0 0 ${size.width} ${size.height}`}
				style={{ display: "block" }}
			>
				{renderShape()}
			</svg>
		</div>
	);
};

// ============================================================================
// Placeholder
// ============================================================================

export const ShapePlaceholder: React.FC<PlaceholderProps<ShapeNodeData>> = ({
	data,
	size,
}) => {
	const {
		shape,
		fill = "transparent",
		stroke = "#000",
		strokeWidth = 2,
		points,
	} = data;

	const renderShape = () => {
		switch (shape) {
			case "ellipse":
				return (
					<ellipse
						cx={size.width / 2}
						cy={size.height / 2}
						rx={Math.max(0, (size.width - strokeWidth) / 2)}
						ry={Math.max(0, (size.height - strokeWidth) / 2)}
						fill={fill}
						stroke={stroke}
						strokeWidth={strokeWidth}
					/>
				);
			case "line":
				if (points && points.length >= 2) {
					return (
						<line
							x1={points[0].x}
							y1={points[0].y}
							x2={points[1].x}
							y2={points[1].y}
							stroke={stroke}
							strokeWidth={strokeWidth}
							strokeLinecap="round"
						/>
					);
				}
				return null;
			default:
				return (
					<rect
						x={strokeWidth / 2}
						y={strokeWidth / 2}
						width={Math.max(0, size.width - strokeWidth)}
						height={Math.max(0, size.height - strokeWidth)}
						fill={fill}
						stroke={stroke}
						strokeWidth={strokeWidth}
					/>
				);
		}
	};

	return (
		<div style={{ width: "100%", height: "100%" }}>
			<svg
				width="100%"
				height="100%"
				viewBox={`0 0 ${size.width} ${size.height}`}
				style={{ display: "block" }}
			>
				{renderShape()}
			</svg>
		</div>
	);
};
