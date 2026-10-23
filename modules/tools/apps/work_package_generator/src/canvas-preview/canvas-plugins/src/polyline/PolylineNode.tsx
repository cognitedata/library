/**
 * Polyline Node Component
 * Multi-segment line with customizable style
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
	Point,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useId } from "react";

export type PolylineEndType = "none" | "arrow";

export interface PolylineNodeData {
	points: Point[];
	stroke?: string;
	strokeWidth?: number;
	strokeDasharray?: string;
	startEndType?: PolylineEndType;
	endEndType?: PolylineEndType;
}

const ArrowMarker: React.FC<{ id: string; color: string }> = ({
	id,
	color,
}) => (
	<marker
		id={id}
		markerWidth="10"
		markerHeight="10"
		refX="9"
		refY="3"
		orient="auto"
		markerUnits="strokeWidth"
	>
		<path d="M0,0 L0,6 L9,3 z" fill={color} />
	</marker>
);

export const PolylineNode: React.FC<NodeComponentProps<PolylineNodeData>> = ({
	data,
	size,
	isSelected,
}) => {
	// Use React's useId for stable unique ID generation (must be called before any early returns)
	const reactId = useId();
	const markerId = `arrow-${reactId.replace(/:/g, "-")}`;

	const stroke = data.stroke || "#000000";
	const strokeWidth = data.strokeWidth || 2;
	const points = data.points || [];

	if (points.length < 2) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					color: "#999",
					fontSize: 12,
				}}
			>
				Insufficient points
			</div>
		);
	}

	// Create SVG path from points
	const pathData = points
		.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`)
		.join(" ");

	return (
		<svg
			width={size.width}
			height={size.height}
			style={{
				position: "absolute",
				left: 0,
				top: 0,
				overflow: "visible",
				pointerEvents: "none",
			}}
		>
			<defs>
				{(data.startEndType === "arrow" || data.endEndType === "arrow") && (
					<ArrowMarker id={markerId} color={stroke} />
				)}
			</defs>

			<path
				d={pathData}
				fill="none"
				stroke={stroke}
				strokeWidth={strokeWidth}
				strokeDasharray={data.strokeDasharray}
				strokeLinecap="round"
				strokeLinejoin="round"
				markerStart={
					data.startEndType === "arrow" ? `url(#${markerId})` : undefined
				}
				markerEnd={
					data.endEndType === "arrow" ? `url(#${markerId})` : undefined
				}
				style={{ pointerEvents: "stroke" }}
			/>

			{/* Vertex markers for selected state */}
			{isSelected &&
				points.map((p, i) => (
					<circle
						key={i}
						cx={p.x}
						cy={p.y}
						r={4}
						fill="#fff"
						stroke={stroke}
						strokeWidth={2}
					/>
				))}
		</svg>
	);
};

export const PolylinePlaceholder: React.FC<
	PlaceholderProps<PolylineNodeData>
> = ({ data, size }) => {
	const stroke = data.stroke || "#000000";
	const points = data.points || [];

	if (points.length < 2) {
		return (
			<div
				style={{
					width: size.width,
					height: size.height,
					backgroundColor: "#f5f5f5",
					borderRadius: 4,
				}}
			/>
		);
	}

	const pathData = points
		.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`)
		.join(" ");

	return (
		<svg
			width={size.width}
			height={size.height}
			style={{ overflow: "visible" }}
		>
			<path
				d={pathData}
				fill="none"
				stroke={stroke}
				strokeWidth={data.strokeWidth || 2}
				strokeOpacity={0.5}
			/>
		</svg>
	);
};
