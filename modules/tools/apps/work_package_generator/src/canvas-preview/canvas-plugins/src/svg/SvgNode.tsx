/**
 * SVG Node Component
 * Renders custom SVG paths
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";

export interface SvgPath {
	d: string;
	fill?: string;
	stroke?: string;
	strokeWidth?: number;
}

export interface SvgNodeData {
	paths: SvgPath[];
	viewBox?: string;
	preserveAspectRatio?: string;
	backgroundColor?: string;
}

export const SvgNode: React.FC<NodeComponentProps<SvgNodeData>> = ({
	data,
	size,
	isSelected,
}) => {
	const viewBox = data.viewBox || `0 0 ${size.width} ${size.height}`;
	const preserveAspectRatio = data.preserveAspectRatio || "xMidYMid meet";

	if (!data.paths || data.paths.length === 0) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					backgroundColor: data.backgroundColor || "#f5f5f5",
					border: "1px dashed #ccc",
					borderRadius: 4,
					color: "#999",
					fontSize: 12,
				}}
			>
				No SVG paths
			</div>
		);
	}

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor: data.backgroundColor || "transparent",
				borderRadius: 4,
				overflow: "hidden",
			}}
		>
			<svg
				width="100%"
				height="100%"
				viewBox={viewBox}
				preserveAspectRatio={preserveAspectRatio}
				style={{ display: "block" }}
			>
				{data.paths.map((path, index) => (
					<path
						key={index}
						d={path.d}
						fill={path.fill || "none"}
						stroke={path.stroke || "#000000"}
						strokeWidth={path.strokeWidth || 1}
						strokeLinecap="round"
						strokeLinejoin="round"
					/>
				))}
			</svg>
		</div>
	);
};

export const SvgPlaceholder: React.FC<PlaceholderProps<SvgNodeData>> = ({
	data,
	size,
}) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.backgroundColor || "#f5f5f5",
				borderRadius: 4,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
			}}
		>
			<svg
				width={32}
				height={32}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#999"
				strokeWidth="1.5"
			>
				<path d="M12 2L2 7l10 5 10-5-10-5z" />
				<path d="M2 17l10 5 10-5" />
				<path d="M2 12l10 5 10-5" />
			</svg>
		</div>
	);
};
