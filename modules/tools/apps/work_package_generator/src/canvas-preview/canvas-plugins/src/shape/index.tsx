/**
 * Shape Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";
import type React from "react";

import { ShapeNode, type ShapeNodeData, ShapePlaceholder } from "./ShapeNode";

const ShapeIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="3" y="3" width="18" height="18" rx="2" />
	</svg>
);

export const shapePlugin: NodeTypePlugin<ShapeNodeData> = {
	type: "shape",
	displayName: "Shape",
	component: ShapeNode,
	placeholder: ShapePlaceholder,
	stayMounted: true, // Shapes are lightweight
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 10, height: 10 },
	},
	connectable: true,
	defaultSize: { width: 100, height: 100 },
	defaultData: () => ({
		shape: "rectangle",
		fill: "transparent",
		stroke: "#000000",
		strokeWidth: 2,
	}),
	icon: ShapeIcon,
};

export { ShapeNode, type ShapeNodeData, ShapePlaceholder };
