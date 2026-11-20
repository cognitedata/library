/**
 * Polyline Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import {
	type PolylineEndType,
	PolylineNode,
	type PolylineNodeData,
	PolylinePlaceholder,
} from "./PolylineNode";

const PolylineIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<polyline points="4,18 8,10 14,14 20,6" />
	</svg>
);

export const polylinePlugin: NodeTypePlugin<PolylineNodeData> = {
	type: "polyline",
	displayName: "Polyline",
	component: PolylineNode,
	placeholder: PolylinePlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: false,
		vertical: false,
	},
	connectable: true,
	defaultSize: { width: 100, height: 100 },
	defaultData: () => ({
		points: [],
		stroke: "#000000",
		strokeWidth: 2,
		startEndType: "none",
		endEndType: "none",
	}),
	icon: PolylineIcon,
};

export {
	type PolylineEndType,
	PolylineNode,
	type PolylineNodeData,
	PolylinePlaceholder,
};
