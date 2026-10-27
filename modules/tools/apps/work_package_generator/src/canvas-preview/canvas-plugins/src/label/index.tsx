/**
 * Label Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import { LabelNode, type LabelNodeData, LabelPlaceholder } from "./LabelNode";

const LabelIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="3" y="7" width="18" height="10" rx="2" />
		<path d="M7 12h10" />
	</svg>
);

export const labelPlugin: NodeTypePlugin<LabelNodeData> = {
	type: "label",
	displayName: "Label",
	component: LabelNode,
	placeholder: LabelPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 40, height: 24 },
	},
	connectable: true,
	defaultSize: { width: 120, height: 36 },
	defaultData: () => ({
		text: "",
		fontSize: 14,
		fontWeight: "normal",
		textColor: "#333",
		backgroundColor: "#ffffff",
		borderColor: "#e0e0e0",
		borderWidth: 1,
		borderRadius: 4,
		padding: 8,
	}),
	icon: LabelIcon,
};

export { LabelNode, type LabelNodeData, LabelPlaceholder };
