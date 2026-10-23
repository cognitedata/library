/**
 * Cause Map Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";
import type React from "react";

import { CauseMapAnchors } from "./CauseMapAnchors";
import {
	CauseMapNode,
	type CauseMapNodeData,
	type CauseMapNodeStatus,
	CauseMapPlaceholder,
	STATUS_COLORS,
} from "./CauseMapNode";

const CauseMapIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="3" y="3" width="7" height="7" rx="1" />
		<rect x="14" y="3" width="7" height="7" rx="1" />
		<rect x="8" y="14" width="8" height="7" rx="1" />
		<path d="M6.5 10v2.5a1 1 0 001 1h9a1 1 0 001-1V10" />
		<path d="M12 13.5V14" />
	</svg>
);

export const causeMapPlugin: NodeTypePlugin<CauseMapNodeData> = {
	type: "cause-map",
	displayName: "Cause Map Node",
	component: CauseMapNode,
	placeholder: CauseMapPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 80, height: 60 },
	},
	connectable: true,
	defaultSize: { width: 150, height: 100 },

	// Selection overlay for adding connected nodes via anchors
	selectionOverlay: CauseMapAnchors,
	defaultData: () => ({
		text: "",
		status: "pending",
		backgroundColor: undefined,
		borderColor: undefined,
		textColor: "#333",
		fontSize: 14,
	}),
	icon: CauseMapIcon,
};

export { CauseMapAnchors } from "./CauseMapAnchors";
export * from "./causeMapLayout";
export * from "./constants";
export {
	CauseMapNode,
	type CauseMapNodeData,
	type CauseMapNodeStatus,
	CauseMapPlaceholder,
	STATUS_COLORS,
};
