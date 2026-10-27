/**
import React from 'react';
 * Sticky Note Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import {
	STICKY_COLORS,
	StickyNode,
	type StickyNodeData,
	StickyPlaceholder,
} from "./StickyNode";

const StickyIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<path d="M15.5 3H5a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2V8.5L15.5 3z" />
		<polyline points="15,3 15,9 21,9" />
	</svg>
);

export const stickyPlugin: NodeTypePlugin<StickyNodeData> = {
	type: "sticky",
	displayName: "Sticky Note",
	component: StickyNode,
	placeholder: StickyPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 100, height: 100 },
	},
	connectable: true,
	defaultSize: { width: 200, height: 200 },
	defaultData: () => ({
		content: "",
		backgroundColor: STICKY_COLORS[0],
		textColor: "#333",
		fontSize: 14,
	}),
	icon: StickyIcon,
};

export { STICKY_COLORS, StickyNode, type StickyNodeData, StickyPlaceholder };
