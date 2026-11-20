/**
 * Comment Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import {
	CommentNode,
	type CommentNodeData,
	CommentPlaceholder,
} from "./CommentNode";

const CommentIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />
	</svg>
);

export const commentPlugin: NodeTypePlugin<CommentNodeData> = {
	type: "comment",
	displayName: "Comment",
	component: CommentNode,
	placeholder: CommentPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 150, height: 100 },
	},
	connectable: true,
	defaultSize: { width: 250, height: 150 },
	defaultData: () => ({
		text: "",
		author: "User",
		timestamp: new Date().toISOString(),
		resolved: false,
		backgroundColor: "#4a67fb",
		textColor: "#ffffff",
	}),
	icon: CommentIcon,
};

export { CommentNode, type CommentNodeData, CommentPlaceholder };
