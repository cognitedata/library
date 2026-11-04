/**
 * Text Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";
import type React from "react";

import { TextNode, type TextNodeData, TextPlaceholder } from "./TextNode";

const TextIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<path d="M4 7V4h16v3" />
		<path d="M9 20h6" />
		<path d="M12 4v16" />
	</svg>
);

export const textPlugin: NodeTypePlugin<TextNodeData> = {
	type: "text",
	displayName: "Text",
	component: TextNode,
	placeholder: TextPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 50, height: 30 },
	},
	connectable: true,
	defaultSize: { width: 200, height: 100 },
	defaultData: () => ({
		content: "",
		fontSize: 16,
		fontFamily: "sans-serif",
		fontWeight: "normal",
		fontStyle: "normal",
		textAlign: "left",
		color: "#000000",
		backgroundColor: "transparent",
	}),
	icon: TextIcon,
};

export { TextNode, type TextNodeData, TextPlaceholder };
