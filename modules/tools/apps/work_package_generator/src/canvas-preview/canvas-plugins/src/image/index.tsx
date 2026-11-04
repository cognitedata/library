/**
 * Image Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";
import type React from "react";

import {
	ImageNode,
	type ImageNodeData,
	type ImageNodeDataDirect,
	ImagePlaceholder,
} from "./ImageNode";

const ImageIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="3" y="3" width="18" height="18" rx="2" />
		<circle cx="8.5" cy="8.5" r="1.5" />
		<path d="M21 15l-5-5L5 21" />
	</svg>
);

export const imagePlugin: NodeTypePlugin<ImageNodeData> = {
	type: "image",
	displayName: "Image",
	component: ImageNode,
	placeholder: ImagePlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "lock",
		minSize: { width: 50, height: 50 },
	},
	connectable: true,
	defaultSize: { width: 200, height: 200 },
	defaultData: () => ({ url: "", objectFit: "contain" }),
	icon: ImageIcon,
};

export {
	ImageNode,
	type ImageNodeData,
	type ImageNodeDataDirect,
	ImagePlaceholder,
};
