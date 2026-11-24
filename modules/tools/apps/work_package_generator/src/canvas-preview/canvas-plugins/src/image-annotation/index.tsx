/**
 * Image Annotation Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import {
	ImageAnnotationNode,
	type ImageAnnotationNodeData,
	ImageAnnotationPlaceholder,
} from "./ImageAnnotationNode";

const ImageAnnotationIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
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
		<circle cx="18" cy="6" r="3" fill="currentColor" />
	</svg>
);

export const imageAnnotationPlugin: NodeTypePlugin<ImageAnnotationNodeData> = {
	type: "image-annotation",
	displayName: "Image Annotation",
	component: ImageAnnotationNode,
	placeholder: ImageAnnotationPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "lock",
		minSize: { width: 20, height: 20 },
	},
	connectable: false, // Annotations typically don't have connections
	defaultSize: { width: 64, height: 64 },
	defaultData: () => ({
		url: "",
		alt: "Annotation",
		tintColor: undefined,
		opacity: 1,
	}),
	icon: ImageAnnotationIcon,
};

export {
	ImageAnnotationNode,
	type ImageAnnotationNodeData,
	ImageAnnotationPlaceholder,
};
