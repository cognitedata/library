/**
 * SVG Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import {
	SvgNode,
	type SvgNodeData,
	type SvgPath,
	SvgPlaceholder,
} from "./SvgNode";

const SvgIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<path d="M12 2L2 7l10 5 10-5-10-5z" />
		<path d="M2 17l10 5 10-5" />
		<path d="M2 12l10 5 10-5" />
	</svg>
);

export const svgPlugin: NodeTypePlugin<SvgNodeData> = {
	type: "svg",
	displayName: "SVG",
	component: SvgNode,
	placeholder: SvgPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 20, height: 20 },
	},
	connectable: true,
	defaultSize: { width: 100, height: 100 },
	defaultData: () => ({
		paths: [],
		viewBox: "0 0 100 100",
		preserveAspectRatio: "xMidYMid meet",
		backgroundColor: "transparent",
	}),
	icon: SvgIcon,
};

export { SvgNode, type SvgNodeData, type SvgPath, SvgPlaceholder };
