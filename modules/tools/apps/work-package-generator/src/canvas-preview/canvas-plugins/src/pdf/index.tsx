/**
 * PDF Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";
import type React from "react";

import { PDFNode, type PDFNodeData, PDFPlaceholder } from "./PDFNode";

const PDFIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
		<polyline points="14,2 14,8 20,8" />
		<line x1="16" y1="13" x2="8" y2="13" />
		<line x1="16" y1="17" x2="8" y2="17" />
	</svg>
);

export const pdfPlugin: NodeTypePlugin<PDFNodeData> = {
	type: "pdf",
	displayName: "PDF Document",
	component: PDFNode,
	placeholder: PDFPlaceholder,
	stayMounted: true, // Always show PDF content
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "lock", // Lock aspect ratio to preserve PDF proportions
		minSize: { width: 100, height: 100 },
	},
	connectable: true,
	defaultSize: { width: 400, height: 500 }, // Initial size, will be updated when PDF loads
	defaultData: () => ({ url: "", page: 1 }),
	icon: PDFIcon,
};

export { PDFNode, type PDFNodeData, PDFPlaceholder };
