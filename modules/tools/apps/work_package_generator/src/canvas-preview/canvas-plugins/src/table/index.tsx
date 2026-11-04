/**
 * Table Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import { TableNode, type TableNodeData, TablePlaceholder } from "./TableNode";

const TableIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="3" y="3" width="18" height="18" rx="2" />
		<line x1="3" y1="9" x2="21" y2="9" />
		<line x1="3" y1="15" x2="21" y2="15" />
		<line x1="9" y1="3" x2="9" y2="21" />
		<line x1="15" y1="3" x2="15" y2="21" />
	</svg>
);

export const tablePlugin: NodeTypePlugin<TableNodeData> = {
	type: "table",
	displayName: "Data Table",
	component: TableNode,
	placeholder: TablePlaceholder,
	stayMounted: true, // Tables are lightweight, keep mounted
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 200, height: 150 },
	},
	connectable: true,
	defaultSize: { width: 400, height: 300 },
	defaultData: () => ({
		source: {
			type: "direct" as const,
			columns: [
				{ key: "id", label: "ID" },
				{ key: "name", label: "Name" },
			],
			rows: [],
		},
		title: "Table",
	}),
	icon: TableIcon,
};

export {
	type PropertyConfig,
	type TableColumn,
	TableNode,
	type TableNodeData,
	TablePlaceholder,
} from "./TableNode";
