/**
 * Timeseries Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import { TimeseriesNode } from "./TimeseriesNode";
import { TimeseriesPlaceholder } from "./TimeseriesPlaceholder";
import type { TimeseriesNodeData } from "./types";

const TimeseriesIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<polyline points="22,12 18,12 15,21 9,3 6,12 2,12" />
	</svg>
);

export const timeseriesPlugin: NodeTypePlugin<TimeseriesNodeData> = {
	type: "timeseries",
	displayName: "Timeseries Chart",
	component: TimeseriesNode,
	placeholder: TimeseriesPlaceholder,
	// Note: Toolbar is now handled by DemoApp/CanvasApp via renderToolbar prop
	stayMounted: false,
	unmountDelay: 1500,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 200, height: 150 },
	},
	connectable: true,
	defaultSize: { width: 400, height: 250 },
	defaultData: () => ({ series: [], label: "Chart" }),
	icon: TimeseriesIcon,
};

export { TimeseriesNode } from "./TimeseriesNode";
export { TimeseriesPlaceholder } from "./TimeseriesPlaceholder";
export type {
	TimeseriesDataPoint,
	TimeseriesNodeData,
	TimeseriesNodeDataDirect,
	TimeseriesNodeDataSource,
} from "./types";
