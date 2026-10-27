/** Shared font stack used across timeseries components */
export const FONT_FAMILY =
	'-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';

/** Height of the draggable header bar above the chart */
export const HEADER_HEIGHT = 28;

/** Mouse movement beyond this threshold (px) is treated as a drag, not a click */
export const DRAG_THRESHOLD = 4;

/** Tooltip layout constants */
export const TOOLTIP_WIDTH = 150;
export const TOOLTIP_GAP = 16;
export const TIMESTAMP_HALF_WIDTH = 110;
export const TIMESTAMP_FULL_WIDTH = 228;

/** Shared style for loading/error/empty states */
export const stateContainerStyle: React.CSSProperties = {
	width: "100%",
	height: "100%",
	backgroundColor: "#f9f9f9",
	border: "1px solid #ddd",
	display: "flex",
	alignItems: "center",
	justifyContent: "center",
	flexDirection: "column",
	gap: 8,
	padding: 16,
	overflow: "hidden",
	boxSizing: "border-box",
};
