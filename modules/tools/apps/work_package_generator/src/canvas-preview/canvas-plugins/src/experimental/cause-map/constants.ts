/**
 * Constants for Cause Map Anchors
 * Based on the source implementation in unified-file-viewer
 */

// Anchor positions
export const ANCHOR_POSITIONS = [
	"top-center",
	"middle-right",
	"middle-left",
	"bottom-center",
] as const;

export type AnchorPosition = (typeof ANCHOR_POSITIONS)[number];

// Only show left/right anchors (for adding parent/child nodes)
export const LEFT_RIGHT_ANCHORS: AnchorPosition[] = [
	"middle-right",
	"middle-left",
];

// All anchors (including top/bottom for siblings)
export const ALL_ANCHORS: AnchorPosition[] = [
	"top-center",
	"middle-right",
	"middle-left",
	"bottom-center",
];

// Anchor styling
export const ANCHOR_STYLE = {
	radius: 10,
	hoveredRadius: 15,
	fill: "rgba(74, 103, 251, 1)",
	stroke: "rgba(255, 255, 255, 1)",
	strokeWidth: 2,
	shadowColor: "rgba(0, 0, 0, 0.3)",
	shadowBlur: 4,
	crossStrokeWidth: 2,
	crossLength: 8,
};

// Padding between anchor and node edge
export const ANCHOR_PADDING = {
	default: 40,
	min: 20,
	max: 200,
};

// Default size for new cause map nodes created via anchor
export const TARGET_NODE_SIZE = {
	width: 150,
	height: 100,
};

// Layout spacing defaults
