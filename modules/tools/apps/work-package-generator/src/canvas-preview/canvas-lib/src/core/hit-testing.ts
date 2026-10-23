/**
 * Hit testing utilities for determining what's under the cursor
 */

import { nodeToBounds, pointInBounds } from "./spatial-index";
import { getAbsolutePosition, getAbsoluteSize } from "./transforms";
import type { Bounds, CanvasNode, Point } from "./types";

// ============================================================================
// Hit Test Results
// ============================================================================

export type HitArea =
	| "body"
	| "resize-n"
	| "resize-s"
	| "resize-e"
	| "resize-w"
	| "resize-ne"
	| "resize-nw"
	| "resize-se"
	| "resize-sw";

export interface HitTestResult {
	nodeId: string;
	area: HitArea;
}

// ============================================================================
// Hit Testing Functions
// ============================================================================

const HANDLE_SIZE = 8;

/**
 * Get absolute bounds for a node, accounting for parent-child hierarchy
 */
function getNodeAbsoluteBounds(
	node: CanvasNode,
	allNodes: CanvasNode[]
): Bounds {
	const pos = getAbsolutePosition(node, allNodes);
	const size = getAbsoluteSize(node, allNodes);
	return {
		x: pos.x,
		y: pos.y,
		width: size.width,
		height: size.height,
	};
}

/**
 * Test if a point hits a node, returns the hit area
 * @param allNodes - All nodes in the canvas (needed for computing absolute positions of child nodes)
 * @param cornersOnly - If true, only corner resize handles are considered (for uniform scaling)
 */
export function hitTestNode(
	point: Point,
	node: CanvasNode,
	scale: number = 1,
	allNodes?: CanvasNode[],
	cornersOnly: boolean = false
): HitTestResult | null {
	// Use absolute bounds if allNodes is provided, otherwise use node's direct position
	const bounds = allNodes
		? getNodeAbsoluteBounds(node, allNodes)
		: nodeToBounds(node);
	const handleSize = HANDLE_SIZE / scale;

	// Check resize handles first (they're on top)
	const resizeHandle = hitTestResizeHandles(
		point,
		bounds,
		handleSize,
		cornersOnly
	);
	if (resizeHandle) {
		return { nodeId: node.id, area: resizeHandle };
	}

	// Check body
	if (pointInBounds(point, bounds)) {
		return { nodeId: node.id, area: "body" };
	}

	return null;
}

/**
 * Test if a point hits any resize handle
 * @param cornersOnly - If true, only corner handles are tested (for uniform scaling)
 */
function hitTestResizeHandles(
	point: Point,
	bounds: Bounds,
	handleSize: number,
	cornersOnly: boolean = false
): HitArea | null {
	// Corner handles (always present for resizable nodes)
	const cornerHandles: Array<{ area: HitArea; x: number; y: number }> = [
		{ area: "resize-nw", x: bounds.x, y: bounds.y },
		{ area: "resize-ne", x: bounds.x + bounds.width, y: bounds.y },
		{
			area: "resize-se",
			x: bounds.x + bounds.width,
			y: bounds.y + bounds.height,
		},
		{ area: "resize-sw", x: bounds.x, y: bounds.y + bounds.height },
	];

	// Edge handles (only for non-uniform scaling)
	const edgeHandles: Array<{ area: HitArea; x: number; y: number }> = [
		{ area: "resize-n", x: bounds.x + bounds.width / 2, y: bounds.y },
		{
			area: "resize-e",
			x: bounds.x + bounds.width,
			y: bounds.y + bounds.height / 2,
		},
		{
			area: "resize-s",
			x: bounds.x + bounds.width / 2,
			y: bounds.y + bounds.height,
		},
		{ area: "resize-w", x: bounds.x, y: bounds.y + bounds.height / 2 },
	];

	const handles = cornersOnly
		? cornerHandles
		: [...cornerHandles, ...edgeHandles];

	for (const handle of handles) {
		const handleBounds: Bounds = {
			x: handle.x - handleSize / 2,
			y: handle.y - handleSize / 2,
			width: handleSize,
			height: handleSize,
		};
		if (pointInBounds(point, handleBounds)) {
			return handle.area;
		}
	}

	return null;
}

/**
 * Find all nodes at a point, sorted by z-index (highest first)
 * Children are considered to be "above" their parents in hit testing
 * @param getCornersOnly - Optional function to determine if a node should use corners-only hit testing
 */
export function hitTestNodes(
	point: Point,
	nodes: CanvasNode[],
	scale: number = 1,
	getCornersOnly?: (node: CanvasNode) => boolean
): HitTestResult[] {
	const results: HitTestResult[] = [];

	// Sort by z-index descending (top first), with children above parents
	const sortedNodes = [...nodes].sort((a, b) => {
		// Children should be hit-tested before parents (they're visually on top)
		const aIsChildOfB = a.parentId === b.id;
		const bIsChildOfA = b.parentId === a.id;

		if (aIsChildOfB) return -1; // a is a child of b, a comes first
		if (bIsChildOfA) return 1; // b is a child of a, b comes first

		// Otherwise sort by z-index
		return b.zIndex - a.zIndex;
	});

	for (const node of sortedNodes) {
		const cornersOnly = getCornersOnly ? getCornersOnly(node) : false;
		const hit = hitTestNode(point, node, scale, nodes, cornersOnly);
		if (hit) {
			results.push(hit);
		}
	}

	return results;
}

/**
 * Find the topmost node at a point
 * @param getCornersOnly - Optional function to determine if a node should use corners-only hit testing
 */
export function hitTestTopNode(
	point: Point,
	nodes: CanvasNode[],
	scale: number = 1,
	getCornersOnly?: (node: CanvasNode) => boolean
): HitTestResult | null {
	const results = hitTestNodes(point, nodes, scale, getCornersOnly);
	return results.length > 0 ? results[0] : null;
}

/**
 * Find all nodes within a selection box
 * Uses absolute positions for child nodes
 */
export function boxSelect(
	selectionBounds: Bounds,
	nodes: CanvasNode[]
): string[] {
	const selected: string[] = [];

	for (const node of nodes) {
		// Use absolute bounds for child nodes
		const nodeBounds = getNodeAbsoluteBounds(node, nodes);

		// Check if node is fully contained in selection box
		if (
			nodeBounds.x >= selectionBounds.x &&
			nodeBounds.y >= selectionBounds.y &&
			nodeBounds.x + nodeBounds.width <=
				selectionBounds.x + selectionBounds.width &&
			nodeBounds.y + nodeBounds.height <=
				selectionBounds.y + selectionBounds.height
		) {
			selected.push(node.id);
		}
	}

	return selected;
}
