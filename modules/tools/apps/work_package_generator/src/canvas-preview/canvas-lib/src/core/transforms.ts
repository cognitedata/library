/**
 * Coordinate transformation utilities
 * Convert between screen, viewport, and canvas coordinates
 */

import type { Bounds, CanvasNode, Point, Size, ViewportState } from "./types";

// ============================================================================
// Coordinate Transforms
// ============================================================================

/**
 * Convert screen coordinates to canvas coordinates
 */
export function screenToCanvas(
	screenPoint: Point,
	viewport: ViewportState,
	containerOffset: Point = { x: 0, y: 0 }
): Point {
	return {
		x: (screenPoint.x - containerOffset.x - viewport.x) / viewport.scale,
		y: (screenPoint.y - containerOffset.y - viewport.y) / viewport.scale,
	};
}

// ============================================================================
// Viewport Operations
// ============================================================================

/**
 * Calculate new viewport after zooming at a point
 */
export function zoomAtPoint(
	viewport: ViewportState,
	screenPoint: Point,
	newScale: number,
	minScale: number = 0.1,
	maxScale: number = 5
): ViewportState {
	// Clamp scale
	const clampedScale = Math.max(minScale, Math.min(maxScale, newScale));

	if (clampedScale === viewport.scale) {
		return viewport;
	}

	// Calculate the canvas point under the cursor before zoom
	const canvasPoint = screenToCanvas(screenPoint, viewport);

	// Calculate new viewport position to keep the same canvas point under cursor
	const newX = screenPoint.x - canvasPoint.x * clampedScale;
	const newY = screenPoint.y - canvasPoint.y * clampedScale;

	return {
		x: newX,
		y: newY,
		scale: clampedScale,
	};
}

/**
 * Calculate new viewport after panning
 */
export function pan(
	viewport: ViewportState,
	deltaX: number,
	deltaY: number
): ViewportState {
	return {
		...viewport,
		x: viewport.x + deltaX,
		y: viewport.y + deltaY,
	};
}

/**
 * Calculate viewport to fit bounds in view
 */
export function fitBoundsInView(
	bounds: Bounds,
	containerSize: Size,
	padding: number = 50,
	minScale: number = 0.05,
	maxScale: number = 4.0
): ViewportState {
	const availableWidth = containerSize.width - padding * 2;
	const availableHeight = containerSize.height - padding * 2;

	// Prevent division by zero
	if (bounds.width <= 0 || bounds.height <= 0) {
		return { x: 0, y: 0, scale: 1 };
	}

	const scaleX = availableWidth / bounds.width;
	const scaleY = availableHeight / bounds.height;
	// Clamp scale between min and max
	const scale = Math.max(minScale, Math.min(scaleX, scaleY, maxScale));

	// Center the content in the viewport
	const contentCenterX = bounds.x + bounds.width / 2;
	const contentCenterY = bounds.y + bounds.height / 2;

	const x = containerSize.width / 2 - contentCenterX * scale;
	const y = containerSize.height / 2 - contentCenterY * scale;

	return { x, y, scale };
}

/**
 * Get CSS transform string for viewport
 */
export function getViewportTransform(viewport: ViewportState): string {
	return `translate(${viewport.x}px, ${viewport.y}px) scale(${viewport.scale})`;
}

// ============================================================================
// Distance & Geometry
// ============================================================================

/**
 * Calculate distance between two points
 */
export function distance(a: Point, b: Point): number {
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	return Math.sqrt(dx * dx + dy * dy);
}

/**
 * Calculate midpoint between two points
 */
export function midpoint(a: Point, b: Point): Point {
	return {
		x: (a.x + b.x) / 2,
		y: (a.y + b.y) / 2,
	};
}

/**
 * Snap point to angle increments (e.g., 45 degrees)
 */
export function snapToAngle(
	origin: Point,
	point: Point,
	angleIncrement: number
): Point {
	const dx = point.x - origin.x;
	const dy = point.y - origin.y;
	const dist = Math.sqrt(dx * dx + dy * dy);

	const angle = Math.atan2(dy, dx);
	const snappedAngle =
		Math.round(angle / ((angleIncrement * Math.PI) / 180)) *
		((angleIncrement * Math.PI) / 180);

	return {
		x: origin.x + Math.cos(snappedAngle) * dist,
		y: origin.y + Math.sin(snappedAngle) * dist,
	};
}

/**
 * Get bounds from two corner points
 */
export function getBoundsFromPoints(p1: Point, p2: Point): Bounds {
	return {
		x: Math.min(p1.x, p2.x),
		y: Math.min(p1.y, p2.y),
		width: Math.abs(p2.x - p1.x),
		height: Math.abs(p2.y - p1.y),
	};
}

// ============================================================================
// Parent-Child Hierarchy Utilities
// ============================================================================

/**
 * Get a node by ID from the nodes array
 */
export function getNodeById(
	nodes: CanvasNode[],
	id: string
): CanvasNode | undefined {
	return nodes.find((n) => n.id === id);
}

/**
 * Get all direct children of a node
 */
export function getChildren(
	parentId: string,
	nodes: CanvasNode[]
): CanvasNode[] {
	return nodes.filter((n) => n.parentId === parentId);
}

/**
 * Get all descendants of a node (recursive)
 */
export function getDescendants(
	parentId: string,
	nodes: CanvasNode[]
): CanvasNode[] {
	const descendants: CanvasNode[] = [];
	const children = getChildren(parentId, nodes);

	for (const child of children) {
		descendants.push(child);
		descendants.push(...getDescendants(child.id, nodes));
	}

	return descendants;
}

/**
 * Get all descendant IDs of a node (recursive)
 */
export function getDescendantIds(
	parentId: string,
	nodes: CanvasNode[]
): string[] {
	return getDescendants(parentId, nodes).map((n) => n.id);
}

/**
 * Check if a node is an ancestor of another node
 */
export function isAncestor(
	potentialAncestorId: string,
	nodeId: string,
	nodes: CanvasNode[]
): boolean {
	const node = getNodeById(nodes, nodeId);
	if (!node?.parentId) return false;
	if (node.parentId === potentialAncestorId) return true;
	return isAncestor(potentialAncestorId, node.parentId, nodes);
}

/**
 * Check if a point is inside bounds
 */
export function isPointInBounds(point: Point, bounds: Bounds): boolean {
	return (
		point.x >= bounds.x &&
		point.x <= bounds.x + bounds.width &&
		point.y >= bounds.y &&
		point.y <= bounds.y + bounds.height
	);
}

/**
 * Rotate a point around a center by the given angle in degrees
 */
export function rotatePointAroundCenter(
	point: Point,
	center: Point,
	angleDegrees: number
): Point {
	if (angleDegrees === 0) return point;

	const radians = (angleDegrees * Math.PI) / 180;
	const cos = Math.cos(radians);
	const sin = Math.sin(radians);

	const dx = point.x - center.x;
	const dy = point.y - center.y;

	return {
		x: center.x + dx * cos - dy * sin,
		y: center.y + dx * sin + dy * cos,
	};
}

/**
 * Get the absolute position of a node (accounting for parent hierarchy and rotation)
 * For root nodes, returns the node's position directly.
 * For child nodes, calculates position based on parent's position, relative position,
 * and parent's rotation (children rotate with their parent).
 */
export function getAbsolutePosition(
	node: CanvasNode,
	nodes: CanvasNode[]
): Point {
	if (!node.parentId || !node.relativePosition) {
		return node.position;
	}

	const parent = getNodeById(nodes, node.parentId);
	if (!parent) {
		return node.position;
	}

	// Get parent's absolute position and size (recursive for nested hierarchies)
	const parentAbsPos = getAbsolutePosition(parent, nodes);
	const parentAbsSize = getAbsoluteSize(parent, nodes);
	const parentRotation = parent.rotation ?? 0;

	// Calculate child's position relative to parent's unrotated frame
	const childAbsPos = {
		x: parentAbsPos.x + node.relativePosition.x * parentAbsSize.width,
		y: parentAbsPos.y + node.relativePosition.y * parentAbsSize.height,
	};

	// If parent is rotated, rotate child's position around parent's center
	if (parentRotation !== 0) {
		const parentCenter = {
			x: parentAbsPos.x + parentAbsSize.width / 2,
			y: parentAbsPos.y + parentAbsSize.height / 2,
		};
		return rotatePointAroundCenter(childAbsPos, parentCenter, parentRotation);
	}

	return childAbsPos;
}

/**
 * Get the absolute size of a node (accounting for parent hierarchy)
 * For root nodes, returns the node's size directly.
 * For child nodes, calculates size based on parent's size and relative size.
 */
export function getAbsoluteSize(node: CanvasNode, nodes: CanvasNode[]): Size {
	if (!node.parentId || !node.relativeSize) {
		return node.size;
	}

	const parent = getNodeById(nodes, node.parentId);
	if (!parent) {
		return node.size;
	}

	// Get parent's absolute size (recursive for nested hierarchies)
	const parentAbsSize = getAbsoluteSize(parent, nodes);

	return {
		width: node.relativeSize.width * parentAbsSize.width,
		height: node.relativeSize.height * parentAbsSize.height,
	};
}

/**
 * Get the absolute bounds of a node (position + size)
 */
export function getAbsoluteBounds(
	node: CanvasNode,
	nodes: CanvasNode[]
): Bounds {
	const pos = getAbsolutePosition(node, nodes);
	const size = getAbsoluteSize(node, nodes);
	return {
		x: pos.x,
		y: pos.y,
		width: size.width,
		height: size.height,
	};
}

/**
 * Convert an absolute position to a relative position within a parent
 */
export function toRelativePosition(
	absolutePos: Point,
	parentBounds: Bounds
): Point {
	return {
		x: (absolutePos.x - parentBounds.x) / parentBounds.width,
		y: (absolutePos.y - parentBounds.y) / parentBounds.height,
	};
}

// ============================================================================
// Viewport Culling Utilities
// ============================================================================

/**
 * Check if two bounds rectangles intersect.
 */
export function boundsIntersect(a: Bounds, b: Bounds): boolean {
	return !(
		a.x + a.width < b.x ||
		b.x + b.width < a.x ||
		a.y + a.height < b.y ||
		b.y + b.height < a.y
	);
}

/**
 * Calculate the visible viewport bounds in canvas coordinates.
 *
 * @param viewport - Current viewport state (x, y, scale)
 * @param containerSize - Size of the canvas container in screen pixels
 * @returns Bounds in canvas coordinates representing the visible area
 */
export function getViewportBounds(
	viewport: ViewportState,
	containerSize: Size
): Bounds {
	return {
		x: -viewport.x / viewport.scale,
		y: -viewport.y / viewport.scale,
		width: containerSize.width / viewport.scale,
		height: containerSize.height / viewport.scale,
	};
}

/**
 * Calculate extended bounds with a buffer zone.
 * Useful for virtualization - keeping nodes mounted slightly outside the viewport.
 *
 * @param bounds - Original bounds
 * @param bufferMultiplier - How much to extend (1.0 = 100% of original size as buffer)
 * @returns Extended bounds
 */
export function getExtendedBounds(
	bounds: Bounds,
	bufferMultiplier: number = 1
): Bounds {
	const bufferX = bounds.width * bufferMultiplier;
	const bufferY = bounds.height * bufferMultiplier;

	return {
		x: bounds.x - bufferX,
		y: bounds.y - bufferY,
		width: bounds.width + bufferX * 2,
		height: bounds.height + bufferY * 2,
	};
}

/**
 * Calculate the bounding box containing all nodes.
 * Useful for "fit all" viewport calculations.
 *
 * By default, only considers root-level nodes (nodes without a parentId) since
 * child nodes are contained within their parents. This avoids issues with
 * relative positioning calculations expanding the bounds unexpectedly.
 *
 * @param nodes - All canvas nodes
 * @param includeChildren - If true, includes child nodes in bounds calculation (default: false)
 * @returns Bounds containing all nodes, or null if no nodes
 */
export function getAllNodesBounds(
	nodes: CanvasNode[],
	includeChildren: boolean = false
): Bounds | null {
	if (nodes.length === 0) return null;

	// Filter to root-level nodes unless includeChildren is true
	const nodesToConsider = includeChildren
		? nodes
		: nodes.filter((node) => !node.parentId);

	if (nodesToConsider.length === 0) return null;

	// Drop sub-pixel nodes (collapsed stickies, empty placeholders). They sit
	// far from the real content and pull zoom-to-fit off center.
	const measurable = nodesToConsider.filter(
		(node) => node.size.width >= 1 && node.size.height >= 1
	);
	const fitted = measurable.length > 0 ? measurable : nodesToConsider;

	let minX = Infinity;
	let minY = Infinity;
	let maxX = -Infinity;
	let maxY = -Infinity;

	for (const node of fitted) {
		const bounds = getAbsoluteBounds(node, nodes);
		minX = Math.min(minX, bounds.x);
		minY = Math.min(minY, bounds.y);
		maxX = Math.max(maxX, bounds.x + bounds.width);
		maxY = Math.max(maxY, bounds.y + bounds.height);
	}

	return {
		x: minX,
		y: minY,
		width: maxX - minX,
		height: maxY - minY,
	};
}
