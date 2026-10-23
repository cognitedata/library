/**
 * Bounds helpers for viewport queries
 */

import type { Bounds, CanvasNode, Point } from "./types";

/**
 * Check if two bounds intersect
 */
export function boundsIntersect(a: Bounds, b: Bounds): boolean {
	return (
		a.x < b.x + b.width &&
		a.x + a.width > b.x &&
		a.y < b.y + b.height &&
		a.y + a.height > b.y
	);
}

/**
 * Check if a point is inside bounds
 */
export function pointInBounds(point: Point, bounds: Bounds): boolean {
	return (
		point.x >= bounds.x &&
		point.x <= bounds.x + bounds.width &&
		point.y >= bounds.y &&
		point.y <= bounds.y + bounds.height
	);
}

/**
 * Convert a node to bounds
 */
export function nodeToBounds(node: CanvasNode): Bounds {
	return {
		x: node.position.x,
		y: node.position.y,
		width: node.size.width,
		height: node.size.height,
	};
}
