/**
 * Cause Map graph helpers
 * Based on the source implementation in unified-file-viewer
 */

import type { CanvasNode, Connection } from "@dune-canvas/canvas-lib";

export interface CauseMapNode {
	id: string;
	width: number;
	height: number;
	x: number;
	y: number;
}

export interface CauseMapEdge {
	fromId: string;
	toId: string;
}

/**
 * Extract cause map nodes and edges from canvas state.
 * This filters out non-cause-map nodes and finds connections between them.
 */
export const extractCauseMapGraph = (
	nodes: CanvasNode[],
	connections: Connection[]
): { causeMapNodes: CauseMapNode[]; causeMapEdges: CauseMapEdge[] } => {
	// Filter to only cause map nodes
	const causeMapNodes = nodes
		.filter((node) => node.type === "cause-map")
		.map((node) => ({
			id: node.id,
			width: node.size.width,
			height: node.size.height,
			x: node.position.x,
			y: node.position.y,
		}));

	const causeMapNodeIds = new Set(causeMapNodes.map((n) => n.id));

	// Filter connections to only those between cause map nodes
	const causeMapEdges = connections
		.filter(
			(conn) => causeMapNodeIds.has(conn.from) && causeMapNodeIds.has(conn.to)
		)
		.map((conn) => ({
			fromId: conn.from,
			toId: conn.to,
		}));

	return { causeMapNodes, causeMapEdges };
};

/**
 * Get all incoming edges for a node (edges pointing TO this node).
 */
export const getIncomingEdges = (
	nodeId: string,
	edges: CauseMapEdge[]
): CauseMapEdge[] => {
	return edges.filter((edge) => edge.toId === nodeId);
};

/**
 * Get all outgoing edges from a node (edges pointing FROM this node).
 */
export const getOutgoingEdges = (
	nodeId: string,
	edges: CauseMapEdge[]
): CauseMapEdge[] => {
	return edges.filter((edge) => edge.fromId === nodeId);
};

/**
 * Check if a node is a root node (has no incoming edges).
 */
export const isRootNode = (nodeId: string, edges: CauseMapEdge[]): boolean => {
	return getIncomingEdges(nodeId, edges).length === 0;
};

/**
 * Check if a node has multiple parents (more than one incoming edge).
 */
export const hasMultipleParents = (
	nodeId: string,
	edges: CauseMapEdge[]
): boolean => {
	return getIncomingEdges(nodeId, edges).length > 1;
};

/**
 * Get parent node IDs for a given node.
 */
export const getParentIds = (
	nodeId: string,
	edges: CauseMapEdge[]
): string[] => {
	return getIncomingEdges(nodeId, edges).map((edge) => edge.fromId);
};

/**
 * Get child node IDs for a given node.
 */
export const getChildIds = (
	nodeId: string,
	edges: CauseMapEdge[]
): string[] => {
	return getOutgoingEdges(nodeId, edges).map((edge) => edge.toId);
};
