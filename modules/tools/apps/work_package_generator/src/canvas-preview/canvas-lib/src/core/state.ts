/**
 * Canvas state helpers
 */

import type { CanvasNode, CanvasState } from "./types";

export function getNodeById(
	state: CanvasState,
	id: string
): CanvasNode | undefined {
	return state.nodes.find((n) => n.id === id);
}

export function generateId(): string {
	return `node-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}
