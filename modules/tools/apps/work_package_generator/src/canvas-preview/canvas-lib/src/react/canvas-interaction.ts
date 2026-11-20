import type { ReactNode } from "react";
import type { HitArea, Point, ViewportState } from "../core";

export type InteractionMode =
	| "idle"
	| "selecting"
	| "dragging"
	| "resizing"
	| "drawing"
	| "panning";

export interface DragState {
	nodeId: string;
	startPosition: Point;
	startMousePosition: Point;
}

export interface ResizeState {
	nodeId: string;
	handle: HitArea;
	startBounds: { x: number; y: number; width: number; height: number };
	startMousePosition: Point;
}

export interface PanState {
	startMousePosition: Point;
	startViewport: ViewportState;
}

export interface MultiClickState {
	points: Point[];
	toolState: unknown;
}

export interface InteractionState {
	mode: InteractionMode;
	drawStartPoint: Point | null;
	selectionBox: { start: Point; end: Point } | null;
	dragState: DragState[] | null;
	resizeState: ResizeState | null;
	panState: PanState | null;
	multiClickState: MultiClickState | null;
	drawToolState: unknown;
	drawPreview: ReactNode | null;
}
