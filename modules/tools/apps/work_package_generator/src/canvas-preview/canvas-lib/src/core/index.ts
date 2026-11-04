/**
 * Core library exports
 */

// Downsampling algorithms
export {
	adaptiveDownsample,
	type DataPoint,
	type DownsampleMode,
	downsample,
	lttbDownsample,
	minMaxDownsample,
} from "./downsampling";
// Hit testing
export {
	boxSelect,
	type HitArea,
	type HitTestResult,
	hitTestNode,
	hitTestNodes,
	hitTestTopNode,
} from "./hit-testing";
// Spatial indexing
export {
	boundsIntersect,
	nodeToBounds,
	pointInBounds,
} from "./spatial-index";
// State management
export { generateId, getNodeById } from "./state";

// Coordinate transforms
export {
	distance,
	fitBoundsInView,
	getAbsoluteBounds,
	getAbsolutePosition,
	getAbsoluteSize,
	getAllNodesBounds,
	getBoundsFromPoints,
	getChildren,
	getDescendantIds,
	getDescendants,
	getExtendedBounds,
	// Parent-child hierarchy utilities
	getNodeById as getNodeByIdFromArray,
	// Viewport culling utilities
	getViewportBounds,
	getViewportTransform,
	isAncestor,
	isPointInBounds,
	midpoint,
	pan,
	rotatePointAroundCenter,
	screenToCanvas,
	snapToAngle,
	toRelativePosition,
	zoomAtPoint,
} from "./transforms";
// Types
export type {
	Bounds,
	BuiltInToolId,
	CanvasMode,
	CanvasNode,
	CanvasState,
	Connection,
	NodeComponentProps,
	NodeTypePlugin,
	PlaceholderProps,
	PluginRegistry,
	Point,
	RenderToolbarFn,
	ResizeConfig,
	SelectionOverlayProps,
	Size,
	ToolbarContext,
	ToolbarProps,
	ToolDrawResult,
	ToolId,
	ToolModifiers,
	ToolPlugin,
	TransitionCallback,
	TransitionResult,
	ViewportState,
} from "./types";
// Built-in tool ID constants
export { BUILT_IN_TOOL_IDS } from "./types";

// Viewport signals (for components to subscribe to zoom/viewport changes)
export {
	type FidelityLevel,
	type FidelitySettings,
	fidelityLevel,
	fidelitySettings,
	isInteracting,
	isPanning,
	isZooming,
	qualityMultiplier,
	setDownsampleMode,
	setFidelitySettings,
	setForceHighFidelity,
	setMaxDataPoints,
	setQualityBias,
	stableScale,
	updatePanning,
	updateScale,
	updateViewportBounds,
	type VisibilityZone,
	viewportBounds,
	viewportScale,
	virtualBounds,
} from "./viewportSignals";
