/**
 * Core types for the canvas library
 * These types define the contracts for nodes, plugins, tools, and state
 */

// ============================================================================
// Geometry Types
// ============================================================================

export interface Point {
	x: number;
	y: number;
}

export interface Size {
	width: number;
	height: number;
}

export interface Bounds {
	x: number;
	y: number;
	width: number;
	height: number;
}

// ============================================================================
// Node Types
// ============================================================================

export interface CanvasNode<TData = unknown> {
	id: string;
	type: string;
	position: Point;
	size: Size;
	zIndex: number;
	data: TData;
	locked?: boolean;
	rotation?: number; // Rotation in degrees (0-360), defaults to 0
	// Parent-child hierarchy
	parentId?: string; // ID of parent node (undefined = root node)
	relativePosition?: Point; // Position as fraction of parent size (0-1)
	relativeSize?: Size; // Size as fraction of parent size (0-1)
}

export interface Connection {
	id: string;
	from: string;
	to: string;
	fromAnchor?: Point;
	toAnchor?: Point;
	data?: unknown;
}

// ============================================================================
// Viewport & Transform
// ============================================================================

export interface ViewportState {
	x: number;
	y: number;
	scale: number;
}

// ============================================================================
// Canvas State
// ============================================================================

export type CanvasMode = "select" | "draw" | "pan";

/**
 * Known built-in tool identifiers.
 * The (string & {}) escape hatch allows plugin-registered tool IDs
 * (e.g. "create-text", "create-sticky") while preserving autocomplete
 * for the built-in values.
 */
export const BUILT_IN_TOOL_IDS = {
	SELECTION: "selection",
	PAN: "pan",
	RECTANGLE: "rectangle",
	ELLIPSE: "ellipse",
	LINE: "line",
} as const;

export type BuiltInToolId =
	(typeof BUILT_IN_TOOL_IDS)[keyof typeof BUILT_IN_TOOL_IDS];
export type ToolId = BuiltInToolId | (string & {});

export interface CanvasState {
	nodes: CanvasNode[];
	connections: Connection[];
	selection: string[];
	viewport: ViewportState;
	mode: CanvasMode;
	activeTool: ToolId;
	isDrawing: boolean;
	isPanning: boolean;
}

// ============================================================================
// Transition API
// ============================================================================

export interface TransitionResult {
	placeholder: React.ReactNode;
	restoreData?: unknown;
	cleanup?: () => void;
}

/** Callback that accepts a TransitionResult */
export type TransitionResultHandler = (result: TransitionResult) => void;

/**
 * Function that provides a transition result when requested.
 * Called with a handler that should receive the result.
 */
export type TransitionCallback = (
	resultHandler: TransitionResultHandler
) => void;

// ============================================================================
// Node Component Props
// ============================================================================

export interface NodeComponentProps<TData = unknown> {
	data: TData;
	onDataChange: (newData: TData) => void;
	size: Size;
	isSelected: boolean;
	isFocused: boolean;
	canvasScale: number;
	isCanvasPanning: boolean;
	onTransitionOut?: (callback: TransitionCallback) => void;
	onSizeChange?: (newSize: Size) => void;
	restoreData?: unknown;
	readOnly?: boolean;
	rotation?: number; // Current rotation in degrees
}

export interface PlaceholderProps<TData = unknown> {
	data: TData;
	size: Size;
	transitionPlaceholder?: React.ReactNode;
	restoreData?: unknown;
}

// ============================================================================
// Toolbar Types
// ============================================================================

export interface ToolbarProps<TData = unknown> {
	data: TData;
	nodeId: string;
	rotation: number;
	onRotate: (degrees: number) => void;
	onDataChange: (newData: TData) => void;
	canvasScale: number;
}

// ============================================================================
// Selection Overlay Types
// ============================================================================

/**
 * Props passed to a plugin's selection overlay component.
 * The overlay is rendered when a node of this type is selected.
 * The component receives the full selection context and can decide
 * whether to render based on conditions (e.g., only render if exactly
 * one node is selected).
 */
export interface SelectionOverlayProps<TData = unknown> {
	/** The node this overlay is being rendered for */
	node: CanvasNode<TData>;
	/** Current canvas zoom scale */
	canvasScale: number;
	/** Full canvas state including all nodes and connections */
	canvasState: CanvasState;
	/** All currently selected nodes */
	selectedNodes: CanvasNode[];
	/** Callback to update nodes (add, modify, or remove) and optionally change selection */
	onUpdateNodes: (updates: {
		add?: CanvasNode[];
		update?: Array<{ id: string; updates: Partial<CanvasNode> }>;
		remove?: string[];
		/** Node IDs to select after the update */
		select?: string[];
		/** Connections to add */
		addConnections?: Connection[];
	}) => void;
	/** Callback to add connections */
	onAddConnection?: (connection: Connection) => void;
}

// ============================================================================
// Node Type Plugin Contract
// ============================================================================

export interface ResizeConfig {
	horizontal?: boolean;
	vertical?: boolean;
	aspectRatio?: "lock" | "free";
	minSize?: Size;
	maxSize?: Size;
}

export interface NodeTypePlugin<TData = unknown> {
	type: string;
	displayName: string;
	component: React.ComponentType<NodeComponentProps<TData>>;
	placeholder?: React.ComponentType<PlaceholderProps<TData>>;
	stayMounted?: boolean;
	unmountDelay?: number;
	resizable?: ResizeConfig;
	connectable?: boolean;
	connectionAnchors?: (node: CanvasNode<TData>) => Point[];

	validateData?: (data: unknown) => data is TData;
	defaultData?: () => TData;
	defaultSize?: Size;
	icon?: React.ComponentType<{ size?: number }>;

	/**
	 * Optional component to render when this node type is selected.
	 * The overlay receives the full selection context and can render
	 * interactive UI elements like add-node anchors.
	 *
	 * The component should internally check selection conditions
	 * (e.g., only render if exactly one node of this type is selected).
	 */
	selectionOverlay?: React.ComponentType<SelectionOverlayProps<TData>>;
}

// ============================================================================
// Tool Plugin Contract
// ============================================================================

export interface ToolModifiers {
	shift: boolean;
	alt: boolean;
	ctrl: boolean;
	meta: boolean;
}

export interface ToolDrawResult {
	state: unknown;
	preview: React.ReactNode;
}

export interface ToolPlugin {
	id: ToolId;
	displayName: string;
	icon: React.ComponentType<{ size?: number }>;
	shortcut?: string;
	category: "shape" | "annotation" | "connector" | "selection" | "custom";
	cursor?: string;

	// Drag-based tool pattern (existing)
	onDrawStart?: (point: Point, canvasState: CanvasState) => unknown;
	onDrawMove?: (
		point: Point,
		startPoint: Point,
		state: unknown,
		modifiers: ToolModifiers
	) => ToolDrawResult;
	onDrawEnd?: (
		endPoint: Point,
		startPoint: Point,
		state: unknown,
		canvasState: CanvasState
	) => CanvasNode[] | null;

	// Click-to-place pattern (for sticky, comment, text, etc.)
	onClick?: (point: Point, canvasState: CanvasState) => CanvasNode[] | null;

	// Multi-click pattern (for polyline)
	isMultiClick?: boolean;
	onClickStart?: (point: Point, canvasState: CanvasState) => unknown;
	onClickAdd?: (
		point: Point,
		state: unknown,
		modifiers: ToolModifiers
	) => ToolDrawResult;
	onClickComplete?: (
		state: unknown,
		canvasState: CanvasState
	) => CanvasNode[] | null;

	isValidDraw?: (startPoint: Point, endPoint: Point) => boolean;
	constrainPoint?: (
		point: Point,
		startPoint: Point,
		modifiers: ToolModifiers
	) => Point;
}

// ============================================================================
// Registry Types
// ============================================================================

export interface PluginRegistry {
	nodeTypes: Map<string, NodeTypePlugin>;
	tools: Map<ToolId, ToolPlugin>;
}

// ============================================================================
// Canvas Actions
// ============================================================================

// ============================================================================
// Application-Controlled Toolbar Types
// ============================================================================

/**
 * Context passed to the application's renderToolbar function.
 * Provides everything needed to render and interact with toolbars.
 */
export interface ToolbarContext {
	/** All currently selected nodes */
	selectedNodes: CanvasNode[];
	/**
	 * Update the data for all selected nodes.
	 * The updater receives each node and should return the new data.
	 */
	updateSelectedNodes: (updater: (node: CanvasNode) => unknown) => void;
	/**
	 * Rotate all selected nodes by a delta in degrees.
	 * The rotation is applied relative to current rotation.
	 */
	rotateSelectedNodes: (deltaDegrees: number) => void;
	/** Current canvas zoom scale */
	canvasScale: number;
	/** Current viewport state */
	viewport: ViewportState;
}

/**
 * Function signature for application-provided toolbar renderer.
 * The application decides what toolbar to show based on selection state.
 */
export type RenderToolbarFn = (context: ToolbarContext) => React.ReactNode;
