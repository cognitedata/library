/**
 * Main Canvas component
 * Renders the infinite canvas with all nodes
 *
 * Features:
 * - Viewport culling: Only renders nodes visible in viewport
 * - Virtualization: Keeps nearby nodes mounted at reduced fidelity
 * - Progressive fidelity: Reduces detail during pan/zoom interactions
 */

import React, {
	type ReactNode,
	useCallback,
	useMemo,
	useRef,
	useState,
} from "react";

import {
	boundsIntersect,
	type CanvasNode,
	type Connection,
	getAbsoluteBounds,
	getAbsolutePosition,
	getAbsoluteSize,
	getBoundsFromPoints,
	getExtendedBounds,
	getViewportBounds,
	getViewportTransform,
	type Point,
	type RenderToolbarFn,
	type Size,
	screenToCanvas,
	type ToolId,
	type ViewportState,
} from "../core";
import type { VisibilityZone } from "../core/viewportSignals";
import { getNodeType } from "../plugins";

import { Background } from "./Background";
import { ConnectionLines } from "./ConnectionLines";
import type {
	InteractionMode,
	InteractionState,
	ResizeState,
} from "./canvas-interaction";
import { SelectionToolbarContainer } from "./NodeToolbar";
import { NodeWrapper } from "./NodeWrapper";
import { useCanvasNodeMutations } from "./useCanvasNodeMutations";
import { useCanvasPointer } from "./useCanvasPointer";
import { useCanvasViewport } from "./useCanvasViewport";

const VIRTUALIZATION_BUFFER = 1.5;

export interface CanvasProps {
	nodes: CanvasNode[];
	connections?: Connection[];
	selection?: string[];
	viewport?: ViewportState;
	activeTool?: ToolId;
	isPanning?: boolean;

	// Callbacks
	onNodesChange?: (nodes: CanvasNode[]) => void;
	onConnectionsChange?: (connections: Connection[]) => void;
	onSelectionChange?: (selection: string[]) => void;
	onViewportChange?: (viewport: ViewportState) => void;
	onNodeDataChange?: (nodeId: string, data: unknown) => void;
	/** Called after a tool completes creating node(s) - use to switch back to selection tool */
	onToolComplete?: () => void;
	/** Called when node dragging ends - useful for triggering relayout */
	onDragEnd?: (draggedNodeIds: string[]) => void;
	/** Disables all editing interactions; pan and zoom still work */
	readOnly?: boolean;

	// Configuration
	minZoom?: number;
	maxZoom?: number;
	className?: string;
	style?: React.CSSProperties;

	// Render props
	renderToolPreview?: () => ReactNode;

	// Application-controlled toolbar
	/** Render prop for toolbar - application decides what to show based on selection */
	renderToolbar?: RenderToolbarFn;
}

export const Canvas: React.FC<CanvasProps> = ({
	nodes,
	connections = [],
	selection = [],
	viewport: controlledViewport,
	activeTool = "selection",
	isPanning: isPanningProp = false,
	onNodesChange,
	onConnectionsChange,
	onSelectionChange,
	onViewportChange,
	onNodeDataChange,
	onToolComplete,
	onDragEnd,
	minZoom = 0.1,
	maxZoom = 5,
	className,
	style,
	renderToolPreview,
	renderToolbar,
	readOnly = false,
}) => {
	const containerRef = useRef<HTMLDivElement>(null);
	const [internalViewport, setInternalViewport] = useState<ViewportState>({
		x: 0,
		y: 0,
		scale: 1,
	});
	const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);

	// Parent-child hierarchy drag state
	const [dropTargetId, setDropTargetId] = useState<string | null>(null);
	const [willDetachIds, setWillDetachIds] = useState<Set<string>>(new Set());

	// Use ref for interaction state to ensure synchronous access during mouse events
	const interactionRef = useRef<InteractionState>({
		mode: "idle",
		drawStartPoint: null,
		selectionBox: null,
		dragState: null,
		resizeState: null,
		panState: null,
		multiClickState: null,
		drawToolState: null,
		drawPreview: null,
	});

	// State for interaction values that affect rendering
	const [currentMode, setCurrentMode] = useState<InteractionMode>("idle");
	const [currentResizeState, setCurrentResizeState] =
		useState<ResizeState | null>(null);
	const [currentSelectionBox, setCurrentSelectionBox] = useState<{
		start: Point;
		end: Point;
	} | null>(null);

	// State for drawing preview (needs to be in state for React to render it)
	const [drawingPreview, setDrawingPreview] = useState<React.ReactNode>(null);

	const viewport = controlledViewport ?? internalViewport;
	const setViewport = onViewportChange ?? setInternalViewport;

	const { containerSize, isWheelPanning } = useCanvasViewport({
		containerRef,
		viewport,
		setViewport,
		isPanningProp,
		interactionRef,
		minZoom,
		maxZoom,
	});

	const selectionSet = useMemo(() => new Set(selection), [selection]);

	// Get selected nodes for selection overlay
	const selectedNodes = useMemo(
		() => nodes.filter((n) => selectionSet.has(n.id)),
		[nodes, selectionSet]
	);

	// Create canvas state object for selection overlay
	const canvasState = useMemo(
		() => ({
			nodes,
			connections,
			selection,
			viewport,
			mode: "select" as const,
			activeTool,
			isDrawing: false,
			isPanning: isPanningProp,
		}),
		[nodes, connections, selection, viewport, activeTool, isPanningProp]
	);

	// Calculate visibility bounds
	const { visibleBounds, virtualBounds } = useMemo(() => {
		const visible = getViewportBounds(viewport, containerSize);
		const virtual = getExtendedBounds(visible, VIRTUALIZATION_BUFFER);
		return { visibleBounds: visible, virtualBounds: virtual };
	}, [viewport, containerSize]);

	// Compute visibility zone for each node
	const nodeVisibilityMap = useMemo(() => {
		const map = new Map<string, VisibilityZone>();

		for (const node of nodes) {
			const nodeBounds = getAbsoluteBounds(node, nodes);

			if (boundsIntersect(nodeBounds, visibleBounds)) {
				map.set(node.id, "visible");
			} else if (boundsIntersect(nodeBounds, virtualBounds)) {
				map.set(node.id, "near");
			} else {
				map.set(node.id, "culled");
			}
		}

		return map;
	}, [nodes, visibleBounds, virtualBounds]);

	// Helper to update interaction state (both ref and state for rendering)
	const updateInteraction = useCallback(
		(updates: Partial<InteractionState>) => {
			Object.assign(interactionRef.current, updates);
			// Update state for values that affect rendering
			if ("mode" in updates) setCurrentMode(updates.mode!);
			if ("resizeState" in updates)
				setCurrentResizeState(updates.resizeState ?? null);
			if ("selectionBox" in updates)
				setCurrentSelectionBox(updates.selectionBox ?? null);
		},
		[]
	);

	// Get container bounds
	const getContainerOffset = useCallback((): Point => {
		if (!containerRef.current) return { x: 0, y: 0 };
		const rect = containerRef.current.getBoundingClientRect();
		return { x: rect.left, y: rect.top };
	}, []);

	// Convert screen point to canvas point
	const toCanvasPoint = useCallback(
		(screenX: number, screenY: number): Point => {
			const offset = getContainerOffset();
			return screenToCanvas({ x: screenX, y: screenY }, viewport, offset);
		},
		[viewport, getContainerOffset]
	);

	const {
		handleMouseDown,
		handleMouseMove,
		handleMouseUp,
		handleDoubleClick,
		handleContextMenu,
	} = useCanvasPointer({
		updateInteraction,
		viewport,
		nodes,
		selection,
		selectionSet,
		setCurrentSelectionBox,
		toCanvasPoint,
		readOnly,
		activeTool,
		onSelectionChange,
		onNodesChange,
		onDragEnd,
		setDropTargetId,
		setWillDetachIds,
		interactionRef,
		setViewport,
		connections,
		onToolComplete,
		setDrawingPreview,
		dropTargetId,
		willDetachIds,
		setFocusedNodeId,
	});

	const {
		updateSelectedNodes,
		rotateSelectedNodes,
		getNodeSizeChangeCallback,
		getNodeDataChangeCallback,
		getNodeRotateCallback,
		handleUpdateNodes,
		handleAddConnection,
		handleNodeFocus,
		handleNodeBlur,
	} = useCanvasNodeMutations({
		nodes,
		selection,
		connections,
		onNodesChange,
		onNodeDataChange,
		onSelectionChange,
		onConnectionsChange,
		setFocusedNodeId,
	});

	// Compute absolute positions and sizes for all nodes
	const computedNodeGeometry = useMemo(() => {
		const geometry = new Map<string, { position: Point; size: Size }>();
		for (const node of nodes) {
			geometry.set(node.id, {
				position: getAbsolutePosition(node, nodes),
				size: getAbsoluteSize(node, nodes),
			});
		}
		return geometry;
	}, [nodes]);

	// Sort nodes by z-index for rendering, with children rendering after parents
	const sortedNodes = useMemo(() => {
		// First sort by z-index
		const sorted = [...nodes].sort((a, b) => a.zIndex - b.zIndex);

		// Then ensure children are rendered after their parents
		// by doing a topological sort based on parent-child relationships
		const result: CanvasNode[] = [];
		const added = new Set<string>();

		const addNode = (node: CanvasNode) => {
			if (added.has(node.id)) return;

			// If node has a parent, ensure parent is added first
			if (node.parentId) {
				const parent = nodes.find((n) => n.id === node.parentId);
				if (parent && !added.has(parent.id)) {
					addNode(parent);
				}
			}

			result.push(node);
			added.add(node.id);
		};

		for (const node of sorted) {
			addNode(node);
		}

		return result;
	}, [nodes]);

	// Get cursor based on current state (uses state values, not refs)
	const getCursor = () => {
		if (currentMode === "panning") return "grabbing";
		if (activeTool === "pan") return "grab";
		if (currentMode === "dragging") return "grabbing";
		if (currentMode === "resizing" && currentResizeState) {
			const handle = currentResizeState.handle;
			if (handle === "resize-n" || handle === "resize-s") return "ns-resize";
			if (handle === "resize-e" || handle === "resize-w") return "ew-resize";
			if (handle === "resize-ne" || handle === "resize-sw")
				return "nesw-resize";
			if (handle === "resize-nw" || handle === "resize-se")
				return "nwse-resize";
		}
		if (activeTool !== "selection") return "crosshair";
		return "default";
	};

	// Render selection box
	const renderSelectionBox = () => {
		if (!currentSelectionBox) return null;

		const bounds = getBoundsFromPoints(
			currentSelectionBox.start,
			currentSelectionBox.end
		);

		return (
			<div
				data-selection-box
				style={{
					position: "absolute",
					left: bounds.x,
					top: bounds.y,
					width: bounds.width,
					height: bounds.height,
					border: "1px dashed #0066ff",
					backgroundColor: "rgba(0, 102, 255, 0.1)",
					pointerEvents: "none",
				}}
			/>
		);
	};

	return (
		<div
			ref={containerRef}
			data-canvas
			data-testid="canvas"
			data-mode={currentMode}
			className={className}
			style={{
				position: "relative",
				overflow: "hidden",
				width: "100%",
				height: "100%",
				cursor: getCursor(),
				...style,
			}}
			onMouseDown={handleMouseDown}
			onMouseMove={handleMouseMove}
			onMouseUp={handleMouseUp}
			onMouseLeave={handleMouseUp}
			onDoubleClick={handleDoubleClick}
			onContextMenu={handleContextMenu}
		>
			{/* Background layer - responds to pan/zoom */}
			<Background viewport={viewport} />

			{/* Transform container */}
			<div
				data-transform-container
				style={{
					position: "absolute",
					transformOrigin: "0 0",
					transform: getViewportTransform(viewport),
					willChange: "transform",
				}}
			>
				{/* Render connection lines */}
				<ConnectionLines
					connections={connections}
					nodes={nodes}
					computedNodeGeometry={computedNodeGeometry}
				/>

				{/* Render nodes - with viewport culling */}
				{sortedNodes.map((node) => {
					const visibilityZone = nodeVisibilityMap.get(node.id) ?? "culled";

					// Completely skip culled nodes - no DOM element at all
					// Exception: always render selected/focused nodes for interaction continuity
					const isInteractive =
						selectionSet.has(node.id) || focusedNodeId === node.id;
					if (visibilityZone === "culled" && !isInteractive) {
						return null;
					}

					const plugin = getNodeType(node.type);
					if (!plugin) {
						console.warn(`Unknown node type: ${node.type}`);
						return null;
					}

					const geometry = computedNodeGeometry.get(node.id);

					const isNodeSelected = selectionSet.has(node.id);

					return (
						<NodeWrapper
							key={node.id}
							node={node}
							plugin={plugin}
							isSelected={isNodeSelected}
							isFocused={focusedNodeId === node.id}
							canvasScale={viewport.scale}
							isCanvasPanning={
								isPanningProp || isWheelPanning || activeTool === "pan"
							}
							onDataChange={getNodeDataChangeCallback(node.id)}
							onSizeChange={getNodeSizeChangeCallback(node.id)}
							onRotate={getNodeRotateCallback(node.id)}
							onFocus={() => handleNodeFocus(node.id)}
							onBlur={handleNodeBlur}
							computedPosition={geometry?.position}
							computedSize={geometry?.size}
							isDropTarget={dropTargetId === node.id}
							willDetach={willDetachIds.has(node.id)}
							visibilityZone={visibilityZone}
							readOnly={readOnly}
							selectedNodes={isNodeSelected ? selectedNodes : undefined}
							canvasState={isNodeSelected ? canvasState : undefined}
							onUpdateNodes={handleUpdateNodes}
							onAddConnection={handleAddConnection}
						/>
					);
				})}

				{/* Selection box */}
				{renderSelectionBox()}

				{/* Drawing preview (during drag-based tool drawing) */}
				{drawingPreview}

				{/* Tool preview */}
				{renderToolPreview?.()}
			</div>

			{/* Selection toolbar - rendered outside transform container for fixed positioning */}
			{renderToolbar && selectedNodes.length > 0 && (
				<SelectionToolbarContainer
					selectedNodes={selectedNodes}
					nodes={nodes}
					viewport={viewport}
					containerSize={containerSize}
				>
					{renderToolbar({
						selectedNodes,
						updateSelectedNodes,
						rotateSelectedNodes,
						canvasScale: viewport.scale,
						viewport,
					})}
				</SelectionToolbarContainer>
			)}
		</div>
	);
};
