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
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
} from "react";

import {
	boundsIntersect,
	boxSelect,
	type CanvasNode,
	type Connection,
	getAbsoluteBounds,
	getAbsolutePosition,
	getAbsoluteSize,
	getBoundsFromPoints,
	getDescendantIds,
	getExtendedBounds,
	getViewportBounds,
	getViewportTransform,
	type HitArea,
	hitTestTopNode,
	isAncestor,
	isPointInBounds,
	type Point,
	pan,
	type RenderToolbarFn,
	type Size,
	screenToCanvas,
	type ToolId,
	toRelativePosition,
	type ViewportState,
	zoomAtPoint,
} from "../core";
import {
	updatePanning,
	updateScale,
	updateViewportBounds,
	type VisibilityZone,
} from "../core/viewportSignals";
import { getNodeType, getTool } from "../plugins";

import { Background } from "./Background";
import { SelectionToolbarContainer } from "./NodeToolbar";
import { NodeWrapper } from "./NodeWrapper";

// ============================================================================
// Types
// ============================================================================

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

type InteractionMode =
	| "idle"
	| "selecting" // Drawing selection box
	| "dragging" // Moving nodes
	| "resizing" // Resizing a node
	| "drawing" // Using a tool
	| "panning"; // Right-click drag to pan

interface DragState {
	nodeId: string;
	startPosition: Point;
	startMousePosition: Point;
}

interface ResizeState {
	nodeId: string;
	handle: HitArea;
	startBounds: { x: number; y: number; width: number; height: number };
	startMousePosition: Point;
}

interface PanState {
	startMousePosition: Point; // Screen coordinates
	startViewport: ViewportState;
}

interface MultiClickState {
	points: Point[];
	toolState: unknown;
}

interface InteractionState {
	mode: InteractionMode;
	drawStartPoint: Point | null;
	selectionBox: { start: Point; end: Point } | null;
	dragState: DragState[] | null;
	resizeState: ResizeState | null;
	panState: PanState | null;
	multiClickState: MultiClickState | null; // For multi-click tools (e.g., polyline)
	drawToolState: unknown; // Tool-specific state during drawing
	drawPreview: React.ReactNode | null; // Preview element during drawing
}

// ============================================================================
// Connection Lines Component
// ============================================================================

interface ConnectionLinesProps {
	connections: Connection[];
	nodes: CanvasNode[];
	computedNodeGeometry: Map<string, { position: Point; size: Size }>;
}

const ConnectionLines: React.FC<ConnectionLinesProps> = ({
	connections,
	nodes,
	computedNodeGeometry,
}) => {
	if (connections.length === 0) return null;

	const nodeMap = new Map(nodes.map((n) => [n.id, n]));

	return (
		<svg
			style={{
				position: "absolute",
				top: 0,
				left: 0,
				width: "100%",
				height: "100%",
				overflow: "visible",
				pointerEvents: "none",
			}}
		>
			{connections.map((conn) => {
				const fromNode = nodeMap.get(conn.from);
				const toNode = nodeMap.get(conn.to);
				if (!fromNode || !toNode) return null;

				const fromGeom = computedNodeGeometry.get(conn.from);
				const toGeom = computedNodeGeometry.get(conn.to);
				if (!fromGeom || !toGeom) return null;

				// Calculate connection points (right side of from, left side of to)
				const fromX = fromGeom.position.x + fromGeom.size.width;
				const fromY = fromGeom.position.y + fromGeom.size.height / 2;
				const toX = toGeom.position.x;
				const toY = toGeom.position.y + toGeom.size.height / 2;

				// Create elbowed path (horizontal first, then vertical, then horizontal)
				const midX = (fromX + toX) / 2;
				const path = `M ${fromX} ${fromY} L ${midX} ${fromY} L ${midX} ${toY} L ${toX} ${toY}`;

				return (
					<g key={conn.id}>
						<path d={path} fill="none" stroke="#333" strokeWidth={2} />
						{/* Arrow at the end */}
						<polygon
							points={`${toX},${toY} ${toX - 8},${toY - 4} ${toX - 8},${toY + 4}`}
							fill="#333"
						/>
					</g>
				);
			})}
		</svg>
	);
};

// ============================================================================
// Component
// ============================================================================

// Virtualization buffer multiplier - how far outside viewport to keep nodes mounted
const VIRTUALIZATION_BUFFER = 1.5;

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

	// Container size for viewport calculations
	const [containerSize, setContainerSize] = useState<Size>({
		width: 1000,
		height: 800,
	});

	// Wheel-based panning/zooming state (touchpad scroll & pinch-to-zoom)
	const [isWheelPanning, setIsWheelPanning] = useState(false);
	const wheelPanTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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

	// ============================================================================
	// Container Size Tracking
	// ============================================================================

	// Track container size for viewport calculations
	useLayoutEffect(() => {
		const container = containerRef.current;
		if (!container) return;

		const updateSize = () => {
			const rect = container.getBoundingClientRect();
			setContainerSize({ width: rect.width, height: rect.height });
		};

		// Initial size
		updateSize();

		// Use ResizeObserver for size changes
		const resizeObserver = new ResizeObserver(updateSize);
		resizeObserver.observe(container);

		return () => resizeObserver.disconnect();
	}, []);

	// ============================================================================
	// Viewport Signals Update
	// ============================================================================

	// Update viewport bounds signals when viewport or container size changes
	useEffect(() => {
		updateViewportBounds(viewport, containerSize);
	}, [viewport, containerSize]);

	// Update panning signal
	useEffect(() => {
		updatePanning(isPanningProp || interactionRef.current.mode === "panning");
	}, [isPanningProp]);

	// ============================================================================
	// Viewport Culling & Virtualization
	// ============================================================================

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

	// Track values in refs for the wheel handler (avoids listener re-registration)
	const viewportRef = useRef(viewport);
	const setViewportRef = useRef(setViewport);
	const minZoomRef = useRef(minZoom);
	const maxZoomRef = useRef(maxZoom);

	useLayoutEffect(() => {
		viewportRef.current = viewport;
		setViewportRef.current = setViewport;
		minZoomRef.current = minZoom;
		maxZoomRef.current = maxZoom;
	});

	// Attach wheel listener with { passive: false } to allow preventDefault
	// This is necessary because browsers treat wheel events as passive by default,
	// which would allow pinch-to-zoom to zoom the page instead of the canvas
	useEffect(() => {
		const container = containerRef.current;
		if (!container) return;

		const handleWheel = (e: WheelEvent) => {
			e.preventDefault();
			const currentViewport = viewportRef.current;

			if (e.ctrlKey || e.metaKey) {
				// Zoom - use higher multiplier for responsive pinch-to-zoom
				const delta = -e.deltaY * 0.01;
				const newScale = currentViewport.scale * (1 + delta);
				const containerRect = container.getBoundingClientRect();
				const newViewport = zoomAtPoint(
					currentViewport,
					{
						x: e.clientX - containerRect.left,
						y: e.clientY - containerRect.top,
					},
					newScale,
					minZoomRef.current,
					maxZoomRef.current
				);
				setViewportRef.current(newViewport);

				// Update viewport signals so components can react to zoom changes
				if (newViewport.scale !== currentViewport.scale) {
					updateScale(newViewport.scale);
				}
			} else {
				// Pan (touchpad two-finger scroll)
				const newViewport = pan(currentViewport, -e.deltaX, -e.deltaY);
				setViewportRef.current(newViewport);
			}

			// Signal wheel-based panning/zoom to both signals and React state.
			// React state (isWheelPanning) propagates as a prop to node components
			// so they can clear stale cursors, etc.
			updatePanning(true);
			updatePanning(false);
			setIsWheelPanning(true);
			if (wheelPanTimeoutRef.current) clearTimeout(wheelPanTimeoutRef.current);
			wheelPanTimeoutRef.current = setTimeout(
				() => setIsWheelPanning(false),
				200
			);
		};

		container.addEventListener("wheel", handleWheel, { passive: false });
		return () => container.removeEventListener("wheel", handleWheel);
	}, []);

	// Handle mouse down
	const handleMouseDown = useCallback(
		(e: React.MouseEvent) => {
			// Right click - start panning
			if (e.button === 2) {
				e.preventDefault();
				updateInteraction({
					mode: "panning",
					panState: {
						startMousePosition: { x: e.clientX, y: e.clientY },
						startViewport: { ...viewport },
					},
				});
				return;
			}

			if (e.button !== 0) return; // Left click only for other interactions

			// In read-only mode, left-click drag pans the canvas
			if (readOnly) {
				e.preventDefault();
				updateInteraction({
					mode: "panning",
					panState: {
						startMousePosition: { x: e.clientX, y: e.clientY },
						startViewport: { ...viewport },
					},
				});
				return;
			}

			// Pan tool - left click starts panning (no node interaction)
			if (activeTool === "pan") {
				e.preventDefault();
				updateInteraction({
					mode: "panning",
					panState: {
						startMousePosition: { x: e.clientX, y: e.clientY },
						startViewport: { ...viewport },
					},
				});
				return;
			}

			const canvasPoint = toCanvasPoint(e.clientX, e.clientY);
			const tool = getTool(activeTool);

			if (activeTool === "selection") {
				// Hit test for selection
				// Pass a function to determine if a node should use corners-only hit testing
				// (for nodes with locked aspect ratio, only corner handles are valid)
				const getCornersOnly = (node: CanvasNode) => {
					const plugin = getNodeType(node.type);
					return plugin?.resizable?.aspectRatio === "lock";
				};
				const hit = hitTestTopNode(
					canvasPoint,
					nodes,
					viewport.scale,
					getCornersOnly
				);

				if (hit) {
					// Check if clicking on a resize handle of a selected node
					if (hit.area !== "body" && selectionSet.has(hit.nodeId)) {
						// Start resizing
						const node = nodes.find((n) => n.id === hit.nodeId);
						if (node) {
							// Use absolute position/size for child nodes
							const absPos = getAbsolutePosition(node, nodes);
							const absSize = getAbsoluteSize(node, nodes);

							updateInteraction({
								mode: "resizing",
								resizeState: {
									nodeId: hit.nodeId,
									handle: hit.area,
									startBounds: {
										x: absPos.x,
										y: absPos.y,
										width: absSize.width,
										height: absSize.height,
									},
									startMousePosition: canvasPoint,
								},
							});
							return;
						}
					}

					// Clicked on node body - select and prepare for drag
					let nodesToDrag: string[];

					if (e.shiftKey) {
						// Additive selection
						if (selectionSet.has(hit.nodeId)) {
							// Remove from selection, don't start drag
							onSelectionChange?.(selection.filter((id) => id !== hit.nodeId));
							return;
						} else {
							// Add to selection and drag all selected
							const newSelection = [...selection, hit.nodeId];
							onSelectionChange?.(newSelection);
							nodesToDrag = newSelection;
						}
					} else if (selectionSet.has(hit.nodeId)) {
						// Already selected, drag all selected nodes
						nodesToDrag = selection;
					} else {
						// Not selected, select only this node and drag it
						onSelectionChange?.([hit.nodeId]);
						nodesToDrag = [hit.nodeId];
					}

					// Start dragging - use absolute positions for child nodes
					const dragStates: DragState[] = nodesToDrag.map((id) => {
						const node = nodes.find((n) => n.id === id);
						const startPosition = node
							? getAbsolutePosition(node, nodes)
							: { x: 0, y: 0 };
						return {
							nodeId: id,
							startPosition,
							startMousePosition: canvasPoint,
						};
					});

					updateInteraction({
						mode: "dragging",
						dragState: dragStates,
					});
					setFocusedNodeId(hit.nodeId);
				} else {
					// Start selection box
					updateInteraction({
						mode: "selecting",
						drawStartPoint: canvasPoint,
						selectionBox: { start: canvasPoint, end: canvasPoint },
					});
					// Clicking on the canvas background clears node focus
					setFocusedNodeId(null);
					if (!e.shiftKey) {
						onSelectionChange?.([]);
					}
				}
			} else if (tool) {
				// Check for multi-click tool in progress
				const interaction = interactionRef.current;
				if (
					interaction.mode === "drawing" &&
					interaction.multiClickState &&
					tool.isMultiClick
				) {
					// Add another point to multi-click tool
					if (tool.onClickAdd) {
						const result = tool.onClickAdd(
							canvasPoint,
							interaction.multiClickState.toolState,
							{
								shift: e.shiftKey,
								alt: e.altKey,
								ctrl: e.ctrlKey,
								meta: e.metaKey,
							}
						);
						updateInteraction({
							multiClickState: {
								points: [...interaction.multiClickState.points, canvasPoint],
								toolState: result.state,
							},
						});
					}
					return;
				}

				// Check for click-to-place tool
				if (tool.onClick) {
					const result = tool.onClick(canvasPoint, {
						nodes,
						connections,
						selection,
						viewport,
						mode: "draw",
						activeTool,
						isDrawing: false,
						isPanning: false,
					});

					if (result && result.length > 0) {
						onNodesChange?.([...nodes, ...result]);
						onSelectionChange?.(result.map((n) => n.id));
						// Switch back to selection tool after placing node
						onToolComplete?.();
					}
					return;
				}

				// Check for multi-click tool start
				if (tool.isMultiClick && tool.onClickStart) {
					const toolState = tool.onClickStart(canvasPoint, {
						nodes,
						connections,
						selection,
						viewport,
						mode: "draw",
						activeTool,
						isDrawing: false,
						isPanning: false,
					});
					updateInteraction({
						mode: "drawing",
						drawStartPoint: canvasPoint,
						multiClickState: {
							points: [canvasPoint],
							toolState,
						},
					});
					return;
				}

				// Start drawing with drag-based tool
				if (tool.onDrawStart) {
					const toolState = tool.onDrawStart(canvasPoint, {
						nodes,
						connections,
						selection,
						viewport,
						mode: "draw",
						activeTool,
						isDrawing: false,
						isPanning: false,
					});
					updateInteraction({
						mode: "drawing",
						drawStartPoint: canvasPoint,
						drawToolState: toolState,
						drawPreview: null,
					});
				}
			}
		},
		[
			activeTool,
			nodes,
			connections,
			viewport,
			selection,
			selectionSet,
			readOnly,
			onSelectionChange,
			onNodesChange,
			onToolComplete,
			toCanvasPoint,
			updateInteraction,
		]
	);

	// Handle mouse move
	const handleMouseMove = useCallback(
		(e: React.MouseEvent) => {
			const interaction = interactionRef.current;
			const canvasPoint = toCanvasPoint(e.clientX, e.clientY);

			if (
				interaction.mode === "selecting" &&
				interaction.selectionBox &&
				interaction.drawStartPoint
			) {
				// Update selection box
				const newSelectionBox = {
					start: interaction.drawStartPoint,
					end: canvasPoint,
				};
				interactionRef.current.selectionBox = newSelectionBox;
				// Update state to trigger re-render with new selection box position
				setCurrentSelectionBox(newSelectionBox);

				// Real-time selection - update selection as box changes
				const bounds = getBoundsFromPoints(
					interaction.drawStartPoint,
					canvasPoint
				);
				if (bounds.width > 5 || bounds.height > 5) {
					const selectedIds = boxSelect(bounds, nodes);
					onSelectionChange?.(selectedIds);
				}
			} else if (interaction.mode === "dragging" && interaction.dragState) {
				const draggedNodeIds = new Set(
					interaction.dragState.map((d) => d.nodeId)
				);

				// Get all descendants of dragged nodes (they move with their parents)
				const allDraggedIds = new Set(draggedNodeIds);
				for (const id of draggedNodeIds) {
					const descendants = getDescendantIds(id, nodes);
					for (const descendantId of descendants) {
						allDraggedIds.add(descendantId);
					}
				}

				// Move all dragged nodes (only the explicitly dragged ones, not descendants)
				const updatedNodes = nodes.map((node) => {
					const drag = interaction.dragState!.find((d) => d.nodeId === node.id);
					if (drag) {
						const deltaX = canvasPoint.x - drag.startMousePosition.x;
						const deltaY = canvasPoint.y - drag.startMousePosition.y;

						// For child nodes with relative positioning, update relative position
						if (node.parentId && node.relativePosition) {
							const parent = nodes.find((n) => n.id === node.parentId);
							if (parent) {
								const parentBounds = getAbsoluteBounds(parent, nodes);
								const newAbsolutePos = {
									x: drag.startPosition.x + deltaX,
									y: drag.startPosition.y + deltaY,
								};
								const newRelativePos = toRelativePosition(
									newAbsolutePos,
									parentBounds
								);
								return {
									...node,
									relativePosition: newRelativePos,
									position: newAbsolutePos, // Update absolute as fallback
								};
							}
						}

						return {
							...node,
							position: {
								x: drag.startPosition.x + deltaX,
								y: drag.startPosition.y + deltaY,
							},
						};
					}
					return node;
				});

				// Find potential drop target (node under cursor that's not being dragged)
				let potentialDropTarget: string | null = null;
				const newWillDetach = new Set<string>();

				// Check each dragged node for parenting changes
				for (const dragState of interaction.dragState) {
					const draggedNode = nodes.find((n) => n.id === dragState.nodeId);
					if (!draggedNode) continue;

					// Calculate where this node will be after the drag
					const deltaX = canvasPoint.x - dragState.startMousePosition.x;
					const deltaY = canvasPoint.y - dragState.startMousePosition.y;
					const draggedCenter = {
						x: dragState.startPosition.x + deltaX + draggedNode.size.width / 2,
						y: dragState.startPosition.y + deltaY + draggedNode.size.height / 2,
					};

					// Check if dragging outside current parent's bounds
					if (draggedNode.parentId) {
						const parent = nodes.find((n) => n.id === draggedNode.parentId);
						if (parent) {
							const parentBounds = getAbsoluteBounds(parent, nodes);
							if (!isPointInBounds(draggedCenter, parentBounds)) {
								newWillDetach.add(draggedNode.id);
							}
						}
					}

					// Find potential new parent (largest node containing the dragged center)
					for (const node of nodes) {
						// Skip the dragged node itself and its descendants
						if (allDraggedIds.has(node.id)) continue;

						// Skip if the node would create a circular reference
						if (isAncestor(dragState.nodeId, node.id, nodes)) continue;

						const nodeBounds = getAbsoluteBounds(node, nodes);
						if (isPointInBounds(draggedCenter, nodeBounds)) {
							// Only set as drop target if it's different from current parent
							if (node.id !== draggedNode.parentId) {
								potentialDropTarget = node.id;
							}
						}
					}
				}

				setDropTargetId(potentialDropTarget);
				setWillDetachIds(newWillDetach);
				onNodesChange?.(updatedNodes);
			} else if (interaction.mode === "resizing" && interaction.resizeState) {
				// Resize the node
				const { resizeState } = interaction;
				const deltaX = canvasPoint.x - resizeState.startMousePosition.x;
				const deltaY = canvasPoint.y - resizeState.startMousePosition.y;
				const { startBounds, handle } = resizeState;

				// Get the plugin to check for aspect ratio lock
				const resizingNode = nodes.find((n) => n.id === resizeState.nodeId);
				const plugin = resizingNode ? getNodeType(resizingNode.type) : null;
				const lockAspectRatio = plugin?.resizable?.aspectRatio === "lock";
				const aspectRatio = startBounds.width / startBounds.height;

				let newX = startBounds.x;
				let newY = startBounds.y;
				let newWidth = startBounds.width;
				let newHeight = startBounds.height;

				// Calculate new bounds based on which handle is being dragged
				// Extract direction from handle to avoid matching 'e' or 's' in 'resize-' prefix
				const direction = handle.replace("resize-", "");

				if (lockAspectRatio) {
					// For locked aspect ratio, only corner handles should be used
					// Calculate based on the dominant movement direction
					const isCorner =
						(direction.includes("n") || direction.includes("s")) &&
						(direction.includes("e") || direction.includes("w"));

					if (isCorner) {
						// Use the diagonal distance to determine scale
						// Determine which direction to base the scale on
						const absDeltaX = Math.abs(deltaX);
						const absDeltaY = Math.abs(deltaY);

						// Choose the dominant axis for scaling
						let scaleFactor: number;
						if (absDeltaX / aspectRatio > absDeltaY) {
							// Width change is dominant
							if (direction.includes("e")) {
								scaleFactor = (startBounds.width + deltaX) / startBounds.width;
							} else {
								scaleFactor = (startBounds.width - deltaX) / startBounds.width;
							}
						} else {
							// Height change is dominant
							if (direction.includes("s")) {
								scaleFactor =
									(startBounds.height + deltaY) / startBounds.height;
							} else {
								scaleFactor =
									(startBounds.height - deltaY) / startBounds.height;
							}
						}

						// Apply uniform scale
						newWidth = startBounds.width * scaleFactor;
						newHeight = startBounds.height * scaleFactor;

						// Adjust position based on which corner is being dragged
						if (direction.includes("w")) {
							newX = startBounds.x + startBounds.width - newWidth;
						}
						if (direction.includes("n")) {
							newY = startBounds.y + startBounds.height - newHeight;
						}
					}
				} else {
					// Non-locked aspect ratio: normal resize behavior
					if (direction.includes("w")) {
						newX = startBounds.x + deltaX;
						newWidth = startBounds.width - deltaX;
					}
					if (direction.includes("e")) {
						newWidth = startBounds.width + deltaX;
					}
					if (direction.includes("n")) {
						newY = startBounds.y + deltaY;
						newHeight = startBounds.height - deltaY;
					}
					if (direction.includes("s")) {
						newHeight = startBounds.height + deltaY;
					}
				}

				// Enforce minimum size
				const minSize = plugin?.resizable?.minSize?.width ?? 20;
				const minHeight = plugin?.resizable?.minSize?.height ?? 20;
				if (newWidth < minSize) {
					if (lockAspectRatio) {
						// For locked aspect ratio, scale both dimensions
						const scale = minSize / newWidth;
						newWidth = minSize;
						newHeight = newHeight * scale;
					} else {
						newWidth = minSize;
					}
					if (direction.includes("w")) {
						newX = startBounds.x + startBounds.width - newWidth;
					}
				}
				if (newHeight < minHeight) {
					if (lockAspectRatio) {
						// For locked aspect ratio, scale both dimensions
						const scale = minHeight / newHeight;
						newHeight = minHeight;
						newWidth = newWidth * scale;
					} else {
						newHeight = minHeight;
					}
					if (direction.includes("n")) {
						newY = startBounds.y + startBounds.height - newHeight;
					}
				}

				const updatedNodes = nodes.map((node) => {
					if (node.id === resizeState.nodeId) {
						// For child nodes, update relative position and size
						if (node.parentId && node.relativePosition && node.relativeSize) {
							const parent = nodes.find((n) => n.id === node.parentId);
							if (parent) {
								const parentBounds = getAbsoluteBounds(parent, nodes);
								const newRelativePos = toRelativePosition(
									{ x: newX, y: newY },
									parentBounds
								);
								const newRelativeSize = {
									width: newWidth / parentBounds.width,
									height: newHeight / parentBounds.height,
								};
								return {
									...node,
									relativePosition: newRelativePos,
									relativeSize: newRelativeSize,
									position: { x: newX, y: newY },
									size: { width: newWidth, height: newHeight },
								};
							}
						}

						return {
							...node,
							position: { x: newX, y: newY },
							size: { width: newWidth, height: newHeight },
						};
					}
					return node;
				});
				onNodesChange?.(updatedNodes);
			} else if (interaction.mode === "panning" && interaction.panState) {
				// Pan the viewport with right-click drag
				const { panState } = interaction;
				const deltaX = e.clientX - panState.startMousePosition.x;
				const deltaY = e.clientY - panState.startMousePosition.y;

				setViewport({
					...panState.startViewport,
					x: panState.startViewport.x + deltaX,
					y: panState.startViewport.y + deltaY,
				});
			} else if (
				interaction.mode === "drawing" &&
				interaction.drawStartPoint &&
				!interaction.multiClickState
			) {
				// Update drawing preview for drag-based tools
				const tool = getTool(activeTool);
				if (tool?.onDrawMove) {
					const modifiers = {
						shift: e.shiftKey,
						alt: e.altKey,
						ctrl: e.ctrlKey,
						meta: e.metaKey,
					};
					const result = tool.onDrawMove(
						canvasPoint,
						interaction.drawStartPoint,
						interaction.drawToolState,
						modifiers
					);
					interactionRef.current.drawToolState = result.state;
					interactionRef.current.drawPreview = result.preview;
					setDrawingPreview(result.preview);
				}
			}
		},
		[
			nodes,
			toCanvasPoint,
			onNodesChange,
			onSelectionChange,
			setViewport,
			activeTool,
		]
	);

	// Handle mouse up
	const handleMouseUp = useCallback(
		(e: React.MouseEvent) => {
			const interaction = interactionRef.current;
			const canvasPoint = toCanvasPoint(e.clientX, e.clientY);
			const tool = getTool(activeTool);

			if (
				interaction.mode === "drawing" &&
				tool &&
				interaction.drawStartPoint
			) {
				// Skip if this is a multi-click tool (handled by double-click)
				if (tool.isMultiClick && interaction.multiClickState) {
					// Don't reset state for multi-click tools on single click
					return;
				}

				// Complete drag-based tool drawing
				if (tool.onDrawEnd) {
					const result = tool.onDrawEnd(
						canvasPoint,
						interaction.drawStartPoint,
						{},
						{
							nodes,
							connections,
							selection,
							viewport,
							mode: "draw",
							activeTool,
							isDrawing: false,
							isPanning: false,
						}
					);

					if (result && result.length > 0) {
						onNodesChange?.([...nodes, ...result]);
						onSelectionChange?.(result.map((n) => n.id));
						// Switch back to selection tool after drawing
						onToolComplete?.();
					}
				}
			}

			// Handle parenting changes on drag end
			if (interaction.mode === "dragging" && interaction.dragState) {
				let updatedNodes = [...nodes];

				for (const dragState of interaction.dragState) {
					const draggedNode = updatedNodes.find(
						(n) => n.id === dragState.nodeId
					);
					if (!draggedNode) continue;

					// Calculate where this node is now
					const nodePos = getAbsolutePosition(draggedNode, updatedNodes);
					const nodeSize = getAbsoluteSize(draggedNode, updatedNodes);
					// Center point available for future use (e.g., snapping, guides)
					void {
						x: nodePos.x + nodeSize.width / 2,
						y: nodePos.y + nodeSize.height / 2,
					};

					// Check if should detach from current parent
					if (draggedNode.parentId && willDetachIds.has(draggedNode.id)) {
						const absPos = getAbsolutePosition(draggedNode, updatedNodes);
						const absSize = getAbsoluteSize(draggedNode, updatedNodes);

						updatedNodes = updatedNodes.map((n) => {
							if (n.id === draggedNode.id) {
								return {
									...n,
									parentId: undefined,
									relativePosition: undefined,
									relativeSize: undefined,
									position: absPos,
									size: absSize,
								};
							}
							return n;
						});
					}

					// Check if should attach to new parent
					if (dropTargetId && dropTargetId !== draggedNode.parentId) {
						const parent = updatedNodes.find((n) => n.id === dropTargetId);
						if (parent) {
							// Prevent circular references
							if (!isAncestor(draggedNode.id, dropTargetId, updatedNodes)) {
								const parentBounds = getAbsoluteBounds(parent, updatedNodes);
								const nodeAbsPos = getAbsolutePosition(
									draggedNode,
									updatedNodes
								);
								const nodeAbsSize = getAbsoluteSize(draggedNode, updatedNodes);

								const relPos = toRelativePosition(nodeAbsPos, parentBounds);
								const relSize = {
									width: nodeAbsSize.width / parentBounds.width,
									height: nodeAbsSize.height / parentBounds.height,
								};

								updatedNodes = updatedNodes.map((n) => {
									if (n.id === draggedNode.id) {
										return {
											...n,
											parentId: dropTargetId,
											relativePosition: relPos,
											relativeSize: relSize,
											position: nodeAbsPos,
											size: nodeAbsSize,
										};
									}
									return n;
								});
							}
						}
					}
				}

				onNodesChange?.(updatedNodes);

				// Call onDragEnd with the IDs of the dragged nodes
				const draggedIds = interaction.dragState.map((d) => d.nodeId);
				onDragEnd?.(draggedIds);
			}

			// Clear drag parenting state
			setDropTargetId(null);
			setWillDetachIds(new Set());

			// Don't reset if in multi-click mode
			if (tool?.isMultiClick && interaction.multiClickState) {
				return;
			}

			setDrawingPreview(null);
			updateInteraction({
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
		},
		[
			activeTool,
			nodes,
			connections,
			selection,
			viewport,
			onNodesChange,
			onSelectionChange,
			onToolComplete,
			onDragEnd,
			toCanvasPoint,
			updateInteraction,
			dropTargetId,
			willDetachIds,
		]
	);

	// Handle double-click (for completing multi-click tools)
	const handleDoubleClick = useCallback(
		(_e: React.MouseEvent) => {
			const interaction = interactionRef.current;
			const tool = getTool(activeTool);

			// Complete multi-click tool on double-click
			if (
				interaction.mode === "drawing" &&
				tool?.isMultiClick &&
				interaction.multiClickState &&
				tool.onClickComplete
			) {
				const result = tool.onClickComplete(
					interaction.multiClickState.toolState,
					{
						nodes,
						connections,
						selection,
						viewport,
						mode: "draw",
						activeTool,
						isDrawing: false,
						isPanning: false,
					}
				);

				if (result && result.length > 0) {
					onNodesChange?.([...nodes, ...result]);
					onSelectionChange?.(result.map((n) => n.id));
					// Switch back to selection tool after completing multi-click drawing
					onToolComplete?.();
				}

				setDrawingPreview(null);
				updateInteraction({
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
			}
		},
		[
			activeTool,
			nodes,
			connections,
			selection,
			viewport,
			onNodesChange,
			onSelectionChange,
			onToolComplete,
			updateInteraction,
		]
	);

	// Prevent context menu on right-click (we use it for panning)
	const handleContextMenu = useCallback((e: React.MouseEvent) => {
		e.preventDefault();
	}, []);

	// Store nodes ref to avoid dependencies in callbacks
	// This prevents callback recreation when nodes change
	// Refs for stable callbacks - updated synchronously during render
	// This pattern is intentional for performance (avoiding re-renders during drag/pan)
	// eslint rule disabled for this file
	const nodesRef = useRef(nodes);
	nodesRef.current = nodes;
	const selectionRef = useRef(selection);
	selectionRef.current = selection;
	const onNodesChangeRef = useRef(onNodesChange);
	onNodesChangeRef.current = onNodesChange;
	const onNodeDataChangeRef = useRef(onNodeDataChange);
	onNodeDataChangeRef.current = onNodeDataChange;

	// Handle node data change - stable callback using refs
	const handleNodeDataChange = useCallback(
		(nodeId: string, newData: unknown) => {
			onNodeDataChangeRef.current?.(nodeId, newData);
			const updatedNodes = nodesRef.current.map((n) =>
				n.id === nodeId ? { ...n, data: newData } : n
			);
			onNodesChangeRef.current?.(updatedNodes);
		},
		[] // No dependencies - uses refs
	);

	// Handle node size change (when nodes need to resize themselves, e.g., PDFs)
	// Stable callback using refs to prevent cascade of re-renders
	const handleNodeSizeChange = useCallback(
		(nodeId: string, newSize: { width: number; height: number }) => {
			const updatedNodes = nodesRef.current.map((n) =>
				n.id === nodeId ? { ...n, size: newSize } : n
			);
			onNodesChangeRef.current?.(updatedNodes);
		},
		[] // No dependencies - uses refs
	);

	// Handle node rotation change
	// Stable callback using refs to prevent cascade of re-renders
	// Note: Children's rotation values are NOT updated - their positions are automatically
	// rotated around the parent's center via getAbsolutePosition transform.
	// Children maintain their own rotation relative to the parent.
	const handleNodeRotate = useCallback(
		(nodeId: string, rotation: number) => {
			const normalizedRotation = ((rotation % 360) + 360) % 360;
			const updatedNodes = nodesRef.current.map((n) => {
				if (n.id === nodeId) {
					return { ...n, rotation: normalizedRotation };
				}
				return n;
			});
			onNodesChangeRef.current?.(updatedNodes);
		},
		[] // No dependencies - uses refs
	);

	// Update data for all selected nodes - used by toolbar
	const updateSelectedNodes = useCallback(
		(updater: (node: CanvasNode) => unknown) => {
			const selectedIds = new Set(selectionRef.current);
			const updatedNodes = nodesRef.current.map((node) => {
				if (selectedIds.has(node.id)) {
					const newData = updater(node);
					onNodeDataChangeRef.current?.(node.id, newData);
					return { ...node, data: newData };
				}
				return node;
			});
			onNodesChangeRef.current?.(updatedNodes);
		},
		[] // No dependencies - uses refs
	);

	// Rotate all selected nodes - used by toolbar
	const rotateSelectedNodes = useCallback(
		(deltaDegrees: number) => {
			const selectedIds = new Set(selectionRef.current);
			const updatedNodes = nodesRef.current.map((node) => {
				if (selectedIds.has(node.id)) {
					const currentRotation = node.rotation ?? 0;
					const newRotation =
						(((currentRotation + deltaDegrees) % 360) + 360) % 360;
					return { ...node, rotation: newRotation };
				}
				return node;
			});
			onNodesChangeRef.current?.(updatedNodes);
		},
		[] // No dependencies - uses refs
	);

	// Create memoized per-node callbacks to prevent unnecessary re-renders
	// This prevents creating new function references on every render
	const nodeSizeChangeCallbacks = useRef(
		new Map<string, (size: { width: number; height: number }) => void>()
	);
	const nodeDataChangeCallbacks = useRef(
		new Map<string, (data: unknown) => void>()
	);
	const nodeRotateCallbacks = useRef(
		new Map<string, (degrees: number) => void>()
	);

	const getNodeSizeChangeCallback = useCallback(
		(nodeId: string) => {
			let callback = nodeSizeChangeCallbacks.current.get(nodeId);
			if (!callback) {
				callback = (size: { width: number; height: number }) =>
					handleNodeSizeChange(nodeId, size);
				nodeSizeChangeCallbacks.current.set(nodeId, callback);
			}
			return callback;
		},
		[handleNodeSizeChange]
	);

	const getNodeDataChangeCallback = useCallback(
		(nodeId: string) => {
			let callback = nodeDataChangeCallbacks.current.get(nodeId);
			if (!callback) {
				callback = (data: unknown) => handleNodeDataChange(nodeId, data);
				nodeDataChangeCallbacks.current.set(nodeId, callback);
			}
			return callback;
		},
		[handleNodeDataChange]
	);

	const getNodeRotateCallback = useCallback(
		(nodeId: string) => {
			let callback = nodeRotateCallbacks.current.get(nodeId);
			if (!callback) {
				callback = (degrees: number) => handleNodeRotate(nodeId, degrees);
				nodeRotateCallbacks.current.set(nodeId, callback);
			}
			return callback;
		},
		[handleNodeRotate]
	);

	// Store selection change ref to avoid dependency
	// Refs for stable callbacks - updated synchronously during render
	const onSelectionChangeRef = useRef(onSelectionChange);
	onSelectionChangeRef.current = onSelectionChange;
	const onConnectionsChangeRef = useRef(onConnectionsChange);
	onConnectionsChangeRef.current = onConnectionsChange;
	const connectionsRef = useRef(connections);
	connectionsRef.current = connections;

	// Handle updates from selection overlay (add, update, remove nodes and connections)
	const handleUpdateNodes = useCallback(
		(updates: {
			add?: CanvasNode[];
			update?: Array<{ id: string; updates: Partial<CanvasNode> }>;
			remove?: string[];
			select?: string[];
			addConnections?: Connection[];
		}) => {
			let updatedNodes = [...nodesRef.current];

			// Remove nodes
			if (updates.remove && updates.remove.length > 0) {
				const removeSet = new Set(updates.remove);
				updatedNodes = updatedNodes.filter((n) => !removeSet.has(n.id));
			}

			// Update existing nodes
			if (updates.update && updates.update.length > 0) {
				const updateMap = new Map(updates.update.map((u) => [u.id, u.updates]));
				updatedNodes = updatedNodes.map((n) => {
					const nodeUpdates = updateMap.get(n.id);
					return nodeUpdates ? { ...n, ...nodeUpdates } : n;
				});
			}

			// Add new nodes
			if (updates.add && updates.add.length > 0) {
				updatedNodes = [...updatedNodes, ...updates.add];
			}

			onNodesChangeRef.current?.(updatedNodes);

			// Update selection if specified
			if (updates.select) {
				onSelectionChangeRef.current?.(updates.select);
			}

			// Add connections if specified
			if (updates.addConnections && updates.addConnections.length > 0) {
				const updatedConnections = [
					...connectionsRef.current,
					...updates.addConnections,
				];
				onConnectionsChangeRef.current?.(updatedConnections);
			}
		},
		[]
	);

	// Handle adding a connection from selection overlay
	const handleAddConnection = useCallback((connection: Connection) => {
		const updatedConnections = [...connectionsRef.current, connection];
		onConnectionsChangeRef.current?.(updatedConnections);
	}, []);

	// Handle focus
	const handleNodeFocus = useCallback((nodeId: string) => {
		setFocusedNodeId(nodeId);
	}, []);

	const handleNodeBlur = useCallback(() => {
		setFocusedNodeId(null);
	}, []);

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
