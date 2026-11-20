import type React from "react";
import { useCallback } from "react";
import {
	boxSelect,
	type CanvasNode,
	type Connection,
	getAbsoluteBounds,
	getAbsolutePosition,
	getAbsoluteSize,
	getBoundsFromPoints,
	getDescendantIds,
	hitTestTopNode,
	isAncestor,
	isPointInBounds,
	type Point,
	type ToolId,
	toRelativePosition,
	type ViewportState,
} from "../core";
import { getNodeType, getTool } from "../plugins";
import type { DragState, InteractionState } from "./canvas-interaction";

export interface CanvasPointerArgs {
	updateInteraction: (updates: Partial<InteractionState>) => void;
	viewport: ViewportState;
	nodes: CanvasNode[];
	selection: string[];
	selectionSet: Set<string>;
	setCurrentSelectionBox: (box: { start: Point; end: Point } | null) => void;
	toCanvasPoint: (screenX: number, screenY: number) => Point;
	readOnly: boolean;
	activeTool: ToolId;
	onSelectionChange?: (selection: string[]) => void;
	onNodesChange?: (nodes: CanvasNode[]) => void;
	onDragEnd?: (draggedNodeIds: string[]) => void;
	setDropTargetId: (id: string | null) => void;
	setWillDetachIds: (ids: Set<string>) => void;
	interactionRef: React.MutableRefObject<InteractionState>;
	setViewport: (viewport: ViewportState) => void;
	connections: Connection[];
	onToolComplete?: () => void;
	setDrawingPreview: (preview: React.ReactNode) => void;
	dropTargetId: string | null;
	willDetachIds: Set<string>;
	setFocusedNodeId: (id: string | null) => void;
}

export function useCanvasPointer(args: CanvasPointerArgs) {
	const {
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
	} = args;

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

	return {
		handleMouseDown,
		handleMouseMove,
		handleMouseUp,
		handleDoubleClick,
		handleContextMenu,
	};
}
