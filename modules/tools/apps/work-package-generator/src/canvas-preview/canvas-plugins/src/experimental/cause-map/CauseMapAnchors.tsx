/**
 * Cause Map Anchors Selection Overlay
 * Renders interactive plus anchors around a selected cause map node
 * for adding connected nodes in different directions.
 */

import type {
	CanvasNode,
	Connection,
	SelectionOverlayProps,
} from "@dune-canvas/canvas-lib";
import { generateId } from "@dune-canvas/canvas-lib";
import type React from "react";
import { useCallback, useMemo, useState } from "react";
import type { CauseMapNodeData } from "./CauseMapNode";
import {
	extractCauseMapGraph,
	getChildIds,
	getOutgoingEdges,
	getParentIds,
	hasMultipleParents,
	isRootNode,
} from "./causeMapLayout";
import {
	ALL_ANCHORS,
	ANCHOR_PADDING,
	ANCHOR_STYLE,
	type AnchorPosition,
	LEFT_RIGHT_ANCHORS,
	TARGET_NODE_SIZE,
} from "./constants";

// ============================================================================
// Anchor Position Utilities
// ============================================================================

interface AnchorPositionResult {
	x: number;
	y: number;
}

/**
 * Calculate the position for an anchor based on the node bounds and anchor type.
 */
const getAnchorPosition = (
	nodeWidth: number,
	nodeHeight: number,
	anchorName: AnchorPosition,
	padding: number
): AnchorPositionResult => {
	const halfWidth = nodeWidth / 2;
	const halfHeight = nodeHeight / 2;

	switch (anchorName) {
		case "top-center":
			return { x: halfWidth, y: -padding };
		case "bottom-center":
			return { x: halfWidth, y: nodeHeight + padding };
		case "middle-left":
			return { x: -padding, y: halfHeight };
		case "middle-right":
			return { x: nodeWidth + padding, y: halfHeight };
		default:
			return { x: halfWidth, y: halfHeight };
	}
};

// ============================================================================
// Single Anchor Component
// ============================================================================

interface AnchorProps {
	position: AnchorPosition;
	x: number;
	y: number;
	scale: number;
	isHovered: boolean;
	onHover: (hovered: boolean) => void;
	onClick: () => void;
}

const Anchor: React.FC<AnchorProps> = ({
	position,
	x,
	y,
	scale,
	isHovered,
	onHover,
	onClick,
}) => {
	const radius = isHovered ? ANCHOR_STYLE.hoveredRadius : ANCHOR_STYLE.radius;
	const displayRadius = radius / scale;
	const crossLength = ANCHOR_STYLE.crossLength / scale;
	const strokeWidth = ANCHOR_STYLE.strokeWidth / scale;
	const crossStrokeWidth = ANCHOR_STYLE.crossStrokeWidth / scale;

	return (
		<div
			data-anchor={position}
			style={{
				position: "absolute",
				left: x - displayRadius,
				top: y - displayRadius,
				width: displayRadius * 2,
				height: displayRadius * 2,
				borderRadius: "50%",
				backgroundColor: ANCHOR_STYLE.fill,
				border: `${strokeWidth}px solid ${ANCHOR_STYLE.stroke}`,
				boxShadow: `0 0 ${ANCHOR_STYLE.shadowBlur}px ${ANCHOR_STYLE.shadowColor}`,
				cursor: "pointer",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				transition: "transform 0.15s ease",
				transform: isHovered ? "scale(1.1)" : "scale(1)",
				zIndex: 1000,
			}}
			onMouseEnter={() => onHover(true)}
			onMouseLeave={() => onHover(false)}
			onMouseDown={(e) => {
				// Prevent Canvas from deselecting the node
				e.stopPropagation();
			}}
			onPointerDown={(e) => {
				// Also stop pointer events for touch devices
				e.stopPropagation();
			}}
			onClick={(e) => {
				e.stopPropagation();
				onClick();
			}}
		>
			{/* Plus sign shown on hover */}
			{isHovered && (
				<svg
					width={crossLength * 2}
					height={crossLength * 2}
					viewBox={`0 0 ${crossLength * 2} ${crossLength * 2}`}
					style={{ pointerEvents: "none" }}
				>
					{/* Horizontal line */}
					<line
						x1={crossLength * 0.3}
						y1={crossLength}
						x2={crossLength * 1.7}
						y2={crossLength}
						stroke={ANCHOR_STYLE.stroke}
						strokeWidth={crossStrokeWidth}
						strokeLinecap="round"
					/>
					{/* Vertical line */}
					<line
						x1={crossLength}
						y1={crossLength * 0.3}
						x2={crossLength}
						y2={crossLength * 1.7}
						stroke={ANCHOR_STYLE.stroke}
						strokeWidth={crossStrokeWidth}
						strokeLinecap="round"
					/>
				</svg>
			)}
		</div>
	);
};

// ============================================================================
// Preview Node Component (shown on hover)
// ============================================================================

interface PreviewNodeProps {
	x: number;
	y: number;
	width: number;
	height: number;
}

const PreviewNode: React.FC<PreviewNodeProps> = ({ x, y, width, height }) => (
	<div
		data-preview-node
		style={{
			position: "absolute",
			left: x,
			top: y,
			width,
			height,
			backgroundColor: "rgba(255, 255, 255, 0.6)",
			border: "2px dashed rgba(74, 103, 251, 0.5)",
			borderRadius: 8,
			pointerEvents: "none",
		}}
	/>
);

// ============================================================================
// Preview Connection Line (shown on hover)
// ============================================================================

interface PreviewConnectionProps {
	fromX: number;
	fromY: number;
	toX: number;
	toY: number;
}

const PreviewConnection: React.FC<PreviewConnectionProps> = ({
	fromX,
	fromY,
	toX,
	toY,
}) => {
	// Calculate SVG bounds
	const minX = Math.min(fromX, toX) - 10;
	const minY = Math.min(fromY, toY) - 10;
	const width = Math.abs(toX - fromX) + 20;
	const height = Math.abs(toY - fromY) + 20;

	// Adjust coordinates to be relative to SVG
	const relFromX = fromX - minX;
	const relFromY = fromY - minY;
	const relToX = toX - minX;
	const relToY = toY - minY;

	// Calculate elbow midpoint (horizontal first, then vertical)
	const midX = (relFromX + relToX) / 2;

	return (
		<svg
			style={{
				position: "absolute",
				left: minX,
				top: minY,
				width,
				height,
				pointerEvents: "none",
				overflow: "visible",
			}}
		>
			<path
				d={`M ${relFromX} ${relFromY} L ${midX} ${relFromY} L ${midX} ${relToY} L ${relToX} ${relToY}`}
				fill="none"
				stroke="rgba(74, 103, 251, 0.5)"
				strokeWidth={2}
				strokeDasharray="4 4"
			/>
			{/* Arrow at the end */}
			<polygon
				points={`${relToX},${relToY} ${relToX - 8},${relToY - 4} ${relToX - 8},${relToY + 4}`}
				fill="rgba(74, 103, 251, 0.5)"
			/>
		</svg>
	);
};

// ============================================================================
// Main CauseMapAnchors Component
// ============================================================================

export const CauseMapAnchors: React.FC<
	SelectionOverlayProps<CauseMapNodeData>
> = ({ node, canvasScale, canvasState, selectedNodes, onUpdateNodes }) => {
	const [hoveredAnchor, setHoveredAnchor] = useState<AnchorPosition | null>(
		null
	);

	// Only show anchors if exactly 1 cause map node is selected
	const shouldShowAnchors =
		selectedNodes.length === 1 &&
		selectedNodes[0].type === "cause-map" &&
		selectedNodes[0].id === node.id;

	// Extract cause map graph from canvas state
	const { causeMapEdges } = useMemo(
		() => extractCauseMapGraph(canvasState.nodes, canvasState.connections),
		[canvasState.nodes, canvasState.connections]
	);

	// Determine which anchors to show based on node's position in the graph
	const anchorsToShow = useMemo(() => {
		if (!shouldShowAnchors) return [];

		// If there are no connections, show all anchors to allow building the graph
		if (causeMapEdges.length === 0) {
			return ALL_ANCHORS;
		}

		// If root node with != 1 child, or node has multiple parents,
		// only show left/right anchors (adding siblings is ambiguous)
		const isRoot = isRootNode(node.id, causeMapEdges);
		const outgoingCount = getOutgoingEdges(node.id, causeMapEdges).length;
		const hasMultParents = hasMultipleParents(node.id, causeMapEdges);

		if ((isRoot && outgoingCount !== 1) || hasMultParents) {
			return LEFT_RIGHT_ANCHORS;
		}

		return ALL_ANCHORS;
	}, [shouldShowAnchors, node.id, causeMapEdges]);

	// Anchor padding for visual display (scale-dependent for good appearance)
	const anchorPadding = Math.max(
		ANCHOR_PADDING.min,
		Math.min(ANCHOR_PADDING.default / canvasScale, ANCHOR_PADDING.max)
	);

	// Fixed spacing for node placement (NOT scale-dependent)
	const NODE_SPACING = 80; // Fixed spacing in canvas units

	// Handle anchor click - create new node and connection
	const handleAnchorClick = useCallback(
		(anchorName: AnchorPosition) => {
			const nodeWidth = node.size.width;
			const nodeHeight = node.size.height;
			const nodeX = node.position.x;
			const nodeY = node.position.y;

			// Calculate new node position using fixed spacing (not scale-dependent)
			let newNodeX = nodeX;
			let newNodeY = nodeY;

			switch (anchorName) {
				case "middle-right":
					// Place to the right of current node
					newNodeX = nodeX + nodeWidth + NODE_SPACING;
					newNodeY = nodeY + (nodeHeight - TARGET_NODE_SIZE.height) / 2;
					break;
				case "middle-left":
					// Place to the left of current node
					newNodeX = nodeX - TARGET_NODE_SIZE.width - NODE_SPACING;
					newNodeY = nodeY + (nodeHeight - TARGET_NODE_SIZE.height) / 2;
					break;
				case "top-center":
					// Place above current node
					newNodeX = nodeX + (nodeWidth - TARGET_NODE_SIZE.width) / 2;
					newNodeY = nodeY - TARGET_NODE_SIZE.height - NODE_SPACING;
					break;
				case "bottom-center":
					// Place below current node
					newNodeX = nodeX + (nodeWidth - TARGET_NODE_SIZE.width) / 2;
					newNodeY = nodeY + nodeHeight + NODE_SPACING;
					break;
			}

			// Create new node
			const newNodeId = generateId();
			const newNode: CanvasNode<CauseMapNodeData> = {
				id: newNodeId,
				type: "cause-map",
				position: { x: newNodeX, y: newNodeY },
				size: { ...TARGET_NODE_SIZE },
				zIndex: Math.max(...canvasState.nodes.map((n) => n.zIndex), 0) + 1,
				data: {
					text: "",
					status: "pending",
				},
			};

			// Determine connections based on anchor direction
			const connectionsToAdd: Array<{ from: string; to: string }> = [];

			if (anchorName === "middle-right") {
				// Adding a child node to the right
				const existingChildIds = getChildIds(node.id, causeMapEdges);
				if (existingChildIds.length === 0) {
					// No existing children, just add connection from current to new
					connectionsToAdd.push({ from: node.id, to: newNodeId });
				} else {
					// Has children - insert new node between current and its children
					// Connect current -> new, and new -> all existing children
					connectionsToAdd.push({ from: node.id, to: newNodeId });
					existingChildIds.forEach((childId) => {
						connectionsToAdd.push({ from: newNodeId, to: childId });
					});
				}
			} else if (anchorName === "middle-left") {
				// Adding a parent node to the left
				const existingParentIds = getParentIds(node.id, causeMapEdges);
				if (existingParentIds.length === 0) {
					// No existing parents, just add connection from new to current
					connectionsToAdd.push({ from: newNodeId, to: node.id });
				} else {
					// Has parents - insert new node between parents and current
					// Connect all parents -> new, and new -> current
					existingParentIds.forEach((parentId) => {
						connectionsToAdd.push({ from: parentId, to: newNodeId });
					});
					connectionsToAdd.push({ from: newNodeId, to: node.id });
				}
			} else if (
				anchorName === "top-center" ||
				anchorName === "bottom-center"
			) {
				// Adding a sibling node
				const existingParentIds = getParentIds(node.id, causeMapEdges);
				if (existingParentIds.length > 0) {
					// Connect the same parents to the new sibling
					existingParentIds.forEach((parentId) => {
						connectionsToAdd.push({ from: parentId, to: newNodeId });
					});
				} else {
					// Root node case - connect new node to same children as current
					const existingChildIds = getChildIds(node.id, causeMapEdges);
					existingChildIds.forEach((childId) => {
						connectionsToAdd.push({ from: newNodeId, to: childId });
					});
				}
			}

			// Create connection objects
			const newConnections: Connection[] = connectionsToAdd.map((conn) => ({
				id: generateId(),
				from: conn.from,
				to: conn.to,
			}));

			// Apply updates: add node, select it, and add connections
			onUpdateNodes({
				add: [newNode],
				select: [newNodeId],
				addConnections: newConnections,
			});
		},
		[node, canvasState, causeMapEdges, onUpdateNodes]
	);

	// Don't render if conditions aren't met
	if (!shouldShowAnchors || anchorsToShow.length === 0) {
		return null;
	}

	// Calculate preview node position if hovering (uses same fixed spacing as actual placement)
	const previewPosition = hoveredAnchor
		? (() => {
				const nodeWidth = node.size.width;
				const nodeHeight = node.size.height;

				switch (hoveredAnchor) {
					case "middle-right":
						return {
							x: nodeWidth + NODE_SPACING,
							y: (nodeHeight - TARGET_NODE_SIZE.height) / 2,
						};
					case "middle-left":
						return {
							x: -TARGET_NODE_SIZE.width - NODE_SPACING,
							y: (nodeHeight - TARGET_NODE_SIZE.height) / 2,
						};
					case "top-center":
						return {
							x: (nodeWidth - TARGET_NODE_SIZE.width) / 2,
							y: -TARGET_NODE_SIZE.height - NODE_SPACING,
						};
					case "bottom-center":
						return {
							x: (nodeWidth - TARGET_NODE_SIZE.width) / 2,
							y: nodeHeight + NODE_SPACING,
						};
					default:
						return { x: 0, y: 0 };
				}
			})()
		: null;

	// Calculate connection line endpoints for preview
	const connectionPreview =
		hoveredAnchor && previewPosition
			? (() => {
					const nodeCenter = {
						x: node.size.width / 2,
						y: node.size.height / 2,
					};
					const previewCenter = {
						x: previewPosition.x + TARGET_NODE_SIZE.width / 2,
						y: previewPosition.y + TARGET_NODE_SIZE.height / 2,
					};

					// For left/right anchors, connection flows horizontally
					if (hoveredAnchor === "middle-right") {
						return {
							fromX: node.size.width,
							fromY: nodeCenter.y,
							toX: previewPosition.x,
							toY: previewCenter.y,
						};
					} else if (hoveredAnchor === "middle-left") {
						return {
							fromX: previewPosition.x + TARGET_NODE_SIZE.width,
							fromY: previewCenter.y,
							toX: 0,
							toY: nodeCenter.y,
						};
					}

					// For top/bottom, no direct connection preview
					return null;
				})()
			: null;

	return (
		<>
			{/* Render anchors */}
			{anchorsToShow.map((anchorName) => {
				const pos = getAnchorPosition(
					node.size.width,
					node.size.height,
					anchorName,
					anchorPadding
				);

				return (
					<Anchor
						key={anchorName}
						position={anchorName}
						x={pos.x}
						y={pos.y}
						scale={canvasScale}
						isHovered={hoveredAnchor === anchorName}
						onHover={(hovered) => setHoveredAnchor(hovered ? anchorName : null)}
						onClick={() => handleAnchorClick(anchorName)}
					/>
				);
			})}

			{/* Preview node on hover */}
			{previewPosition && (
				<PreviewNode
					x={previewPosition.x}
					y={previewPosition.y}
					width={TARGET_NODE_SIZE.width}
					height={TARGET_NODE_SIZE.height}
				/>
			)}

			{/* Preview connection line on hover */}
			{connectionPreview && (
				<PreviewConnection
					fromX={connectionPreview.fromX}
					fromY={connectionPreview.fromY}
					toX={connectionPreview.toX}
					toY={connectionPreview.toY}
				/>
			)}
		</>
	);
};
