import { useCallback, useRef } from "react";
import type { CanvasNode, Connection } from "../core";

export function useCanvasNodeMutations(args: {
	nodes: CanvasNode[];
	selection: string[];
	connections: Connection[];
	onNodesChange?: (nodes: CanvasNode[]) => void;
	onNodeDataChange?: (nodeId: string, data: unknown) => void;
	onSelectionChange?: (selection: string[]) => void;
	onConnectionsChange?: (connections: Connection[]) => void;
	setFocusedNodeId: (nodeId: string | null) => void;
}) {
	const {
		nodes,
		selection,
		connections,
		onNodesChange,
		onNodeDataChange,
		onSelectionChange,
		onConnectionsChange,
		setFocusedNodeId,
	} = args;
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

	return {
		handleNodeDataChange,
		handleNodeSizeChange,
		handleNodeRotate,
		updateSelectedNodes,
		rotateSelectedNodes,
		getNodeSizeChangeCallback,
		getNodeDataChangeCallback,
		getNodeRotateCallback,
		handleUpdateNodes,
		handleAddConnection,
		handleNodeFocus,
		handleNodeBlur,
	};
}
