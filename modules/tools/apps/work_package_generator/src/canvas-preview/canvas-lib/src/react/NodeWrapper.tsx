/**
 * NodeWrapper component
 * Handles mounting/unmounting, transitions, selection UI, virtualization, and rotation
 *
 * Virtualization behavior based on visibility zone:
 * - 'visible': Full render with high fidelity
 * - 'near': Mounted but may use reduced fidelity placeholder
 * - 'culled': Not rendered (handled by Canvas, but kept as fallback)
 */

import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type {
	CanvasNode,
	CanvasState,
	Connection,
	NodeComponentProps,
	NodeTypePlugin,
	TransitionResult,
} from "../core/types";
import type { VisibilityZone } from "../core/viewportSignals";
import { fidelityLevel, isInteracting } from "../core/viewportSignals";

// ============================================================================
// Types
// ============================================================================

interface NodeWrapperProps {
	node: CanvasNode;
	plugin: NodeTypePlugin;
	isSelected: boolean;
	isFocused: boolean;
	canvasScale: number;
	isCanvasPanning: boolean;
	onDataChange: (data: unknown) => void;
	onSizeChange: (size: { width: number; height: number }) => void;
	onRotate: (degrees: number) => void;
	onFocus: () => void;
	onBlur: () => void;
	// Computed absolute position and size (for child nodes in hierarchy)
	computedPosition?: { x: number; y: number };
	computedSize?: { width: number; height: number };
	// Hierarchy state
	isDropTarget?: boolean;
	willDetach?: boolean;
	// Visibility for virtualization
	visibilityZone?: VisibilityZone;
	// Selection overlay support
	selectedNodes?: CanvasNode[];
	canvasState?: CanvasState;
	onUpdateNodes?: (updates: {
		add?: CanvasNode[];
		update?: Array<{ id: string; updates: Partial<CanvasNode> }>;
		remove?: string[];
		select?: string[];
		addConnections?: Connection[];
	}) => void;
	onAddConnection?: (connection: Connection) => void;
	readOnly?: boolean;
}

// ============================================================================
// Component
// ============================================================================

export const NodeWrapper: React.FC<NodeWrapperProps> = ({
	node,
	plugin,
	isSelected,
	isFocused,
	canvasScale,
	isCanvasPanning,
	onDataChange,
	onSizeChange,
	onRotate: _onRotate, // Reserved for future rotation handle interaction
	onFocus,
	onBlur,
	computedPosition,
	computedSize,
	isDropTarget = false,
	willDetach = false,
	visibilityZone = "visible",
	selectedNodes = [],
	canvasState,
	onUpdateNodes,
	onAddConnection,
	readOnly = false,
}) => {
	// Use computed position/size if provided (for child nodes), otherwise use node values
	const displayPosition = computedPosition ?? node.position;
	const displaySize = computedSize ?? node.size;
	const rotation = node.rotation ?? 0;
	const [isMounted, setIsMounted] = useState(plugin.stayMounted ?? true);
	const [placeholder, setPlaceholder] = useState<React.ReactNode>(null);
	const [restoreData, setRestoreData] = useState<unknown>(null);
	const unmountTimeoutRef = useRef<number | null>(null);
	// Stores a callback that, when called with a result handler, will provide the transition result
	const transitionCallbackRef = useRef<
		((resultHandler: (result: TransitionResult) => void) => void) | null
	>(null);

	// Track previous visibility to detect transitions
	const prevVisibilityRef = useRef<VisibilityZone>(visibilityZone);

	const Component = plugin.component;
	const Placeholder = plugin.placeholder;

	// Subscribe to fidelity level for progressive rendering
	const currentFidelity = fidelityLevel.value;
	const currentlyInteracting = isInteracting.value;

	// Determine if we should show full component or placeholder based on visibility
	// - 'visible' + high fidelity: full component
	// - 'visible' + low fidelity: full component (but component can render simplified)
	// - 'near': placeholder to save resources
	// - 'culled': handled by Canvas (not rendered at all)
	const shouldShowPlaceholder = useMemo(() => {
		// Always show full component if focused or selected
		if (isFocused || isSelected) return false;

		// If plugin requires staying mounted, don't use placeholder
		if (plugin.stayMounted) return false;

		// Show placeholder for 'near' visibility (just outside viewport)
		if (visibilityZone === "near") return true;

		return false;
	}, [visibilityZone, isFocused, isSelected, plugin.stayMounted]);

	// Handle mounting based on focus AND visibility
	useEffect(() => {
		// Track visibility transitions
		const isVisible = visibilityZone === "visible";
		prevVisibilityRef.current = visibilityZone;

		// For stayMounted plugins, ensure mounted state is correct
		// Using early return avoids unnecessary state updates
		if (plugin.stayMounted) {
			if (!isMounted) setIsMounted(true);
			return;
		}

		// Always mount when visible or focused
		if (isVisible || isFocused) {
			// Clear any pending unmount
			if (unmountTimeoutRef.current) {
				clearTimeout(unmountTimeoutRef.current);
				unmountTimeoutRef.current = null;
			}
			setIsMounted(true);
		} else if (isMounted) {
			// Transitioning from visible to not visible, or losing focus
			// Schedule unmount after delay
			const delay = plugin.unmountDelay ?? 2000;

			// For nodes moving outside viewport, use shorter delay
			const actualDelay =
				visibilityZone === "near" ? Math.min(delay, 500) : delay;

			// First, try to get transition result
			if (transitionCallbackRef.current) {
				transitionCallbackRef.current((result) => {
					setPlaceholder(result.placeholder);
					setRestoreData(result.restoreData);
					result.cleanup?.();
				});
			}

			unmountTimeoutRef.current = window.setTimeout(() => {
				setIsMounted(false);
				unmountTimeoutRef.current = null;
			}, actualDelay);
		}

		return () => {
			if (unmountTimeoutRef.current) {
				clearTimeout(unmountTimeoutRef.current);
			}
		};
	}, [
		isFocused,
		isMounted,
		visibilityZone,
		plugin.stayMounted,
		plugin.unmountDelay,
	]);

	// Register transition callback - components call this with a function that
	// when invoked with a result handler, will provide the transition result
	const handleTransitionOut = useCallback(
		(callback: (resultHandler: (result: TransitionResult) => void) => void) => {
			transitionCallbackRef.current = callback;
		},
		[]
	);

	// Handle double-click to focus (for components that need explicit interaction)
	const handleDoubleClick = useCallback(
		(e: React.MouseEvent) => {
			e.stopPropagation();
			onFocus();
			if (!isMounted) {
				setIsMounted(true);
			}
		},
		[onFocus, isMounted]
	);

	// Render the appropriate content based on mount state and visibility
	const renderContent = () => {
		// Not mounted - show placeholder
		if (!isMounted || shouldShowPlaceholder) {
			// Show transition placeholder if available
			if (placeholder) {
				return placeholder;
			}
			// Show plugin's custom placeholder if available
			if (Placeholder) {
				return (
					<Placeholder
						data={node.data}
						size={displaySize}
						transitionPlaceholder={placeholder}
						restoreData={restoreData}
					/>
				);
			}
			// Default placeholder for virtualized nodes
			return (
				<div
					style={{
						width: "100%",
						height: "100%",
						backgroundColor: visibilityZone === "near" ? "#fafafa" : "#f0f0f0",
						border: "1px solid #ddd",
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						color: "#999",
						fontSize: 14,
					}}
				>
					{plugin.displayName}
				</div>
			);
		}

		// Render the actual component with fidelity hints
		const props: NodeComponentProps = {
			data: node.data,
			onDataChange,
			size: displaySize,
			isSelected,
			isFocused,
			canvasScale,
			isCanvasPanning: isCanvasPanning || currentlyInteracting,
			onTransitionOut: handleTransitionOut,
			onSizeChange,
			restoreData,
			readOnly,
			rotation,
		};

		return <Component {...props} />;
	};

	// Compute outline style based on state
	// Scale border thickness with zoom to maintain consistent screen pixel width
	//
	// Drop-target and detach states use box-shadow / outline since they are
	// transient decorations on top of normal content.
	// The *selection* border is rendered as a separate overlay <div> (see below)
	// so it reliably paints on top of child stacking contexts such as uPlot's
	// <canvas> elements, which can occlude CSS outline/box-shadow.
	const getOutlineStyle = (): React.CSSProperties => {
		const borderWidth = 2 / canvasScale; // Always 2 screen pixels

		if (isDropTarget) {
			const dropWidth = 3 / canvasScale;
			return {
				boxShadow: `0 0 0 ${dropWidth}px #00cc66, 0 0 20px rgba(0, 204, 102, 0.4)`,
			};
		}
		if (willDetach) {
			return {
				outline: `${borderWidth}px dashed #ff6600`,
				outlineOffset: 2 / canvasScale,
			};
		}
		return {};
	};

	// Get the toolbar component if plugin provides one

	// Get the selection overlay component if plugin provides one
	const SelectionOverlayComponent = plugin.selectionOverlay;

	return (
		<div
			data-node-id={node.id}
			data-node-type={node.type}
			data-selected={isSelected}
			data-focused={isFocused}
			data-mounted={isMounted}
			data-parent-id={node.parentId}
			data-drop-target={isDropTarget}
			data-will-detach={willDetach}
			data-visibility={visibilityZone}
			data-fidelity={currentFidelity}
			data-rotation={rotation}
			style={{
				position: "absolute",
				left: displayPosition.x,
				top: displayPosition.y,
				width: displaySize.width,
				height: displaySize.height,
				boxSizing: "border-box",
				...getOutlineStyle(),
				cursor: isCanvasPanning ? "inherit" : "move",
				pointerEvents: isCanvasPanning ? "none" : undefined,
				userSelect: "none",
				WebkitUserSelect: "none",
				transition:
					isDropTarget || willDetach
						? "outline 0.15s ease, box-shadow 0.15s ease"
						: "none",
				// Apply rotation transform
				transform: rotation !== 0 ? `rotate(${rotation}deg)` : undefined,
				transformOrigin: "center center",
			}}
			onDoubleClick={handleDoubleClick}
			onBlur={onBlur}
		>
			{renderContent()}

			{/* Selection border overlay — rendered as a sibling div so it reliably
          paints on top of child stacking contexts (e.g. uPlot <canvas> elements)
          which can occlude CSS outline / box-shadow on the parent. */}
			{isSelected && (
				<div
					style={{
						position: "absolute",
						inset: 0,
						border: `${2 / canvasScale}px solid #0066ff`,
						pointerEvents: "none",
						zIndex: 999,
						borderRadius: "inherit",
					}}
				/>
			)}

			{/* Resize handles (shown when selected) */}
			{isSelected && plugin.resizable != null && (
				<>
					{/* Corner handles are always shown for resizable nodes */}
					<ResizeHandle position="nw" scale={canvasScale} />
					<ResizeHandle position="ne" scale={canvasScale} />
					<ResizeHandle position="se" scale={canvasScale} />
					<ResizeHandle position="sw" scale={canvasScale} />
					{/* Edge handles only shown when aspect ratio is not locked (uniform scaling only) */}
					{plugin.resizable?.aspectRatio !== "lock" && (
						<>
							<ResizeHandle position="n" scale={canvasScale} />
							<ResizeHandle position="e" scale={canvasScale} />
							<ResizeHandle position="s" scale={canvasScale} />
							<ResizeHandle position="w" scale={canvasScale} />
						</>
					)}
				</>
			)}

			{/* Selection overlay (shown when selected and plugin provides one) */}
			{isSelected &&
				SelectionOverlayComponent &&
				canvasState &&
				onUpdateNodes && (
					<SelectionOverlayComponent
						node={node}
						canvasScale={canvasScale}
						canvasState={canvasState}
						selectedNodes={selectedNodes}
						onUpdateNodes={onUpdateNodes}
						onAddConnection={onAddConnection}
					/>
				)}
		</div>
	);
};

// ============================================================================
// Resize Handle
// ============================================================================

interface ResizeHandleProps {
	position: "n" | "s" | "e" | "w" | "ne" | "nw" | "se" | "sw";
	scale: number;
}

const ResizeHandle: React.FC<ResizeHandleProps> = ({ position, scale }) => {
	// Scale size with zoom - larger when zoomed in, smaller when zoomed out
	const size = 12 / scale;
	const offset = -size / 2;

	const positionStyles: Record<string, React.CSSProperties> = {
		nw: { top: offset, left: offset, cursor: "nwse-resize" },
		n: {
			top: offset,
			left: "50%",
			transform: "translateX(-50%)",
			cursor: "ns-resize",
		},
		ne: { top: offset, right: offset, cursor: "nesw-resize" },
		e: {
			top: "50%",
			right: offset,
			transform: "translateY(-50%)",
			cursor: "ew-resize",
		},
		se: { bottom: offset, right: offset, cursor: "nwse-resize" },
		s: {
			bottom: offset,
			left: "50%",
			transform: "translateX(-50%)",
			cursor: "ns-resize",
		},
		sw: { bottom: offset, left: offset, cursor: "nesw-resize" },
		w: {
			top: "50%",
			left: offset,
			transform: "translateY(-50%)",
			cursor: "ew-resize",
		},
	};

	return (
		<div
			data-resize-handle={position}
			style={{
				position: "absolute",
				width: size,
				height: size,
				backgroundColor: "#fff",
				// Use box-shadow instead of border for crisp rendering without antialiasing
				boxShadow: `inset 0 0 0 ${1 / scale}px #0066ff`,
				// Circular anchors as shown in reference
				borderRadius: "50%",
				...positionStyles[position],
			}}
		/>
	);
};
