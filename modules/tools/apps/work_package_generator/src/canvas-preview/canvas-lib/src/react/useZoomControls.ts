/**
 * useZoomControls Hook
 * Provides zoom control functionality with smooth animations
 */

import { useCallback, useEffect, useRef } from "react";
import type { CanvasNode, ViewportState } from "../core";
import { fitBoundsInView, getAllNodesBounds } from "../core";

export interface UseZoomControlsOptions {
	/** Current viewport state */
	viewport: ViewportState;
	/** Callback to update viewport */
	setViewport: (viewport: ViewportState) => void;
	/** Ref to the canvas container element */
	containerRef: React.RefObject<HTMLDivElement | null>;
	/** Current nodes for zoom-to-fit calculation */
	nodes: CanvasNode[];
	/** Padding around content for zoom-to-fit (default: 50) */
	fitPadding?: number;
	/** Minimum zoom scale (default: 0.05) */
	minScale?: number;
	/** Maximum zoom scale (default: 4.0) */
	maxScale?: number;
	/** Automatically zoom-to-fit once when nodes first become available (default: false) */
	fitOnLoad?: boolean;
}

export interface UseZoomControlsResult {
	/** Animate viewport to a target state */
	animateToViewport: (targetViewport: ViewportState, duration?: number) => void;
	/** Zoom to a specific scale, keeping the viewport center fixed */
	zoomTo: (scale: number) => void;
	/** Zoom to fit all nodes in view */
	zoomToFit: () => void;
}

/**
 * Hook for managing zoom controls with smooth animations
 */
export function useZoomControls({
	viewport,
	setViewport,
	containerRef,
	nodes,
	fitPadding = 50,
	minScale = 0.05,
	maxScale = 4.0,
	fitOnLoad = false,
}: UseZoomControlsOptions): UseZoomControlsResult {
	// Track animation frame for cleanup
	const animationFrameRef = useRef<number | null>(null);

	// Track whether fitOnLoad has already fired
	const hasFittedRef = useRef(false);

	// Store viewport in ref for animation closure
	const viewportRef = useRef(viewport);
	useEffect(() => {
		viewportRef.current = viewport;
	}, [viewport]);

	/**
	 * Animate viewport changes smoothly using requestAnimationFrame
	 */
	const animateToViewport = useCallback(
		(targetViewport: ViewportState, duration: number = 300) => {
			// Cancel any existing animation
			if (animationFrameRef.current !== null) {
				cancelAnimationFrame(animationFrameRef.current);
			}

			const startViewport = { ...viewportRef.current };
			const startTime = performance.now();

			const animate = (currentTime: number) => {
				const elapsed = currentTime - startTime;
				const progress = Math.min(elapsed / duration, 1);

				// Ease out cubic for smooth deceleration
				const eased = 1 - (1 - progress) ** 3;

				const newViewport: ViewportState = {
					x: startViewport.x + (targetViewport.x - startViewport.x) * eased,
					y: startViewport.y + (targetViewport.y - startViewport.y) * eased,
					scale:
						startViewport.scale +
						(targetViewport.scale - startViewport.scale) * eased,
				};

				setViewport(newViewport);

				if (progress < 1) {
					animationFrameRef.current = requestAnimationFrame(animate);
				} else {
					animationFrameRef.current = null;
				}
			};

			animationFrameRef.current = requestAnimationFrame(animate);
		},
		[setViewport]
	);

	/**
	 * Zoom to a specific scale, keeping the viewport center fixed
	 */
	const zoomTo = useCallback(
		(newScale: number) => {
			const container = containerRef.current;
			if (!container) {
				setViewport({ ...viewportRef.current, scale: newScale });
				return;
			}

			const rect = container.getBoundingClientRect();
			const centerX = rect.width / 2;
			const centerY = rect.height / 2;

			const currentViewport = viewportRef.current;

			// Calculate the canvas point at the center
			const canvasCenterX =
				(centerX - currentViewport.x) / currentViewport.scale;
			const canvasCenterY =
				(centerY - currentViewport.y) / currentViewport.scale;

			// Calculate new viewport position to keep the same point at center
			const newX = centerX - canvasCenterX * newScale;
			const newY = centerY - canvasCenterY * newScale;

			animateToViewport(
				{
					x: newX,
					y: newY,
					scale: newScale,
				},
				200
			);
		},
		[containerRef, setViewport, animateToViewport]
	);

	/**
	 * Zoom to fit all nodes in view with padding
	 */
	const zoomToFit = useCallback(() => {
		if (nodes.length === 0) return;

		const container = containerRef.current;
		if (!container) return;

		const rect = container.getBoundingClientRect();
		if (rect.width < 1 || rect.height < 1) return;
		const containerSize = { width: rect.width, height: rect.height };

		// Calculate bounding box of all nodes
		const bounds = getAllNodesBounds(nodes);
		if (!bounds) return;

		// Calculate viewport to fit all nodes
		const targetViewport = fitBoundsInView(
			bounds,
			containerSize,
			fitPadding,
			minScale,
			maxScale
		);

		animateToViewport(targetViewport, 400);
	}, [nodes, containerRef, fitPadding, minScale, maxScale, animateToViewport]);

	// Auto zoom-to-fit once the container has a real size. Nodes often arrive
	// before layout, and a zero-size fit would lock the viewport off-center.
	useEffect(() => {
		if (!fitOnLoad || nodes.length === 0 || hasFittedRef.current) return;
		const container = containerRef.current;
		if (!container) return;

		const tryFit = () => {
			if (hasFittedRef.current) return;
			const rect = container.getBoundingClientRect();
			if (rect.width < 1 || rect.height < 1) return;
			hasFittedRef.current = true;
			zoomToFit();
		};

		tryFit();
		const observer = new ResizeObserver(tryFit);
		observer.observe(container);
		return () => observer.disconnect();
	}, [fitOnLoad, nodes.length, zoomToFit, containerRef]);

	return {
		animateToViewport,
		zoomTo,
		zoomToFit,
	};
}
