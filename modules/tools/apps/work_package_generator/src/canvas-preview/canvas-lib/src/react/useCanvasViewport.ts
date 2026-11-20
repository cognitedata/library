import {
	type MutableRefObject,
	useEffect,
	useLayoutEffect,
	useRef,
	useState,
} from "react";
import type { Size, ViewportState } from "../core";
import { pan, zoomAtPoint } from "../core";
import {
	updatePanning,
	updateScale,
	updateViewportBounds,
} from "../core/viewportSignals";
import type { InteractionState } from "./canvas-interaction";

export function useCanvasViewport({
	containerRef,
	viewport,
	setViewport,
	isPanningProp,
	interactionRef,
	minZoom,
	maxZoom,
}: {
	containerRef: MutableRefObject<HTMLDivElement | null>;
	viewport: ViewportState;
	setViewport: (viewport: ViewportState) => void;
	isPanningProp: boolean;
	interactionRef: MutableRefObject<InteractionState>;
	minZoom: number;
	maxZoom: number;
}) {
	const [containerSize, setContainerSize] = useState<Size>({
		width: 1000,
		height: 800,
	});
	const [isWheelPanning, setIsWheelPanning] = useState(false);
	const wheelPanTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

	useLayoutEffect(() => {
		const container = containerRef.current;
		if (!container) return;

		const updateSize = () => {
			const rect = container.getBoundingClientRect();
			setContainerSize({ width: rect.width, height: rect.height });
		};

		updateSize();

		const resizeObserver = new ResizeObserver(updateSize);
		resizeObserver.observe(container);

		return () => resizeObserver.disconnect();
	}, [containerRef]);

	useEffect(() => {
		updateViewportBounds(viewport, containerSize);
	}, [viewport, containerSize]);

	useEffect(() => {
		updatePanning(isPanningProp || interactionRef.current.mode === "panning");
	}, [isPanningProp, interactionRef]);

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

	useEffect(() => {
		const container = containerRef.current;
		if (!container) return;

		const handleWheel = (e: WheelEvent) => {
			e.preventDefault();
			const currentViewport = viewportRef.current;

			if (e.ctrlKey || e.metaKey) {
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

				if (newViewport.scale !== currentViewport.scale) {
					updateScale(newViewport.scale);
				}
			} else {
				const newViewport = pan(currentViewport, -e.deltaX, -e.deltaY);
				setViewportRef.current(newViewport);
			}

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
	}, [containerRef]);

	return { containerSize, isWheelPanning };
}
