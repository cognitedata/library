import { useState, useRef, useCallback, useEffect } from 'react';
import type React from 'react';

const ZERO_PAN = { x: 0, y: 0 };

export interface ViewportOptions {
  zoom?: number;
  onZoomChange?: (zoom: number) => void;
  minZoom?: number;
  maxZoom?: number;
  panOffset?: { x: number; y: number };
  onPanChange?: (offset: { x: number; y: number }) => void;
}

/** Get distance between two touch points. */
function getTouchDistance(t1: Touch, t2: Touch): number {
  const dx = t1.clientX - t2.clientX;
  const dy = t1.clientY - t2.clientY;
  return Math.hypot(dx, dy);
}

/** Get midpoint between two touch points. */
function getTouchCenter(t1: Touch, t2: Touch): { x: number; y: number } {
  return {
    x: (t1.clientX + t2.clientX) / 2,
    y: (t1.clientY + t2.clientY) / 2,
  };
}

export function useViewport(options: ViewportOptions) {
  const {
    zoom: controlledZoom,
    onZoomChange,
    minZoom = 0.25,
    maxZoom = 5,
    panOffset: controlledPan,
    onPanChange,
  } = options;

  // -- Zoom state (controlled + uncontrolled) --
  const [internalZoom, setInternalZoom] = useState(1);
  const currentZoom = controlledZoom ?? internalZoom;

  const clampZoom = useCallback(
    (z: number) => Math.min(maxZoom, Math.max(minZoom, z)),
    [minZoom, maxZoom],
  );

  const handleZoomChange = useCallback(
    (newZoom: number) => {
      const clamped = clampZoom(newZoom);
      setInternalZoom(clamped);
      onZoomChange?.(clamped);
    },
    [onZoomChange, clampZoom],
  );

  // -- Pan state (controlled + uncontrolled) --
  const [internalPan, setInternalPan] = useState(ZERO_PAN);
  const currentPan = controlledPan ?? internalPan;

  const handlePanChange = useCallback(
    (offset: { x: number; y: number }) => {
      setInternalPan(offset);
      onPanChange?.(offset);
    },
    [onPanChange],
  );

  const effectivePan = currentZoom <= 1 ? ZERO_PAN : currentPan;

  // -- Stable refs for event handlers --
  const currentZoomRef = useRef(currentZoom);
  currentZoomRef.current = currentZoom;
  const currentPanRef = useRef(currentPan);
  currentPanRef.current = currentPan;
  const clampZoomRef = useRef(clampZoom);
  clampZoomRef.current = clampZoom;
  const handleZoomChangeRef = useRef(handleZoomChange);
  handleZoomChangeRef.current = handleZoomChange;
  const handlePanChangeRef = useRef(handlePanChange);
  handlePanChangeRef.current = handlePanChange;

  // -- Container dimensions --
  const [containerDims, setContainerDims] = useState({ width: 0, height: 0 });
  const viewportObserverRef = useRef<ResizeObserver | null>(null);
  const eventCleanupRef = useRef<(() => void) | null>(null);

  // -- Touch gesture state (stored in ref to avoid re-renders during gesture) --
  const touchStateRef = useRef<{
    initialDistance: number;
    initialZoom: number;
    initialPan: { x: number; y: number };
    initialCenter: { x: number; y: number };
    initialRect: DOMRect;
  } | null>(null);

  const viewportRef = useCallback((node: HTMLDivElement | null) => {
    eventCleanupRef.current?.();
    eventCleanupRef.current = null;
    viewportObserverRef.current?.disconnect();
    viewportObserverRef.current = null;

    if (node) {
      const measure = () => {
        const w = node.clientWidth;
        const h = node.clientHeight;
        setContainerDims((prev) =>
          prev.width === w && prev.height === h ? prev : { width: w, height: h },
        );
      };
      const observer = new ResizeObserver(measure);
      observer.observe(node);
      measure();
      viewportObserverRef.current = observer;

      // Ctrl/Cmd + wheel → zoom toward cursor
      const wheelHandler = (e: WheelEvent) => {
        // Ctrl/Cmd + wheel → zoom toward cursor
        if (e.ctrlKey || e.metaKey) {
          e.preventDefault();
          const oldZoom = currentZoomRef.current;
          const factor = e.deltaY > 0 ? 0.9 : 1.1;
          const newZoom = clampZoomRef.current(oldZoom * factor);
          if (newZoom === oldZoom) return;
          const rect = node.getBoundingClientRect();
          const cx = e.clientX - rect.left;
          const cy = e.clientY - rect.top;
          const pan = currentPanRef.current;
          const ratio = newZoom / oldZoom;
          handleZoomChangeRef.current(newZoom);
          handlePanChangeRef.current({
            x: cx - (cx - pan.x) * ratio,
            y: cy - (cy - pan.y) * ratio,
          });
          return;
        }

        // Wheel/trackpad scroll → pan when zoomed in
        if (currentZoomRef.current > 1) {
          e.preventDefault();
          const pan = currentPanRef.current;
          handlePanChangeRef.current({
            x: pan.x - e.deltaX,
            y: pan.y - e.deltaY,
          });
        }
      };

      // Touch: pinch-to-zoom + two-finger pan
      const touchStartHandler = (e: TouchEvent) => {
        if (e.touches.length !== 2) return;
        e.preventDefault();
        const t1 = e.touches[0];
        const t2 = e.touches[1];
        touchStateRef.current = {
          initialDistance: getTouchDistance(t1, t2),
          initialZoom: currentZoomRef.current,
          initialPan: { ...currentPanRef.current },
          initialCenter: getTouchCenter(t1, t2),
          initialRect: node.getBoundingClientRect(),
        };
      };

      const touchMoveHandler = (e: TouchEvent) => {
        if (e.touches.length !== 2 || !touchStateRef.current) return;
        e.preventDefault();
        const t1 = e.touches[0];
        const t2 = e.touches[1];
        const { initialDistance, initialZoom, initialPan, initialCenter, initialRect } = touchStateRef.current;

        // Zoom
        const currentDistance = getTouchDistance(t1, t2);
        const scale = currentDistance / initialDistance;
        const newZoom = clampZoomRef.current(initialZoom * scale);
        handleZoomChangeRef.current(newZoom);

        // Pan toward pinch center (use cached rect to avoid layout thrashing)
        const center = getTouchCenter(t1, t2);
        const cx = initialCenter.x - initialRect.left;
        const cy = initialCenter.y - initialRect.top;
        const ratio = newZoom / initialZoom;
        handlePanChangeRef.current({
          x: cx - (cx - initialPan.x) * ratio + (center.x - initialCenter.x),
          y: cy - (cy - initialPan.y) * ratio + (center.y - initialCenter.y),
        });
      };

      const touchEndHandler = () => {
        touchStateRef.current = null;
      };

      node.addEventListener('wheel', wheelHandler, { passive: false });
      node.addEventListener('touchstart', touchStartHandler, { passive: false });
      node.addEventListener('touchmove', touchMoveHandler, { passive: false });
      node.addEventListener('touchend', touchEndHandler);
      node.addEventListener('touchcancel', touchEndHandler);

      eventCleanupRef.current = () => {
        node.removeEventListener('wheel', wheelHandler);
        node.removeEventListener('touchstart', touchStartHandler);
        node.removeEventListener('touchmove', touchMoveHandler);
        node.removeEventListener('touchend', touchEndHandler);
        node.removeEventListener('touchcancel', touchEndHandler);
      };
    }
  }, []);

  useEffect(() => {
    return () => {
      eventCleanupRef.current?.();
      viewportObserverRef.current?.disconnect();
    };
  }, []);

  // -- Drag to pan (when zoomed in) --
  const [isDragging, setIsDragging] = useState(false);
  const dragStart = useRef(ZERO_PAN);
  const panStart = useRef(ZERO_PAN);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (currentZoomRef.current <= 1) return;
    if (e.button !== 1) return; // middle-click only
    e.preventDefault();
    setIsDragging(true);
    dragStart.current = { x: e.clientX, y: e.clientY };
    panStart.current = currentPanRef.current;
  }, []);

  useEffect(() => {
    if (!isDragging) return;
    const handleMouseMove = (e: MouseEvent) => {
      handlePanChangeRef.current({
        x: panStart.current.x + (e.clientX - dragStart.current.x),
        y: panStart.current.y + (e.clientY - dragStart.current.y),
      });
    };
    const handleMouseUp = () => setIsDragging(false);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isDragging]);

  const cursor = isDragging ? 'grabbing' : currentZoom > 1 ? 'grab' : 'default';

  return {
    currentZoom,
    effectivePan,
    containerDims,
    viewportRef,
    cursor,
    handleMouseDown,
    handleZoomChange,
    handlePanChange,
  };
}

export function computeBaseWidth(
  fitMode: 'width' | 'page' | undefined,
  explicitWidth: number | undefined,
  containerDims: { width: number; height: number },
  naturalSize: { width: number; height: number } | null,
): number | undefined {
  if (!fitMode || containerDims.width <= 0) return explicitWidth;

  if (fitMode === 'width') return containerDims.width;

  if (fitMode === 'page' && naturalSize && naturalSize.height > 0 && containerDims.height > 0) {
    const aspect = naturalSize.width / naturalSize.height;
    const containerAspect = containerDims.width / containerDims.height;
    return containerAspect > aspect
      ? containerDims.height * aspect
      : containerDims.width;
  }

  return explicitWidth;
}
