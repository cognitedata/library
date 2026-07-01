import { useCallback, useEffect, useState } from "react";
import type { RefObject } from "react";
import {
  ANNOTATION_PREVIEW_DEFAULTS,
  ANNOTATION_PREVIEW_FIT,
  ANNOTATION_PREVIEW_ZOOM,
} from "@/shared/constants/annotationPreview";

interface ImageDimensions {
  width: number;
  height: number;
}

interface BoundingBox {
  xMin: number;
  yMin: number;
  xMax: number;
  yMax: number;
}

interface UseCanvasViewportOptions {
  containerRef: RefObject<HTMLDivElement | null>;
  imageDimensions: ImageDimensions;
  resetToken?: string | number;
}

interface FocusOptions {
  targetZoom?: number;
}

export function useCanvasViewport({
  containerRef,
  imageDimensions,
  resetToken,
}: UseCanvasViewportOptions) {
  const [zoom, setZoom] = useState<number>(ANNOTATION_PREVIEW_DEFAULTS.zoom);
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const [panOffset, setPanOffset] = useState({ x: 0, y: 0 });

  useEffect(() => {
    setPanOffset({ x: 0, y: 0 });
  }, [resetToken]);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button === 1 || e.button === 2 || e.altKey) {
      e.preventDefault();
      setIsPanning(true);
      setPanStart({ x: e.clientX - panOffset.x, y: e.clientY - panOffset.y });
    }
  }, [panOffset]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (isPanning) {
      setPanOffset({
        x: e.clientX - panStart.x,
        y: e.clientY - panStart.y,
      });
    }
  }, [isPanning, panStart]);

  const handleMouseUp = useCallback(() => {
    setIsPanning(false);
  }, []);

  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.shiftKey) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -ANNOTATION_PREVIEW_ZOOM.wheelDelta : ANNOTATION_PREVIEW_ZOOM.wheelDelta;
      setZoom((z) => Math.max(ANNOTATION_PREVIEW_ZOOM.min, Math.min(ANNOTATION_PREVIEW_ZOOM.max, z + delta)));
    }
  }, []);

  const handleFitToView = useCallback(() => {
    if (!containerRef.current || !imageDimensions.width || !imageDimensions.height) return;
    const containerWidth = containerRef.current.clientWidth - ANNOTATION_PREVIEW_FIT.horizontalPaddingPx;
    const containerHeight = ANNOTATION_PREVIEW_FIT.containerHeightPx;
    const scaleX = containerWidth / imageDimensions.width;
    const scaleY = containerHeight / imageDimensions.height;
    const newZoom = Math.min(scaleX, scaleY, 1);
    setZoom(newZoom);
    setPanOffset({ x: 0, y: 0 });
  }, [containerRef, imageDimensions.width, imageDimensions.height]);

  const resetView = useCallback(() => {
    setZoom(1);
    setPanOffset({ x: 0, y: 0 });
  }, []);

  const focusOnBoundingBox = useCallback((boundingBox: BoundingBox | null, options?: FocusOptions) => {
    if (!boundingBox || !containerRef.current || !imageDimensions.width || !imageDimensions.height) return;

    const requestedZoom = options?.targetZoom ?? ANNOTATION_PREVIEW_ZOOM.focusTarget;
    const nextZoom = Math.max(ANNOTATION_PREVIEW_ZOOM.min, Math.min(ANNOTATION_PREVIEW_ZOOM.max, requestedZoom));
    const centerX = ((boundingBox.xMin + boundingBox.xMax) / 2) * imageDimensions.width;
    const centerY = ((boundingBox.yMin + boundingBox.yMax) / 2) * imageDimensions.height;
    const containerWidth = containerRef.current.clientWidth;
    const containerHeight = containerRef.current.clientHeight;

    setZoom(nextZoom);

    setPanOffset({
      x: containerWidth / 2 - centerX * nextZoom,
      y: containerHeight / 2 - centerY * nextZoom,
    });
  }, [containerRef, imageDimensions.width, imageDimensions.height]);

  return {
    zoom,
    setZoom,
    isPanning,
    panOffset,
    setPanOffset,
    handleMouseDown,
    handleMouseMove,
    handleMouseUp,
    handleWheel,
    handleFitToView,
    resetView,
    focusOnBoundingBox,
  };
}
