import { useCallback, useEffect, useMemo, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent, RefObject } from "react";
import { ANNOTATION_NAVIGATOR_BEHAVIOR } from "@/shared/constants/annotationPreview";

interface AnnotationMatch {
  id: string;
}

interface UseAnnotationPreviewInteractionOptions {
  annotationMatches: AnnotationMatch[];
  activeMatchIndex: number;
  searchResultsListRef: RefObject<HTMLDivElement>;
  handleSearchKeyDown: (event: ReactKeyboardEvent<HTMLInputElement>) => void;
  handleResultClick: (index: number) => void;
  handleResultDoubleClick: (index: number) => void;
  handleOverlayClickById: (id: string) => void;
  handleOverlayDoubleClickById: (id: string) => void;
}

export function useAnnotationPreviewInteraction({
  annotationMatches,
  activeMatchIndex,
  searchResultsListRef,
  handleSearchKeyDown,
  handleResultClick,
  handleResultDoubleClick,
  handleOverlayClickById,
  handleOverlayDoubleClickById,
}: UseAnnotationPreviewInteractionOptions) {
  const [hoveredAnnotation, setHoveredAnnotation] = useState<string | null>(null);
  const [selectedAnnotationId, setSelectedAnnotationId] = useState<string | null>(null);
  const [navigatorOpenSignal, setNavigatorOpenSignal] = useState<number | null>(null);
  const [navigatorScrollSignal, setNavigatorScrollSignal] = useState<number | null>(null);

  const triggerNavigatorScroll = useCallback(() => {
    setNavigatorScrollSignal((previous) => (previous ?? 0) + 1);
  }, []);

  const triggerNavigatorOpen = useCallback(() => {
    setNavigatorOpenSignal((previous) => (previous ?? 0) + 1);
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedAnnotationId(null);
  }, []);

  const resetInteractionState = useCallback(() => {
    setHoveredAnnotation(null);
    setSelectedAnnotationId(null);
    setNavigatorOpenSignal(null);
  }, []);

  useEffect(() => {
    if (navigatorScrollSignal == null || annotationMatches.length === 0) return;

    const scrollToActiveItem = () => {
      const listContainer = searchResultsListRef.current;
      if (!listContainer) return;

      const activeItem = listContainer.querySelector<HTMLButtonElement>(
        `[data-match-index="${activeMatchIndex}"]`
      );
      if (!activeItem) return;

      activeItem.scrollIntoView({ block: "nearest" });
    };

    const frameIds: number[] = [];
    const schedule = (remainingFrames: number) => {
      const id = requestAnimationFrame(() => {
        if (remainingFrames <= 1) {
          scrollToActiveItem();
          return;
        }
        schedule(remainingFrames - 1);
      });
      frameIds.push(id);
    };

    schedule(ANNOTATION_NAVIGATOR_BEHAVIOR.scrollDelayFrames);

    return () => {
      frameIds.forEach((id) => cancelAnimationFrame(id));
    };
  }, [navigatorScrollSignal, activeMatchIndex, annotationMatches.length, searchResultsListRef]);

  useEffect(() => {
    const handleKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") {
        clearSelection();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [clearSelection]);

  const handleAnnotationHover = useCallback((id: string | null) => {
    setHoveredAnnotation(id);
  }, []);

  const handleNavigatorSearchKeyDown = useCallback((event: ReactKeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" && annotationMatches.length > 0) {
      const targetIndex = event.shiftKey
        ? (activeMatchIndex - 1 + annotationMatches.length) % annotationMatches.length
        : activeMatchIndex;
      setSelectedAnnotationId(annotationMatches[targetIndex]?.id ?? null);
      triggerNavigatorScroll();
    }

    handleSearchKeyDown(event);
  }, [handleSearchKeyDown, annotationMatches, activeMatchIndex, triggerNavigatorScroll]);

  const handleNavigatorResultClick = useCallback((index: number) => {
    handleResultClick(index);
    setSelectedAnnotationId(annotationMatches[index]?.id ?? null);
    triggerNavigatorScroll();
  }, [handleResultClick, annotationMatches, triggerNavigatorScroll]);

  const handleNavigatorResultDoubleClick = useCallback((index: number) => {
    handleResultDoubleClick(index);
    setSelectedAnnotationId(annotationMatches[index]?.id ?? null);
    triggerNavigatorScroll();
  }, [handleResultDoubleClick, annotationMatches, triggerNavigatorScroll]);

  const handleCanvasAnnotationClick = useCallback((annotationId: string) => {
    setSelectedAnnotationId(annotationId);
    handleOverlayClickById(annotationId);
    triggerNavigatorOpen();
    triggerNavigatorScroll();
  }, [handleOverlayClickById, triggerNavigatorOpen, triggerNavigatorScroll]);

  const handleCanvasAnnotationDoubleClick = useCallback((annotationId: string) => {
    setSelectedAnnotationId(annotationId);
    handleOverlayDoubleClickById(annotationId);
    triggerNavigatorOpen();
    triggerNavigatorScroll();
  }, [handleOverlayDoubleClickById, triggerNavigatorOpen, triggerNavigatorScroll]);

  const displayMatchIndex = useMemo(() => {
    if (annotationMatches.length === 0) return 0;

    if (hoveredAnnotation) {
      const hoveredIndex = annotationMatches.findIndex((annotation) => annotation.id === hoveredAnnotation);
      if (hoveredIndex >= 0) {
        return hoveredIndex;
      }
    }

    return activeMatchIndex;
  }, [annotationMatches, hoveredAnnotation, activeMatchIndex]);

  return {
    hoveredAnnotation,
    selectedAnnotationId,
    navigatorOpenSignal,
    displayMatchIndex,
    handleAnnotationHover,
    handleNavigatorSearchKeyDown,
    handleNavigatorResultClick,
    handleNavigatorResultDoubleClick,
    handleCanvasAnnotationClick,
    handleCanvasAnnotationDoubleClick,
    clearSelection,
    resetInteractionState,
  };
}
