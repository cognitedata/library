import { useCallback, useEffect, useMemo, useState } from "react";
import type { KeyboardEvent, RefObject } from "react";
import { useClickOutside } from "@/shared/hooks/useClickOutside";

interface SearchableAnnotation {
  id: string;
  text: string;
  resourceType?: string;
}

interface UseAnnotationSearchOptions<T extends SearchableAnnotation> {
  annotations: T[];
  panelRef?: RefObject<HTMLDivElement>;
  resultsListRef?: RefObject<HTMLDivElement>;
  resetToken?: string | number;
  onSelectMatch?: (annotation: T | null) => void;
  enablePanelDismiss?: boolean;
  initialPanelOpen?: boolean;
}

export function useAnnotationSearch<T extends SearchableAnnotation>({
  annotations,
  panelRef,
  resultsListRef,
  resetToken,
  onSelectMatch,
  enablePanelDismiss = true,
  initialPanelOpen = false,
}: UseAnnotationSearchOptions<T>) {
  const [searchQuery, setSearchQuery] = useState("");
  const [activeMatchIndex, setActiveMatchIndex] = useState(0);
  const [isPanelOpen, setIsPanelOpen] = useState(initialPanelOpen);

  const matches = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();
    if (!query) return annotations;

    return annotations.filter((annotation) => {
      const searchable = `${annotation.text} ${annotation.resourceType || ""}`.toLowerCase();
      return searchable.includes(query);
    });
  }, [annotations, searchQuery]);

  const matchingIds = useMemo(() => {
    return new Set(matches.map((annotation) => annotation.id));
  }, [matches]);

  const hasSearchQuery = searchQuery.trim().length > 0;
  const activeMatch = matches[activeMatchIndex] ?? null;

  useEffect(() => {
    setActiveMatchIndex(0);
  }, [searchQuery, resetToken]);

  useEffect(() => {
    if (matches.length === 0) {
      if (activeMatchIndex !== 0) setActiveMatchIndex(0);
      return;
    }
    if (activeMatchIndex >= matches.length) {
      setActiveMatchIndex(matches.length - 1);
    }
  }, [matches.length, activeMatchIndex]);

  useClickOutside({
    isEnabled: enablePanelDismiss && isPanelOpen && Boolean(panelRef),
    ref: panelRef ?? { current: null },
    onClickOutside: () => setIsPanelOpen(false),
    onEscape: () => setIsPanelOpen(false),
  });

  useEffect(() => {
    if (!isPanelOpen || !hasSearchQuery || matches.length === 0) return;
    const listContainer = resultsListRef?.current;
    if (!listContainer) return;
    const activeItem = listContainer.querySelector<HTMLButtonElement>(
      `[data-match-index="${activeMatchIndex}"]`
    );
    if (!activeItem) return;

    activeItem.scrollIntoView({ block: "nearest" });
  }, [isPanelOpen, hasSearchQuery, matches.length, activeMatchIndex, resultsListRef]);

  const selectMatchByIndex = useCallback((index: number) => {
    const match = matches[index] ?? null;
    onSelectMatch?.(match);
  }, [matches, onSelectMatch]);

  const goToPreviousMatch = useCallback(() => {
    if (matches.length === 0) return;
    const nextIndex = (activeMatchIndex - 1 + matches.length) % matches.length;
    setActiveMatchIndex(nextIndex);
  }, [matches.length, activeMatchIndex]);

  const goToNextMatch = useCallback(() => {
    if (matches.length === 0) return;
    const nextIndex = (activeMatchIndex + 1) % matches.length;
    setActiveMatchIndex(nextIndex);
  }, [matches.length, activeMatchIndex]);

  const handleSearchKeyDown = useCallback((event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      if (matches.length === 0) return;

      if (event.shiftKey) {
        const prevIndex = (activeMatchIndex - 1 + matches.length) % matches.length;
        setActiveMatchIndex(prevIndex);
        selectMatchByIndex(prevIndex);
        return;
      }

      selectMatchByIndex(activeMatchIndex);
    }

    if (event.key === "Escape") {
      event.preventDefault();
      setIsPanelOpen(false);
    }
  }, [matches.length, activeMatchIndex, selectMatchByIndex]);

  const handleResultClick = useCallback((index: number) => {
    setActiveMatchIndex(index);
  }, []);

  const handleResultDoubleClick = useCallback((index: number) => {
    setActiveMatchIndex(index);
    selectMatchByIndex(index);
  }, [selectMatchByIndex]);

  const handleOverlayClickById = useCallback((id: string) => {
    const clickedMatchIndex = matches.findIndex((match) => match.id === id);
    if (clickedMatchIndex >= 0) {
      setActiveMatchIndex(clickedMatchIndex);
    }
  }, [matches]);

  const handleOverlayDoubleClickById = useCallback((id: string) => {
    const clickedMatchIndex = matches.findIndex((match) => match.id === id);
    if (clickedMatchIndex >= 0) {
      setActiveMatchIndex(clickedMatchIndex);
      selectMatchByIndex(clickedMatchIndex);
    }
  }, [matches, selectMatchByIndex]);

  const clearSearch = useCallback(() => {
    setSearchQuery("");
    setIsPanelOpen(true);
  }, []);

  return {
    searchQuery,
    setSearchQuery,
    clearSearch,
    activeMatchIndex,
    setActiveMatchIndex,
    isPanelOpen,
    setIsPanelOpen,
    matches,
    matchingIds,
    hasSearchQuery,
    activeMatch,
    goToPreviousMatch,
    goToNextMatch,
    handleSearchKeyDown,
    handleResultClick,
    handleResultDoubleClick,
    handleOverlayClickById,
    handleOverlayDoubleClickById,
  };
}
