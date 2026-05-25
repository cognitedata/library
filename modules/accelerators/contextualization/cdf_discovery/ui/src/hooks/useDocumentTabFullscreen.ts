import { useCallback, useEffect, useState } from "react";

/** Fullscreen overlay for the active document tab (Escape to close). */
export function useDocumentTabFullscreen(hasActiveTab: boolean) {
  const [open, setOpen] = useState(false);

  const openFullscreen = useCallback(() => {
    if (hasActiveTab) setOpen(true);
  }, [hasActiveTab]);

  const closeFullscreen = useCallback(() => setOpen(false), []);

  const toggleFullscreen = useCallback(() => {
    setOpen((prev) => (hasActiveTab ? !prev : false));
  }, [hasActiveTab]);

  useEffect(() => {
    if (!hasActiveTab) setOpen(false);
  }, [hasActiveTab]);

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  return {
    fullscreenOpen: open,
    openFullscreen,
    closeFullscreen,
    toggleFullscreen,
  };
}
