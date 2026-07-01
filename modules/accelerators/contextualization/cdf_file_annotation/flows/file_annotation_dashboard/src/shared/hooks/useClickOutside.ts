import { useEffect } from "react";
import type { RefObject } from "react";

interface UseClickOutsideOptions {
  isEnabled?: boolean;
  ref: RefObject<HTMLElement | null>;
  onClickOutside?: () => void;
  onEscape?: () => void;
}

export function useClickOutside({
  isEnabled = true,
  ref,
  onClickOutside,
  onEscape,
}: UseClickOutsideOptions) {
  useEffect(() => {
    if (!isEnabled) return;

    const handleMouseDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (ref.current && !ref.current.contains(target)) {
        onClickOutside?.();
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onEscape?.();
      }
    };

    document.addEventListener("mousedown", handleMouseDown);
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("mousedown", handleMouseDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [isEnabled, ref, onClickOutside, onEscape]);
}
