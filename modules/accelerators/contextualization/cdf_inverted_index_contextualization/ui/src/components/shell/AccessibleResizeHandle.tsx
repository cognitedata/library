import { useCallback, type KeyboardEvent } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";

type Orientation = "horizontal" | "vertical";

type Props = {
  orientation: Orientation;
  value: number;
  min: number;
  max: number;
  step?: number;
  labelKey: "layout.resize.treeWidth" | "layout.resize.propertiesSize";
  onMouseDown: (e: React.MouseEvent) => void;
  onValueChange: (next: number) => void;
  className: string;
};

export function AccessibleResizeHandle({
  orientation,
  value,
  min,
  max,
  step = 16,
  labelKey,
  onMouseDown,
  onValueChange,
  className,
}: Props) {
  const { t } = useAppSettings();

  const nudge = useCallback(
    (delta: number) => {
      onValueChange(Math.min(max, Math.max(min, value + delta)));
    },
    [max, min, onValueChange, value]
  );

  const onKeyDown = useCallback(
    (e: KeyboardEvent<HTMLDivElement>) => {
      const isHorizontal = orientation === "horizontal";
      const decKey = isHorizontal ? "ArrowLeft" : "ArrowDown";
      const incKey = isHorizontal ? "ArrowRight" : "ArrowUp";
      if (e.key === decKey) {
        e.preventDefault();
        nudge(-step);
      } else if (e.key === incKey) {
        e.preventDefault();
        nudge(step);
      } else if (e.key === "Home") {
        e.preventDefault();
        onValueChange(min);
      } else if (e.key === "End") {
        e.preventDefault();
        onValueChange(max);
      }
    },
    [max, min, nudge, onValueChange, orientation, step]
  );

  return (
    <div
      className={className}
      role="slider"
      aria-orientation={orientation}
      aria-valuenow={Math.round(value)}
      aria-valuemin={min}
      aria-valuemax={max}
      aria-label={t(labelKey)}
      tabIndex={0}
      onMouseDown={onMouseDown}
      onKeyDown={onKeyDown}
    />
  );
}
