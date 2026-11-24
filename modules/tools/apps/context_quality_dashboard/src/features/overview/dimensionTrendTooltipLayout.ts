const DEFAULT_CHART_WIDTH = 280;
const DEFAULT_CHART_HEIGHT = 80;

export type DimensionTrendTooltipPlacement = {
  left: string;
  top: string;
  transform: string;
};

/** Keeps the trend hover card inside the plot area (avoids clipping on narrow aside columns). */
export function placementForDimensionTrendTooltip(
  anchorX: number,
  anchorY: number,
  chartWidth: number = DEFAULT_CHART_WIDTH,
  chartHeight: number = DEFAULT_CHART_HEIGHT
): DimensionTrendTooltipPlacement {
  const xRatio = anchorX / chartWidth;
  const yRatio = anchorY / chartHeight;
  const leftPercent = xRatio * 100;

  let transform = 'translate(-50%, 0)';
  if (xRatio >= 0.68) {
    transform = 'translate(-100%, 0)';
  } else if (xRatio <= 0.32) {
    transform = 'translate(0, 0)';
  }

  const topPercent =
    yRatio < 0.5
      ? Math.min(yRatio * 100 + 16, 68)
      : Math.max(yRatio * 100 - 32, 6);

  return {
    left: `${leftPercent}%`,
    top: `${topPercent}%`,
    transform,
  };
}
