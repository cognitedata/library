export const DRILLDOWN_SHEET_MIN_WIDTH_PX = 360;
export const DRILLDOWN_SHEET_DEFAULT_WIDTH_PX = 672;
export const DRILLDOWN_SHEET_MAX_WIDTH_VIEWPORT_RATIO = 0.92;

export function drilldownSheetMaxWidthPx(viewportWidth: number): number {
  return Math.round(viewportWidth * DRILLDOWN_SHEET_MAX_WIDTH_VIEWPORT_RATIO);
}

export function clampDrilldownSheetWidth(widthPx: number, viewportWidth: number): number {
  const max = drilldownSheetMaxWidthPx(viewportWidth);
  return Math.min(max, Math.max(DRILLDOWN_SHEET_MIN_WIDTH_PX, Math.round(widthPx)));
}

export function isDrilldownSheetWide(widthPx: number, viewportWidth: number): boolean {
  const max = drilldownSheetMaxWidthPx(viewportWidth);
  return widthPx >= max - 48;
}

export function toggleDrilldownSheetWidth(
  currentWidthPx: number,
  viewportWidth: number
): number {
  if (isDrilldownSheetWide(currentWidthPx, viewportWidth)) {
    return DRILLDOWN_SHEET_DEFAULT_WIDTH_PX;
  }
  return drilldownSheetMaxWidthPx(viewportWidth);
}
