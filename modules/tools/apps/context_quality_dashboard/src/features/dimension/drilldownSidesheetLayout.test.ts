import { describe, expect, it } from 'vitest';

import {
  clampDrilldownSheetWidth,
  DRILLDOWN_SHEET_DEFAULT_WIDTH_PX,
  DRILLDOWN_SHEET_MIN_WIDTH_PX,
  isDrilldownSheetWide,
  toggleDrilldownSheetWidth,
} from './drilldownSidesheetLayout';

describe(clampDrilldownSheetWidth.name, () => {
  it('should clamp between minimum and viewport maximum', () => {
    expect(clampDrilldownSheetWidth(100, 1200)).toBe(DRILLDOWN_SHEET_MIN_WIDTH_PX);
    expect(clampDrilldownSheetWidth(5000, 1200)).toBe(1104);
    expect(clampDrilldownSheetWidth(800, 1200)).toBe(800);
  });
});

describe(toggleDrilldownSheetWidth.name, () => {
  it('should expand to max width then return to default', () => {
    const viewport = 1000;
    const expanded = toggleDrilldownSheetWidth(DRILLDOWN_SHEET_DEFAULT_WIDTH_PX, viewport);
    expect(isDrilldownSheetWide(expanded, viewport)).toBe(true);
    expect(toggleDrilldownSheetWidth(expanded, viewport)).toBe(DRILLDOWN_SHEET_DEFAULT_WIDTH_PX);
  });
});
