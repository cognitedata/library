import type { PageId } from '../../app/state/types';
import type { TrendWindow } from '../../types';

export const PAGE_IDS: readonly PageId[] = [
  'overview',
  'dimensions',
  'issues',
  'alerts',
  'configuration',
];
export const TAB_PAGE_IDS: readonly PageId[] = PAGE_IDS.filter((pageId) => pageId !== 'alerts');
export const TREND_WINDOWS: readonly TrendWindow[] = ['7d', '30d', '90d'];

export function isPageIdValue(value: string): value is PageId {
  return PAGE_IDS.some((pageId) => pageId === value);
}

export function isTrendWindowValue(value: string): value is TrendWindow {
  return TREND_WINDOWS.some((window) => window === value);
}
