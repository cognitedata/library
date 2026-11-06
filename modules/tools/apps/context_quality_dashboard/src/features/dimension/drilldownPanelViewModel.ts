import type { DrilldownDto, MetricId } from '../../types';

/** Subset of dimension view model used by {@link DrilldownPanel}. */
export type DrilldownPanelViewModel = {
  drilldown: DrilldownDto | undefined;
  selectedMetricId: MetricId | undefined;
  isDrilldownLoading: boolean;
  isDrilldownError: boolean;
  clearMetric: () => void;
  canRerun: boolean;
  isRerunningMetric: boolean;
  rerunMetric: () => void;
  canNotifyOwner: boolean;
  isNotifying: boolean;
  notificationSent: boolean;
  notifyError: boolean;
  notifyOwner: (ownerId: string, entityIds: string[]) => void;
  t: (key: string) => string;
};
