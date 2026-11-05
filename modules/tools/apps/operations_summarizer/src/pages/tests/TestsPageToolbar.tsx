import { TestsToolbarActions } from "./TestsToolbarActions";
import { TestsToolbarTitle } from "./TestsToolbarTitle";

export type { TestsFetchErrorBannerProps } from "./TestsFetchErrorBanner";
export { TestsFetchErrorBanner } from "./TestsFetchErrorBanner";
export type { TestsHistoryBarProps } from "./TestsHistoryBar";
export { TestsHistoryBar } from "./TestsHistoryBar";
export { TestsScoreLegend } from "./TestsScoreLegend";

export interface TestsPageToolbarProps {
  pageDisplay: number;
  totalTests: number;
  statsUnavailable: boolean;
  onToggleCreateForm: () => void;
  onRefresh: () => void;
  isRefreshDisabled: boolean;
  refreshSuccess: boolean;
  refreshError: boolean;
  refreshMessage: string;
  onShowInfo: () => void;
}

export function TestsPageToolbar({
  pageDisplay,
  totalTests,
  statsUnavailable,
  onToggleCreateForm,
  onRefresh,
  isRefreshDisabled,
  refreshSuccess,
  refreshError,
  refreshMessage,
  onShowInfo,
}: TestsPageToolbarProps) {
  return (
    <div className="flex flex-col md:flex-row md:items-center gap-3">
      <TestsToolbarTitle
        pageDisplay={pageDisplay}
        totalTests={totalTests}
        statsUnavailable={statsUnavailable}
      />
      <TestsToolbarActions
        onToggleCreateForm={onToggleCreateForm}
        onRefresh={onRefresh}
        isRefreshDisabled={isRefreshDisabled}
        refreshSuccess={refreshSuccess}
        refreshError={refreshError}
        refreshMessage={refreshMessage}
        onShowInfo={onShowInfo}
      />
    </div>
  );
}
