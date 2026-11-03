import { Info, Plus, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

function refreshToneClass(refreshSuccess: boolean, refreshError: boolean) {
  if (refreshSuccess) return "text-emerald-600 dark:text-emerald-400";
  if (refreshError) return "text-red-600 dark:text-red-400";
  return "";
}

export interface TestsToolbarActionsProps {
  onToggleCreateForm: () => void;
  onRefresh: () => void;
  isRefreshDisabled: boolean;
  refreshSuccess: boolean;
  refreshError: boolean;
  refreshMessage: string;
  onShowInfo: () => void;
}

export function TestsToolbarActions({
  onToggleCreateForm,
  onRefresh,
  isRefreshDisabled,
  refreshSuccess,
  refreshError,
  refreshMessage,
  onShowInfo,
}: TestsToolbarActionsProps) {
  const tone = refreshToneClass(refreshSuccess, refreshError);

  return (
    <div className="flex items-center gap-2 md:ml-auto">
      <Button
        variant="secondary"
        size="sm"
        onClick={onToggleCreateForm}
        className="h-8"
        title="Create new test"
      >
        <Plus className="h-4 w-4 mr-1" />
        Create new test
      </Button>
      <Button
        variant="ghost"
        size="sm"
        onClick={onRefresh}
        disabled={isRefreshDisabled}
        className={`h-8 transition-colors ${tone}`}
        title="Refresh tests"
      >
        <RefreshCw
          className={`h-4 w-4 mr-1 ${isRefreshDisabled ? "animate-spin" : ""} ${tone}`}
        />
        {refreshMessage}
      </Button>
      <Button
        variant="ghost"
        size="sm"
        onClick={onShowInfo}
        className="h-8 w-8 p-0 text-muted-foreground hover:text-foreground shrink-0"
        title="Show testing information"
        aria-label="Show testing information"
      >
        <Info className="h-4 w-4" aria-hidden />
      </Button>
    </div>
  );
}
