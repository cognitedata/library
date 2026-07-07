import { Button } from "@/shared/components/ui/button";
import { Loader2, RotateCcw } from "lucide-react";

interface PatternManagementHeaderProps {
  lastDurationLabel: string | null;
  onResetPage?: () => void;
  isResettingPage?: boolean;
}

export function PatternManagementHeader({
  lastDurationLabel,
  onResetPage,
  isResettingPage = false,
}: PatternManagementHeaderProps) {
  return (
    <div className="flex items-start justify-between">
      <div>
        <h2 className="text-xl font-bold tracking-tight">Pattern Management</h2>
        <p className="text-sm text-muted-foreground mt-1">
          Import, curate and refresh patterns used by the file annotation pipeline.
        </p>
      </div>
      <div className="flex items-center gap-3">
        {onResetPage && (
          <Button variant="outline" size="xs" onClick={onResetPage} disabled={isResettingPage}>
            {isResettingPage ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCcw className="h-3.5 w-3.5" />}
            Reset Page
          </Button>
        )}
        {lastDurationLabel && (
          <div className="text-right">
            <p className="text-[11px] text-muted-foreground">Last load time</p>
            <p className="text-sm font-medium">{lastDurationLabel}</p>
          </div>
        )}
      </div>
    </div>
  );
}
