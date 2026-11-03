import {
  AlertCircle,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  TrendingUp,
  XCircle,
} from "lucide-react";
import { Loader } from "@/components/Loader";

export interface TestsHistoryStatsProps {
  statsUnavailable: boolean;
  testStatsLoading: boolean;
  hasStreamedStats: boolean;
  totalTests: number;
  averageScore: number;
  excellentCount: number;
  goodCount: number;
  poorCount: number;
}

export function TestsHistoryStats({
  statsUnavailable,
  testStatsLoading,
  hasStreamedStats,
  totalTests,
  averageScore,
  excellentCount,
  goodCount,
  poorCount,
}: TestsHistoryStatsProps) {
  if (statsUnavailable) {
    return (
      <div className="flex items-center gap-2 text-destructive">
        <AlertCircle className="h-4 w-4 shrink-0" />
        <span className="text-sm">Stats unavailable — refresh to retry.</span>
      </div>
    );
  }

  if (testStatsLoading && !hasStreamedStats) {
    return (
      <div className="flex items-center gap-2 text-muted-foreground">
        <Loader size={16} />
        <span className="text-sm">Loading statistics…</span>
      </div>
    );
  }

  return (
    <>
      <div className="flex items-center gap-2">
        <BarChart3 className="h-4 w-4 text-muted-foreground shrink-0" />
        <span className="text-sm font-medium">{totalTests}</span>
        <span className="text-xs text-muted-foreground">Total</span>
      </div>
      <div className="h-4 w-px bg-border" />
      <div className="flex items-center gap-2">
        <TrendingUp className="h-4 w-4 text-muted-foreground shrink-0" />
        <span className="text-sm font-medium">{averageScore.toFixed(1)}%</span>
        <span className="text-xs text-muted-foreground">Avg Score</span>
      </div>
      <div className="h-4 w-px bg-border" />
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-1">
          <CheckCircle2 className="h-4 w-4 text-emerald-600" />
          <span className="text-sm font-medium text-emerald-700 dark:text-emerald-400">
            {excellentCount}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <AlertTriangle className="h-4 w-4 text-amber-600" />
          <span className="text-sm font-medium text-amber-700 dark:text-amber-400">
            {goodCount}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <XCircle className="h-4 w-4 text-red-600" />
          <span className="text-sm font-medium text-red-700 dark:text-red-400">
            {poorCount}
          </span>
        </div>
      </div>
    </>
  );
}
