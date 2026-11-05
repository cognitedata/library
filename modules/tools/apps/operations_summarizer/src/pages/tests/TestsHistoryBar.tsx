import {
  TestsHistoryScoreRange,
  type TestsHistoryScoreRangeProps,
} from "./TestsHistoryScoreRange";
import {
  TestsHistoryStats,
  type TestsHistoryStatsProps,
} from "./TestsHistoryStats";

export interface TestsHistoryBarProps
  extends TestsHistoryStatsProps, TestsHistoryScoreRangeProps {}

export function TestsHistoryBar({
  statsUnavailable,
  testStatsLoading,
  hasStreamedStats,
  totalTests,
  averageScore,
  excellentCount,
  goodCount,
  poorCount,
  minScore,
  maxScore,
  onMinScoreChange,
  onMaxScoreChange,
  hasActiveHistoryFilters,
  onClearFilters,
}: TestsHistoryBarProps) {
  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="flex flex-col md:flex-row md:items-center gap-3 py-3 bg-muted/30 px-6">
        <div className="flex flex-wrap items-center gap-3 flex-1">
          <TestsHistoryStats
            statsUnavailable={statsUnavailable}
            testStatsLoading={testStatsLoading}
            hasStreamedStats={hasStreamedStats}
            totalTests={totalTests}
            averageScore={averageScore}
            excellentCount={excellentCount}
            goodCount={goodCount}
            poorCount={poorCount}
          />
        </div>
        <TestsHistoryScoreRange
          minScore={minScore}
          maxScore={maxScore}
          onMinScoreChange={onMinScoreChange}
          onMaxScoreChange={onMaxScoreChange}
          hasActiveHistoryFilters={hasActiveHistoryFilters}
          onClearFilters={onClearFilters}
        />
      </div>
    </div>
  );
}
