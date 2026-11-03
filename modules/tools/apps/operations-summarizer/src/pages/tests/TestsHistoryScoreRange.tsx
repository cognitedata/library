import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

function clampScore(raw: string, emptyValue: number) {
  if (raw === "") return emptyValue;
  return Math.max(0, Math.min(100, Number(raw)));
}

export interface TestsHistoryScoreRangeProps {
  minScore: number;
  maxScore: number;
  onMinScoreChange: (value: number) => void;
  onMaxScoreChange: (value: number) => void;
  hasActiveHistoryFilters: boolean;
  onClearFilters: () => void;
}

export function TestsHistoryScoreRange({
  minScore,
  maxScore,
  onMinScoreChange,
  onMaxScoreChange,
  hasActiveHistoryFilters,
  onClearFilters,
}: TestsHistoryScoreRangeProps) {
  return (
    <div className="flex items-center gap-2 md:ml-auto">
      <span className="text-xs text-muted-foreground shrink-0">Score:</span>
      <Input
        type="number"
        min={0}
        max={100}
        value={minScore}
        onChange={(event) =>
          onMinScoreChange(clampScore(event.target.value, 0))
        }
        aria-label="Minimum score"
        className="h-7 w-16 text-sm"
      />
      <span className="text-muted-foreground text-sm shrink-0">→</span>
      <Input
        type="number"
        min={0}
        max={100}
        value={maxScore}
        onChange={(event) =>
          onMaxScoreChange(clampScore(event.target.value, 100))
        }
        aria-label="Maximum score"
        className="h-7 w-16 text-sm"
      />
      {hasActiveHistoryFilters && (
        <Button
          variant="ghost"
          size="sm"
          onClick={onClearFilters}
          className="h-7 text-xs shrink-0"
        >
          Clear
        </Button>
      )}
    </div>
  );
}
