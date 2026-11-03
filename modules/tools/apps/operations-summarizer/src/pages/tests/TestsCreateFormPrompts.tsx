import { FileText, FlaskConical, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface TestsCreateFormPromptsProps {
  hasSummaries: boolean;
  hasSelectedSummary: boolean;
  hasTestResult: boolean;
  onCreateSummaryFirst: () => void;
}

export function TestsCreateFormPrompts({
  hasSummaries,
  hasSelectedSummary,
  hasTestResult,
  onCreateSummaryFirst,
}: TestsCreateFormPromptsProps) {
  if (!hasSummaries) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
        <FileText className="h-12 w-12 text-muted-foreground/30 mb-4" />
        <p className="text-foreground font-medium">No Summaries Available</p>
        <p className="text-sm mt-1 mb-3">
          Create your first summary before running tests
        </p>
        <Button
          variant="outline"
          size="sm"
          onClick={onCreateSummaryFirst}
          className="h-8"
        >
          <Plus className="h-4 w-4 mr-1" />
          Create Summary First
        </Button>
      </div>
    );
  }

  if (!hasSelectedSummary && !hasTestResult) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
        <FlaskConical className="h-12 w-12 text-muted-foreground/30 mb-4" />
        <p className="text-foreground font-medium">Select a Baseline Summary</p>
        <p className="text-sm mt-1">
          Choose an original summary to compare against when testing
        </p>
      </div>
    );
  }

  return null;
}
