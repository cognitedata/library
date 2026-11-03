import { FileText, Plus, TestTube2 } from "lucide-react";
import { TestReportsTableSkeleton } from "@/components/common/list-table-skeletons";
import { TestTable } from "@/components/tests/TestTable";
import { Button } from "@/components/ui/button";
import type { SummaryInstance } from "@/types/operations";
import {
  downloadSummaryAsMarkdown,
  extractSummaryContent,
} from "@/utils/summaryDownload";

export interface TestsReportsSectionProps {
  isLoading: boolean;
  testReports: SummaryInstance[];
  filteredReports: SummaryInstance[];
  summariesCount: number;
  hasActiveHistoryFilters: boolean;
  pageDisplay: number;
  hasNext: boolean;
  hasPrev: boolean;
  onNext: () => void;
  onPrev: () => void;
  onView: (report: SummaryInstance) => void;
  onDelete: (report: SummaryInstance) => void;
  onCreateSummaryFirst: () => void;
  onCreateTest: () => void;
}

export function TestsReportsSection({
  isLoading,
  testReports,
  filteredReports,
  summariesCount,
  hasActiveHistoryFilters,
  pageDisplay,
  hasNext,
  hasPrev,
  onNext,
  onPrev,
  onView,
  onDelete,
  onCreateSummaryFirst,
  onCreateTest,
}: TestsReportsSectionProps) {
  if (isLoading) return <TestReportsTableSkeleton />;

  if (testReports.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
        <TestTube2 className="h-12 w-12 text-muted-foreground/30 mb-4" />
        <p className="text-lg font-medium text-foreground">
          No test reports found
        </p>
        <p className="text-sm mt-1 mb-3">
          {hasActiveHistoryFilters
            ? "Try adjusting your filters or go to another page"
            : summariesCount === 0
              ? "Create a summary first, then run tests to compare results"
              : "Create your first test using the button above"}
        </p>
        {!hasActiveHistoryFilters && summariesCount === 0 && (
          <Button
            variant="outline"
            size="sm"
            onClick={onCreateSummaryFirst}
            className="h-8"
          >
            <FileText className="h-4 w-4 mr-1" />
            Create Summary First
          </Button>
        )}
        {!hasActiveHistoryFilters && summariesCount > 0 && (
          <Button
            variant="outline"
            size="sm"
            onClick={onCreateTest}
            className="h-8"
          >
            <Plus className="h-4 w-4 mr-1" />
            Create Test
          </Button>
        )}
      </div>
    );
  }

  if (filteredReports.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground text-center px-4">
        <p className="text-foreground font-medium">
          No reports on this page match the score range
        </p>
        <p className="text-sm mt-2">
          Adjust the score filter or use Next/Previous to load other pages.
        </p>
      </div>
    );
  }

  return (
    <TestTable
      testReports={filteredReports}
      currentPage={pageDisplay}
      totalPages={pageDisplay + (hasNext ? 1 : 0)}
      totalItems={filteredReports.length}
      hasNextPage={hasNext}
      hasPreviousPage={hasPrev}
      onNextPage={onNext}
      onPreviousPage={onPrev}
      onView={onView}
      onDownload={(report) => {
        const { content } = extractSummaryContent(report);
        downloadSummaryAsMarkdown(report, content);
      }}
      onDelete={onDelete}
      isLoading={false}
      cursorMode
    />
  );
}
