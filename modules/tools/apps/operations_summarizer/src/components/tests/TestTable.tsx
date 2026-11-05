/**
 * Reusable Test Table Component
 *
 * Displays test reports in a table format with pagination and actions.
 */

import { Download, Eye, FlaskConical, Trash2 } from "lucide-react";
import { TestReportsTableSkeleton } from "@/components/common/list-table-skeletons";
import {
  Pagination,
  ResponsiveTable,
} from "@/components/common/ResponsiveTable";
import { TestCard } from "@/components/tests/TestCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { DELETE_ICON_BUTTON_CLASSNAME } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { formatDateTimeShort } from "@/utils/formatting";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";
import { summaryInstanceNodeKey } from "@/utils/summaryInstanceMapping";
import {
  extractDataVolumeFromContent,
  extractPeriodFromContent,
  extractScoreFromContent,
  getScoreBadgeVariant,
  vividScoreBadgeClassName,
} from "@/utils/testReportUtils";

export interface TestTableProps {
  /** Test reports to display */
  testReports: SummaryInstance[];
  /** Current page number */
  currentPage: number;
  /** Total number of pages */
  totalPages: number;
  /** Total number of items */
  totalItems: number;
  /** Whether there is a next page */
  hasNextPage: boolean;
  /** Whether there is a previous page */
  hasPreviousPage: boolean;
  /** Callback for next page */
  onNextPage: () => void;
  /** Callback for previous page */
  onPreviousPage: () => void;
  /** Callback when view is clicked */
  onView?: (report: SummaryInstance) => void;
  /** Callback when download is clicked */
  onDownload?: (report: SummaryInstance) => void;
  /** Callback when delete is clicked */
  onDelete?: (report: SummaryInstance) => void;
  /** Loading state */
  isLoading?: boolean;
  cursorMode?: boolean;
}

export function TestTable({
  testReports,
  currentPage,
  totalPages,
  totalItems,
  hasNextPage,
  hasPreviousPage,
  onNextPage,
  onPreviousPage,
  onView,
  onDownload,
  onDelete,
  isLoading = false,
  cursorMode = false,
}: TestTableProps) {
  if (isLoading) {
    return <TestReportsTableSkeleton />;
  }

  return (
    <div>
      <ResponsiveTable
        table={
          <div className="border rounded-lg overflow-x-auto">
            <table className="w-full text-sm min-w-[700px]">
              <thead className="bg-muted/50">
                <tr>
                  <th className="text-left py-3 px-4 font-medium text-muted-foreground">
                    Report
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-muted-foreground">
                    Period
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-muted-foreground">
                    Score
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-muted-foreground">
                    Data Volume
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-muted-foreground">
                    Tested
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-muted-foreground">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {testReports.map((report) => {
                  const { name, content } = extractSummaryContent(report);
                  const date = getSummaryDisplayDate(report);
                  const score = extractScoreFromContent(content);
                  const volume = extractDataVolumeFromContent(content);
                  const period =
                    extractPeriodFromContent(content) ||
                    extractPeriodFromContent(report.externalId);
                  const createdDate = formatDateTimeShort(
                    new Date(report.createdTime),
                  );
                  const scoreVariant = getScoreBadgeVariant(score);

                  return (
                    <tr
                      key={summaryInstanceNodeKey(report)}
                      className="hover:bg-muted/30 transition-colors"
                    >
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <FlaskConical className="h-4 w-4 text-muted-foreground shrink-0" />
                          <span className="font-medium text-foreground truncate">
                            {name || "Untitled Test"}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-muted-foreground">
                        {period || date || "—"}
                      </td>
                      <td className="py-3 px-4 text-center">
                        <Badge
                          variant={scoreVariant}
                          className={vividScoreBadgeClassName(scoreVariant)}
                        >
                          {score.toFixed(1)}%
                        </Badge>
                      </td>
                      <td className="py-3 px-4 text-center text-muted-foreground">
                        {volume}
                      </td>
                      <td className="py-3 px-4 text-muted-foreground">
                        {createdDate}
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center justify-end gap-1">
                          {onView && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => onView(report)}
                              className="min-h-10 px-3"
                            >
                              <Eye className="h-4 w-4 mr-1" />
                              View
                            </Button>
                          )}
                          {onDownload && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => onDownload(report)}
                              className="min-h-10 px-2"
                              aria-label={`Download test report ${name || "Untitled Test"}`}
                            >
                              <Download className="h-4 w-4" aria-hidden />
                            </Button>
                          )}
                          {onDelete && (
                            <Button
                              type="button"
                              variant="destructive"
                              onClick={() => onDelete(report)}
                              className={DELETE_ICON_BUTTON_CLASSNAME}
                              aria-label={`Delete test report ${name || "Untitled Test"}`}
                            >
                              <Trash2 className="size-4" aria-hidden />
                            </Button>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <Pagination
              currentPage={currentPage}
              totalPages={totalPages}
              totalItems={totalItems}
              hasNextPage={hasNextPage}
              hasPreviousPage={hasPreviousPage}
              onNextPage={onNextPage}
              onPreviousPage={onPreviousPage}
              itemName="test reports"
              cursorMode={cursorMode}
            />
          </div>
        }
        cards={testReports.map((report) => (
          <TestCard
            key={summaryInstanceNodeKey(report)}
            testReport={report}
            onView={onView}
            onDownload={onDownload}
            onDelete={onDelete}
          />
        ))}
      />

      {/* Mobile-only Pagination */}
      <div className="block md:hidden">
        <Pagination
          currentPage={currentPage}
          totalPages={totalPages}
          totalItems={totalItems}
          hasNextPage={hasNextPage}
          hasPreviousPage={hasPreviousPage}
          onNextPage={onNextPage}
          onPreviousPage={onPreviousPage}
          itemName="test reports"
          cursorMode={cursorMode}
        />
      </div>
    </div>
  );
}
