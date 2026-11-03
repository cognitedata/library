/**
 * Reusable Summary Table Component
 *
 * Displays summaries in a table format with pagination and actions.
 */

import { Eye, FileText, TestTube2, Trash2 } from "lucide-react";
import { useState } from "react";
import { DeleteConfirmation } from "@/components/common/ConfirmationDialog";
import { SummariesTableSkeleton } from "@/components/common/list-table-skeletons";
import {
  Pagination,
  ResponsiveTable,
} from "@/components/common/ResponsiveTable";
import { SummaryCard } from "@/components/summaries/SummaryCard";
import { Button } from "@/components/ui/button";
import { DELETE_ICON_BUTTON_CLASSNAME } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { formatDateTimeShort } from "@/utils/formatting";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";
import { summaryInstanceNodeKey } from "@/utils/summaryInstanceMapping";

export interface SummaryTableProps {
  /** Summaries to display */
  summaries: SummaryInstance[];
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
  onView?: (summary: SummaryInstance) => void;
  /** Callback when test is clicked */
  onTest?: (summary: SummaryInstance) => void;
  /** Callback when delete is confirmed */
  onDelete?: (summary: SummaryInstance) => Promise<void>;
  /** Whether delete is pending */
  isDeletePending?: boolean;
  /** Loading state */
  isLoading?: boolean;
  /** Server cursor pagination (unknown total page count). */
  cursorMode?: boolean;
}

export function SummaryTable({
  summaries,
  currentPage,
  totalPages,
  totalItems,
  hasNextPage,
  hasPreviousPage,
  onNextPage,
  onPreviousPage,
  onView,
  onTest,
  onDelete,
  isDeletePending = false,
  isLoading = false,
  cursorMode = false,
}: SummaryTableProps) {
  const [deleteCandidate, setDeleteCandidate] =
    useState<SummaryInstance | null>(null);

  const getRowData = (summary: SummaryInstance) => {
    const { name } = extractSummaryContent(summary);
    const displayDate = getSummaryDisplayDate(summary);
    return { name, displayDate };
  };

  const handleDeleteClick = (summary: SummaryInstance) => {
    setDeleteCandidate(summary);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteCandidate || !onDelete) return;
    await onDelete(deleteCandidate);
    setDeleteCandidate(null);
  };

  const handleDeleteCancel = () => {
    setDeleteCandidate(null);
  };

  if (isLoading) {
    return <SummariesTableSkeleton />;
  }

  return (
    <div>
      <ResponsiveTable
        table={
          <div className="border rounded-lg overflow-x-auto">
            <table className="w-full text-sm min-w-[600px]">
              <thead className="bg-muted/50 border-b">
                <tr>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">
                    Name
                  </th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">
                    Period
                  </th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">
                    Created
                  </th>
                  <th className="text-right px-4 py-3 font-medium text-muted-foreground">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {summaries.map((summary) => {
                  const { name, displayDate } = getRowData(summary);
                  const createdDate = formatDateTimeShort(
                    new Date(summary.createdTime),
                  );

                  return (
                    <tr
                      key={summaryInstanceNodeKey(summary)}
                      className="hover:bg-muted/30 transition-colors"
                    >
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <FileText className="h-4 w-4 text-muted-foreground shrink-0" />
                          <span
                            className="font-medium text-foreground truncate"
                            title={name}
                          >
                            {name || "Untitled Summary"}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">
                        {displayDate || "—"}
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">
                        {createdDate}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-end gap-1">
                          {onView && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => onView(summary)}
                              className="min-h-10 px-3"
                            >
                              <Eye className="h-4 w-4 mr-1" />
                              View
                            </Button>
                          )}
                          {onTest && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => onTest(summary)}
                              className="min-h-10 px-2"
                              title="Create test from this summary"
                            >
                              <TestTube2 className="h-4 w-4 mr-1" />
                              Test
                            </Button>
                          )}
                          {onDelete && (
                            <Button
                              type="button"
                              variant="destructive"
                              onClick={() => handleDeleteClick(summary)}
                              className={DELETE_ICON_BUTTON_CLASSNAME}
                              aria-label={`Delete summary ${name || "Untitled Summary"}`}
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
              {/* Table Footer with Pagination */}
              <tfoot>
                <tr>
                  <td colSpan={4} className="px-0 py-0">
                    <Pagination
                      currentPage={currentPage}
                      totalPages={totalPages}
                      totalItems={totalItems}
                      hasNextPage={hasNextPage}
                      hasPreviousPage={hasPreviousPage}
                      onNextPage={onNextPage}
                      onPreviousPage={onPreviousPage}
                      itemName="summaries"
                      cursorMode={cursorMode}
                    />
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        }
        cards={summaries.map((summary) => (
          <SummaryCard
            key={summaryInstanceNodeKey(summary)}
            summary={summary}
            onView={onView}
            onTest={onTest}
            onDelete={onDelete}
            isDeletePending={isDeletePending}
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
          itemName="summaries"
          cursorMode={cursorMode}
        />
      </div>

      {/* Delete Confirmation Dialog */}
      <DeleteConfirmation
        open={!!deleteCandidate}
        onOpenChange={(open) => !open && setDeleteCandidate(null)}
        itemType="Summary"
        itemName={
          deleteCandidate
            ? extractSummaryContent(deleteCandidate).name || "Untitled Summary"
            : undefined
        }
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
        isProcessing={isDeletePending}
        additionalConsequences={["Associated reports and data will be removed"]}
      />
    </div>
  );
}
