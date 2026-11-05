/**
 * Summary Card Component for Mobile Display
 *
 * Displays summary information in a card format optimized for mobile screens.
 */

import {
  Calendar,
  Clock,
  Eye,
  FileText,
  TestTube2,
  Trash2,
} from "lucide-react";
import { useState } from "react";
import { DeleteConfirmation } from "@/components/common/ConfirmationDialog";
import { Button } from "@/components/ui/button";
import { Card, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { DELETE_ICON_BUTTON_CLASSNAME } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { formatDateTimeShort } from "@/utils/formatting";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";

export interface SummaryCardProps {
  /** Summary to display */
  summary: SummaryInstance;
  /** Callback when view is clicked */
  onView?: (summary: SummaryInstance) => void;
  /** Callback when test is clicked */
  onTest?: (summary: SummaryInstance) => void;
  /** Callback when delete is confirmed */
  onDelete?: (summary: SummaryInstance) => Promise<void>;
  /** Whether delete is pending */
  isDeletePending?: boolean;
}

export function SummaryCard({
  summary,
  onView,
  onTest,
  onDelete,
  isDeletePending = false,
}: SummaryCardProps) {
  const [deleteCandidate, setDeleteCandidate] =
    useState<SummaryInstance | null>(null);

  const { name } = extractSummaryContent(summary);
  const displayDate = getSummaryDisplayDate(summary);
  const createdDate = formatDateTimeShort(new Date(summary.createdTime));

  const handleDeleteClick = () => {
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

  return (
    <>
      <Card className="transition-colors hover:bg-muted/30 min-w-0">
        <CardHeader className="min-w-0">
          <div className="flex items-start gap-3 min-w-0">
            <FileText className="h-5 w-5 text-muted-foreground shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0 overflow-hidden">
              <CardTitle
                className="text-base font-medium text-foreground min-w-0 max-sm:line-clamp-3 max-sm:wrap-break-word max-sm:leading-snug sm:truncate sm:whitespace-nowrap"
                title={name || "Untitled Summary"}
              >
                {name || "Untitled Summary"}
              </CardTitle>
              <div className="flex flex-col gap-2 mt-2">
                {displayDate && (
                  <div className="flex items-center gap-1 text-sm text-muted-foreground">
                    <Calendar className="h-4 w-4" />
                    <span>Period: {displayDate}</span>
                  </div>
                )}
                <div className="flex items-center gap-1 text-sm text-muted-foreground">
                  <Clock className="h-4 w-4" />
                  <span>Created: {createdDate}</span>
                </div>
              </div>
            </div>
          </div>
        </CardHeader>
        <CardFooter className="pt-0 border-t">
          <div className="flex flex-wrap gap-1 w-full justify-end min-w-0">
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
                onClick={handleDeleteClick}
                className={DELETE_ICON_BUTTON_CLASSNAME}
                aria-label={`Delete summary ${name || "Untitled Summary"}`}
              >
                <Trash2 className="size-4" aria-hidden />
              </Button>
            )}
          </div>
        </CardFooter>
      </Card>

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
    </>
  );
}
