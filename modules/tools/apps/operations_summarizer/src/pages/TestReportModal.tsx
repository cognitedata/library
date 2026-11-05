/**
 * Test Report Modal — full-screen dialog for viewing a test report's content.
 */

import { Download, History } from "lucide-react";
import { useMemo } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { PAGE_COLUMN_WIDTH_CLASSNAME } from "@/lib/pageColumnWidth";
import { cn } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { markdownToHtml } from "@/utils/formatting";
import {
  downloadSummaryAsMarkdown,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";
import {
  extractTestReportData,
  getScoreBadgeVariant,
  vividScoreBadgeClassName,
} from "@/utils/testReportUtils";

export interface TestReportModalProps {
  selectedReport: SummaryInstance | null;
  isOpen: boolean;
  onClose: () => void;
}

export function TestReportModal({
  selectedReport,
  isOpen,
  onClose,
}: TestReportModalProps) {
  const reportData = useMemo(() => {
    if (!selectedReport) return null;
    const displayDate = getSummaryDisplayDate(selectedReport);
    return extractTestReportData(selectedReport, displayDate);
  }, [selectedReport]);

  const scoreVariant = reportData
    ? getScoreBadgeVariant(reportData.score)
    : null;
  const accuracyVariant = reportData
    ? getScoreBadgeVariant(reportData.accuracy)
    : null;

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent
        className={cn(
          PAGE_COLUMN_WIDTH_CLASSNAME,
          "h-[min(90dvh,85vh)] max-h-[90dvh] flex flex-col p-0 gap-0 min-h-0",
        )}
      >
        {/* Document Info Bar */}
        <div className="bg-muted/50 border-b border-border px-4 py-3 shrink-0">
          <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-3 md:gap-4">
            <div className="space-y-1 flex-1 min-w-0">
              <div className="flex flex-col md:flex-row md:items-center gap-2">
                <div className="flex items-center gap-2">
                  <History className="w-5 h-5 text-primary shrink-0" />
                  <DialogTitle className="text-base font-semibold text-foreground truncate">
                    {reportData?.name || "Test Results"}
                  </DialogTitle>
                </div>
                {reportData && scoreVariant && accuracyVariant && (
                  <div className="flex gap-2">
                    <Badge
                      variant={scoreVariant}
                      className={cn(
                        "text-xs",
                        vividScoreBadgeClassName(scoreVariant),
                      )}
                    >
                      {reportData.score.toFixed(0)}% Similarity
                    </Badge>
                    <Badge
                      variant={accuracyVariant}
                      className={cn(
                        "text-xs",
                        vividScoreBadgeClassName(accuracyVariant),
                      )}
                    >
                      {reportData.accuracy.toFixed(0)}% Accuracy
                    </Badge>
                  </div>
                )}
              </div>
              <div className="flex flex-col md:flex-wrap md:flex-row md:items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
                {reportData && (
                  <>
                    {reportData.period && (
                      <span className="flex items-center gap-1">
                        <span className="font-medium text-foreground shrink-0">
                          Period:
                        </span>
                        <span className="truncate">{reportData.period}</span>
                      </span>
                    )}
                    <span className="flex items-center gap-1">
                      <span className="font-medium text-foreground shrink-0">
                        Tested:
                      </span>
                      <span className="truncate">{reportData.date}</span>
                    </span>
                    {reportData.dataTypes.length > 0 && (
                      <span className="flex items-center gap-1">
                        <span className="font-medium text-foreground shrink-0">
                          Data Types:
                        </span>
                        <span className="truncate">
                          {reportData.dataTypes.join(", ")}
                        </span>
                      </span>
                    )}
                  </>
                )}
              </div>
            </div>
            {/* Desktop action buttons */}
            <div className="hidden md:flex items-center gap-2 md:shrink-0 md:pr-10">
              {selectedReport && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => downloadSummaryAsMarkdown(selectedReport)}
                  className="h-8"
                >
                  <Download className="w-4 h-4 mr-1" />
                  Download
                </Button>
              )}
            </div>
          </div>
          {/* Mobile action buttons */}
          <div className="md:hidden flex items-center gap-2 mt-3">
            {selectedReport && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => downloadSummaryAsMarkdown(selectedReport)}
                className="h-8"
              >
                <Download className="w-4 h-4 mr-1" />
                Download
              </Button>
            )}
          </div>
        </div>

        {/* Content Area */}
        <div className="flex-1 min-h-0 overflow-auto bg-muted/30 py-4 px-3 sm:py-6 sm:px-8 lg:px-12">
          {reportData && (
            <div
              className="prose prose-sm dark:prose-invert max-w-none leading-relaxed
                prose-headings:font-semibold prose-headings:text-foreground
                prose-h1:text-xl prose-h1:border-b prose-h1:pb-2 prose-h1:mb-4
                prose-h2:text-lg prose-h2:mt-6 prose-h2:mb-3
                prose-h3:text-base prose-h3:mt-4 prose-h3:mb-2
                prose-p:text-sm prose-p:text-muted-foreground prose-p:my-2
                prose-li:text-sm prose-li:text-muted-foreground
                prose-strong:text-foreground
                prose-table:text-sm prose-th:bg-muted/50 prose-th:px-3 prose-th:py-2
                prose-td:px-3 prose-td:py-2 prose-td:border-b"
              /* biome-ignore lint/security/noDangerouslySetInnerHtml: rendered from app-controlled markdown */
              dangerouslySetInnerHTML={{
                __html: markdownToHtml(reportData.content),
              }}
            />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
