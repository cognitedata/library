/**
 * Test Card Component for Mobile Display
 *
 * Displays test report information in a card format optimized for mobile screens.
 */

import {
  BarChart3,
  Calendar,
  Clock,
  Database,
  Download,
  Eye,
  FlaskConical,
  Trash2,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { DELETE_ICON_BUTTON_CLASSNAME } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { formatDateTimeShort } from "@/utils/formatting";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";
import {
  extractDataVolumeFromContent,
  extractPeriodFromContent,
  extractScoreFromContent,
  getScoreBadgeVariant,
  vividScoreBadgeClassName,
} from "@/utils/testReportUtils";

export interface TestCardProps {
  /** Test report to display */
  testReport: SummaryInstance;
  /** Callback when view is clicked */
  onView?: (report: SummaryInstance) => void;
  /** Callback when download is clicked */
  onDownload?: (report: SummaryInstance) => void;
  /** Callback when delete is clicked */
  onDelete?: (report: SummaryInstance) => void;
}

export function TestCard({
  testReport,
  onView,
  onDownload,
  onDelete,
}: TestCardProps) {
  const { name, content } = extractSummaryContent(testReport);
  const date = getSummaryDisplayDate(testReport);
  const score = extractScoreFromContent(content);
  const volume = extractDataVolumeFromContent(content);
  const period =
    extractPeriodFromContent(content) ||
    extractPeriodFromContent(testReport.externalId);
  const createdDate = formatDateTimeShort(new Date(testReport.createdTime));
  const scoreVariant = getScoreBadgeVariant(score);

  return (
    <Card className="transition-colors hover:bg-muted/30 min-w-0">
      <CardHeader className="min-w-0">
        <div className="flex items-start gap-3 min-w-0">
          <FlaskConical className="h-5 w-5 text-muted-foreground shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0 overflow-hidden">
            <CardTitle
              className="text-base font-medium text-foreground min-w-0 max-sm:line-clamp-3 max-sm:break-words max-sm:leading-snug sm:truncate sm:whitespace-nowrap"
              title={name || "Untitled Test"}
            >
              {name || "Untitled Test"}
            </CardTitle>
            <div className="flex flex-col gap-2 mt-3">
              {(period || date) && (
                <div className="flex items-center gap-1 text-sm text-muted-foreground">
                  <Calendar className="h-4 w-4" />
                  <span>Period: {period || date}</span>
                </div>
              )}
              <div className="flex items-center gap-1 text-sm text-muted-foreground">
                <Clock className="h-4 w-4" />
                <span>Tested: {createdDate}</span>
              </div>
              <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between mt-2 min-w-0">
                <div className="flex flex-wrap items-center gap-2 min-w-0">
                  <div className="flex items-center gap-1 shrink-0">
                    <BarChart3 className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm text-muted-foreground">
                      Score:
                    </span>
                  </div>
                  <Badge
                    variant={scoreVariant}
                    className={vividScoreBadgeClassName(scoreVariant)}
                  >
                    {score.toFixed(1)}%
                  </Badge>
                </div>
                <div className="flex items-center gap-1 min-w-0">
                  <Database className="h-4 w-4 text-muted-foreground shrink-0" />
                  <span className="text-sm text-muted-foreground truncate">
                    {volume}
                  </span>
                </div>
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
              onClick={() => onView(testReport)}
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
              onClick={() => onDownload(testReport)}
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
              onClick={() => onDelete(testReport)}
              className={DELETE_ICON_BUTTON_CLASSNAME}
              aria-label={`Delete test report ${name || "Untitled Test"}`}
            >
              <Trash2 className="size-4" aria-hidden />
            </Button>
          )}
        </div>
      </CardFooter>
    </Card>
  );
}
