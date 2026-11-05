import {
  Calendar,
  Download,
  FileText,
  Headphones,
  History,
  Save,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { formatDateTimeShort } from "@/utils/formatting";
import { getContentTypes } from "@/utils/summaryContentTypes";
import { getSummaryDisplayDate } from "@/utils/summaryDownload";

export type SummaryModalTab = "summary" | "podcast";

export interface SummaryDetailHeaderProps {
  selectedSummary: SummaryInstance;
  summaryName: string;
  isPdfFile: boolean;
  activeTab: SummaryModalTab;
  onTabChange: (tab: SummaryModalTab) => void;
  onDownloadMd: (summary: SummaryInstance) => void;
  onDownloadPdf: (summary: SummaryInstance) => void;
}

function DownloadActions({
  selectedSummary,
  onDownloadMd,
  onDownloadPdf,
}: Pick<
  SummaryDetailHeaderProps,
  "selectedSummary" | "onDownloadMd" | "onDownloadPdf"
>) {
  return (
    <>
      <Button
        variant="outline"
        size="sm"
        onClick={() => onDownloadMd(selectedSummary)}
        className="h-8"
      >
        <Download className="h-4 w-4 mr-1" />
        Markdown
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={() => onDownloadPdf(selectedSummary)}
        className="h-8"
      >
        <Save className="h-4 w-4 mr-1" />
        PDF
      </Button>
    </>
  );
}

export function SummaryDetailHeader({
  selectedSummary,
  summaryName,
  isPdfFile,
  activeTab,
  onTabChange,
  onDownloadMd,
  onDownloadPdf,
}: SummaryDetailHeaderProps) {
  return (
    <div className="bg-muted/50 border-b px-3 py-3 sm:px-6 sm:py-4 shrink-0">
      <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4 md:gap-6">
        <div className="space-y-3 flex-1 min-w-0 pr-10 sm:pr-0">
          <div className="flex flex-wrap items-center gap-2 sm:gap-3">
            <FileText className="h-6 w-6 text-primary shrink-0" />
            <h2 className="text-lg sm:text-xl font-semibold text-foreground truncate min-w-0">
              {summaryName}
            </h2>
            {isPdfFile && (
              <Badge variant="secondary" className="text-xs shrink-0">
                PDF
              </Badge>
            )}
          </div>
          <div className="flex flex-col md:flex-row md:flex-wrap md:items-center gap-x-6 gap-y-2 text-sm">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Calendar className="h-4 w-4 shrink-0" />
              <span className="font-medium text-foreground">Period:</span>
              <span className="truncate">
                {getSummaryDisplayDate(selectedSummary) || "—"}
              </span>
            </div>
            <div className="flex items-center gap-2 text-muted-foreground">
              <History className="h-4 w-4 shrink-0" />
              <span className="font-medium text-foreground">Created:</span>
              <span className="truncate">
                {formatDateTimeShort(new Date(selectedSummary.createdTime))}
              </span>
            </div>
          </div>
          <div className="flex flex-col md:flex-row md:items-center gap-2 text-sm">
            <span className="font-medium text-foreground shrink-0">
              Content Types:
            </span>
            <div className="flex flex-wrap gap-1">
              {getContentTypes(selectedSummary).map((type) => (
                <Badge key={type} variant="outline" className="text-xs">
                  {type}
                </Badge>
              ))}
            </div>
          </div>
        </div>
        <div className="hidden md:flex items-center gap-2 md:shrink-0 md:pr-10">
          <DownloadActions
            selectedSummary={selectedSummary}
            onDownloadMd={onDownloadMd}
            onDownloadPdf={onDownloadPdf}
          />
        </div>
      </div>
      <div className="md:hidden flex items-center gap-2 mt-3">
        <DownloadActions
          selectedSummary={selectedSummary}
          onDownloadMd={onDownloadMd}
          onDownloadPdf={onDownloadPdf}
        />
      </div>

      <div
        role="tablist"
        aria-label="Summary views"
        className="flex items-center gap-1 mt-4 -mb-3 sm:-mb-4 border-b border-transparent"
      >
        <button
          type="button"
          role="tab"
          id="summary-tab"
          aria-selected={activeTab === "summary"}
          aria-controls="summary-panel"
          tabIndex={activeTab === "summary" ? 0 : -1}
          onClick={() => onTabChange("summary")}
          className={cn(
            "inline-flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 transition-colors",
            activeTab === "summary"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          )}
        >
          <FileText className="h-4 w-4" aria-hidden />
          Summary
        </button>
        <button
          type="button"
          role="tab"
          id="podcast-tab"
          aria-selected={activeTab === "podcast"}
          aria-controls="podcast-panel"
          tabIndex={activeTab === "podcast" ? 0 : -1}
          onClick={() => onTabChange("podcast")}
          className={cn(
            "inline-flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 transition-colors",
            activeTab === "podcast"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          )}
        >
          <Headphones className="h-4 w-4" aria-hidden />
          Podcast
        </button>
      </div>
    </div>
  );
}
