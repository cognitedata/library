/**
 * Summary Result Panel — document-view area displayed after a summary is generated.
 * Shows the generated markdown, a period/generation metadata bar,
 * and Save/Download actions.
 */

import {
  AlertCircle,
  CheckCircle2,
  ExternalLink,
  FileDown,
  FileText,
  Save,
} from "lucide-react";
import { Loader } from "@/components/Loader";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { getDisplayName } from "@/config/dataTypes";
import { cn } from "@/lib/utils";
import type { DataTypeName, SummaryContents } from "@/types/operations";
import {
  formatDateDisplay,
  formatTimestamp,
  markdownToHtml,
} from "@/utils/formatting";

export interface SavedPdfInfo {
  fileName: string;
  fileId: string | null;
  fusionUrl?: string;
}

export interface SummaryResultPanelProps {
  generatedSummary: {
    fullText: string;
    generatedAt: Date;
    dataTypeTruncation?: Partial<
      Record<DataTypeName, { limit: number; hadMoreInCdf: boolean }>
    >;
  };
  periodStart: Date;
  periodEnd: Date;
  summaryContents: SummaryContents;
  savedPdfInfo: SavedPdfInfo | null;
  isSaving: boolean;
  onSave?: () => Promise<void>;
  onDownload: () => void;
  saveError?: string | null;
}

export function SummaryResultPanel({
  generatedSummary,
  periodStart,
  periodEnd,
  summaryContents,
  savedPdfInfo,
  isSaving,
  onSave,
  onDownload,
  saveError,
}: SummaryResultPanelProps) {
  const activeDataTypes = Object.entries(summaryContents)
    .filter(([, v]) => v)
    .map(([k]) => getDisplayName(k))
    .join(", ");

  return (
    <div className="flex flex-col min-h-0 max-h-[min(78dvh,calc(100vh-10rem))] sm:max-h-[calc(100vh-220px)]">
      {/* Document Info Bar */}
      <div className="bg-muted/50 border border-border rounded-t-lg px-3 py-3 sm:px-4 shrink-0">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-1 min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <FileText className="w-5 h-5 text-primary shrink-0" />
              <h2 className="text-base font-semibold text-foreground">
                Operations Summary
              </h2>
              {savedPdfInfo && (
                <Badge variant="success" className="text-xs">
                  <CheckCircle2 className="w-3 h-3 mr-1" aria-hidden />
                  Saved
                </Badge>
              )}
            </div>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
              <span className="flex flex-wrap items-center gap-1 min-w-0">
                <span className="font-medium text-foreground shrink-0">
                  Period:
                </span>
                <span className="break-words">
                  {formatDateDisplay(periodStart)} —{" "}
                  {formatDateDisplay(periodEnd)}
                </span>
              </span>
              <span className="flex flex-wrap items-center gap-1">
                <span className="font-medium text-foreground shrink-0">
                  Generated:
                </span>
                {formatTimestamp(generatedSummary.generatedAt)}
              </span>
              <span className="flex flex-wrap items-center gap-1 min-w-0">
                <span className="font-medium text-foreground shrink-0">
                  Data Types:
                </span>
                <span className="break-words">{activeDataTypes}</span>
              </span>
            </div>
          </div>
          <div className="flex flex-col gap-2 sm:items-end shrink-0 w-full sm:w-auto">
            <div className="flex flex-wrap items-stretch sm:items-center gap-2 w-full sm:w-auto sm:justify-end">
              <Button
                variant="outline"
                size="sm"
                onClick={onDownload}
                className="h-8 flex-1 sm:flex-initial min-w-0"
              >
                <FileDown className="w-4 h-4 mr-1 shrink-0" />
                Download MD
              </Button>
              {onSave ? (
                <Button
                  size="sm"
                  onClick={onSave}
                  disabled={isSaving || !!savedPdfInfo}
                  className="h-8 flex-1 sm:flex-initial min-w-0"
                >
                  {isSaving ? (
                    <Loader size={14} className="mr-1" />
                  ) : (
                    <Save className="w-4 h-4 mr-1 shrink-0" />
                  )}
                  {savedPdfInfo ? "Saved to Cognite" : "Save to Cognite"}
                </Button>
              ) : null}
              {savedPdfInfo?.fusionUrl ? (
                <a
                  href={savedPdfInfo.fusionUrl}
                  target="_blank"
                  rel="noreferrer"
                  className={cn(
                    buttonVariants({ variant: "outline", size: "sm" }),
                    "h-8 flex-1 sm:flex-initial min-w-0",
                  )}
                >
                  <ExternalLink className="size-4 shrink-0" />
                  Open in Fusion
                </a>
              ) : null}
            </div>
            {saveError && (
              <span className="flex items-start gap-1 text-xs text-destructive sm:text-right">
                <AlertCircle className="w-3 h-3 shrink-0 mt-0.5" />
                {saveError}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Document Content */}
      <div className="flex-1 min-h-0 overflow-auto bg-muted/30 border-x border-b border-border rounded-b-lg">
        <div className="max-w-4xl mx-auto py-6 sm:py-8 px-3 sm:px-4">
          {generatedSummary.dataTypeTruncation &&
            Object.keys(generatedSummary.dataTypeTruncation).length > 0 && (
              <output
                className="mb-4 flex gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-foreground block"
                aria-live="polite"
              >
                <AlertCircle className="h-4 w-4 shrink-0 text-amber-600 mt-0.5" />
                <div>
                  <p className="font-medium">Incomplete data for some types</p>
                  <p className="text-muted-foreground text-xs mt-1">
                    The summary hit the per-type instance cap while more data
                    exists in CDF. See the note at the top of the document.
                  </p>
                </div>
              </output>
            )}
          <div className="bg-background shadow-lg border border-border rounded-lg px-4 py-6 sm:px-10 sm:py-8 min-h-full overflow-x-auto">
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
              // biome-ignore lint/security/noDangerouslySetInnerHtml: app-controlled markdown via markdownToHtml
              dangerouslySetInnerHTML={{
                __html: markdownToHtml(generatedSummary.fullText),
              }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
