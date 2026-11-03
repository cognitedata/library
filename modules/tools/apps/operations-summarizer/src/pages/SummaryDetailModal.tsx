/**
 * Summary Detail Modal — full-screen dialog for viewing a past summary.
 * Owns: PDF page/scale state, usePdfBlob, useFileContent, Summary/Podcast tabs.
 */

import { X } from "lucide-react";
import { useEffect, useState } from "react";
import { PodcastTab } from "@/components/podcast/PodcastTab";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "@/components/ui/dialog";
import { useFileContent } from "@/hooks/useFileContent";
import { usePdfBlob } from "@/hooks/usePdfBlob";
import { PAGE_COLUMN_WIDTH_CLASSNAME } from "@/lib/pageColumnWidth";
import { cn } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";
import { extractSummaryContent } from "@/utils/summaryDownload";
import "react-pdf/dist/Page/TextLayer.css";
import "react-pdf/dist/Page/AnnotationLayer.css";
import { SummaryDetailBody } from "./summaries/SummaryDetailBody";
import {
  SummaryDetailHeader,
  type SummaryModalTab,
} from "./summaries/SummaryDetailHeader";

export interface SummaryDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  selectedSummary: SummaryInstance | null;
  onDownloadMd: (summary: SummaryInstance) => void;
  onDownloadPdf: (summary: SummaryInstance) => void;
}

export function SummaryDetailModal({
  isOpen,
  onClose,
  selectedSummary,
  onDownloadMd,
  onDownloadPdf,
}: SummaryDetailModalProps) {
  const [pdfNumPages, setPdfNumPages] = useState<number | null>(null);
  const [pdfCurrentPage, setPdfCurrentPage] = useState(1);
  const [pdfScale, setPdfScale] = useState(1);
  const [activeTab, setActiveTab] = useState<SummaryModalTab>("summary");
  /** Block outside-dismiss briefly after open so the opening click does not ghost-close the dialog. */
  const [blockPointerDismissal, setBlockPointerDismissal] = useState(false);

  useEffect(() => {
    if (!isOpen) {
      setPdfNumPages(null);
      setPdfCurrentPage(1);
      setPdfScale(1);
      setActiveTab("summary");
      setBlockPointerDismissal(false);
      return;
    }
    setBlockPointerDismissal(true);
    let raf1 = 0;
    let raf2 = 0;
    raf1 = requestAnimationFrame(() => {
      raf2 = requestAnimationFrame(() => {
        setBlockPointerDismissal(false);
      });
    });
    return () => {
      cancelAnimationFrame(raf1);
      cancelAnimationFrame(raf2);
    };
  }, [isOpen]);

  const summaryData = selectedSummary
    ? extractSummaryContent(selectedSummary)
    : null;
  const isPdfFile =
    summaryData?.name?.toLowerCase().endsWith(".pdf") ||
    summaryData?.mimeType === "application/pdf";

  const { pdfFile, isLoadingPdf, pdfError, retryPdf } = usePdfBlob({
    space: selectedSummary?.space,
    externalId: selectedSummary?.externalId,
    enabled: isOpen && !!selectedSummary && !!isPdfFile,
  });

  const fileContentParams =
    isOpen && selectedSummary && !summaryData?.content && !isPdfFile
      ? {
          space: selectedSummary.space,
          externalId: selectedSummary.externalId,
        }
      : null;

  const {
    data: fileContent,
    isLoading: isLoadingContent,
    isError: isFileContentError,
    refetch: refetchFileContent,
  } = useFileContent(fileContentParams);

  const needsRemoteTextFile = fileContentParams !== null;
  const modalContent = summaryData?.content || fileContent || "";

  return (
    <Dialog
      open={isOpen}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) onClose();
      }}
      disablePointerDismissal={blockPointerDismissal}
    >
      <DialogContent
        showCloseButton={false}
        className={cn(
          PAGE_COLUMN_WIDTH_CLASSNAME,
          "h-[min(90dvh,85vh)] max-h-[90dvh] flex flex-col p-0 gap-0 min-h-0 rounded-xl border-border shadow-lg",
        )}
      >
        <DialogClose
          render={
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              className="absolute top-4 right-4 z-10 opacity-70 hover:opacity-100"
              aria-label="Close summary"
            >
              <X className="h-4 w-4" aria-hidden />
            </Button>
          }
        />
        {selectedSummary && (
          <SummaryDetailHeader
            selectedSummary={selectedSummary}
            summaryName={summaryData?.name || "Operations Summary"}
            isPdfFile={Boolean(isPdfFile)}
            activeTab={activeTab}
            onTabChange={setActiveTab}
            onDownloadMd={onDownloadMd}
            onDownloadPdf={onDownloadPdf}
          />
        )}

        <DialogTitle className="sr-only">
          {summaryData?.name || "Summary Details"}
        </DialogTitle>
        <DialogDescription className="sr-only">
          View and download the selected operations summary.
        </DialogDescription>

        <div className="flex-1 min-h-0 overflow-auto bg-muted/20">
          {activeTab === "summary" ? (
            <div
              role="tabpanel"
              id="summary-panel"
              aria-labelledby="summary-tab"
            >
              <SummaryDetailBody
                isLoading={isLoadingContent || isLoadingPdf}
                isPdfFile={Boolean(isPdfFile)}
                pdfError={pdfError}
                onRetryPdf={() => retryPdf()}
                needsRemoteTextFile={needsRemoteTextFile}
                isFileContentError={isFileContentError}
                onRetryFile={() => void refetchFileContent()}
                pdfFile={pdfFile}
                pdfCurrentPage={pdfCurrentPage}
                pdfNumPages={pdfNumPages}
                pdfScale={pdfScale}
                setPdfNumPages={setPdfNumPages}
                setPdfCurrentPage={setPdfCurrentPage}
                setPdfScale={setPdfScale}
                modalContent={modalContent}
              />
            </div>
          ) : (
            selectedSummary && (
              <div
                role="tabpanel"
                id="podcast-panel"
                aria-labelledby="podcast-tab"
              >
                <PodcastTab summary={selectedSummary} />
              </div>
            )
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
