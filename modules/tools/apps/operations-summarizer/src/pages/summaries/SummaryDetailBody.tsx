import { AlertCircle, FileText, RefreshCw } from "lucide-react";
import type { Dispatch, SetStateAction } from "react";
import { Loader } from "@/components/Loader";
import { Button } from "@/components/ui/button";
import { markdownToHtml } from "@/utils/formatting";
import { SummaryPdfViewer } from "./SummaryPdfViewer";

export interface SummaryDetailBodyProps {
  isLoading: boolean;
  isPdfFile: boolean;
  pdfError: string | null;
  onRetryPdf: () => void;
  needsRemoteTextFile: boolean;
  isFileContentError: boolean;
  onRetryFile: () => void;
  pdfFile: Blob | null;
  pdfCurrentPage: number;
  pdfNumPages: number | null;
  pdfScale: number;
  setPdfNumPages: Dispatch<SetStateAction<number | null>>;
  setPdfCurrentPage: Dispatch<SetStateAction<number>>;
  setPdfScale: Dispatch<SetStateAction<number>>;
  modalContent: string;
}

export function SummaryDetailBody({
  isLoading,
  isPdfFile,
  pdfError,
  onRetryPdf,
  needsRemoteTextFile,
  isFileContentError,
  onRetryFile,
  pdfFile,
  pdfCurrentPage,
  pdfNumPages,
  pdfScale,
  setPdfNumPages,
  setPdfCurrentPage,
  setPdfScale,
  modalContent,
}: SummaryDetailBodyProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader size={24} />
        <span className="ml-3 text-muted-foreground">Loading content...</span>
      </div>
    );
  }

  if (isPdfFile && pdfError) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-12 px-6 text-center">
        <AlertCircle className="h-10 w-10 text-destructive shrink-0" />
        <p className="text-sm text-destructive max-w-md">{pdfError}</p>
        <Button type="button" variant="outline" size="sm" onClick={onRetryPdf}>
          <RefreshCw className="h-4 w-4 mr-1" />
          Retry
        </Button>
      </div>
    );
  }

  if (needsRemoteTextFile && isFileContentError) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-12 px-6 text-center">
        <AlertCircle className="h-10 w-10 text-destructive shrink-0" />
        <p className="text-sm text-destructive max-w-md">
          Failed to load file content. Check your connection and try again.
        </p>
        <Button type="button" variant="outline" size="sm" onClick={onRetryFile}>
          <RefreshCw className="h-4 w-4 mr-1" />
          Retry
        </Button>
      </div>
    );
  }

  if (isPdfFile && pdfFile) {
    return (
      <SummaryPdfViewer
        pdfFile={pdfFile}
        pdfCurrentPage={pdfCurrentPage}
        pdfNumPages={pdfNumPages}
        pdfScale={pdfScale}
        setPdfNumPages={setPdfNumPages}
        setPdfCurrentPage={setPdfCurrentPage}
        setPdfScale={setPdfScale}
      />
    );
  }

  if (modalContent) {
    return (
      <div className="max-w-4xl mx-auto py-4 px-3 sm:py-6 sm:px-6">
        <div className="bg-background shadow-sm border border-border rounded-lg px-4 py-4 sm:px-8 sm:py-6 overflow-x-auto">
          <div
            className="prose prose-sm dark:prose-invert max-w-none w-full leading-relaxed
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
              __html: markdownToHtml(modalContent),
            }}
          />
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
      <FileText className="h-10 w-10 text-muted-foreground/50 mb-3" />
      <p>No content available</p>
    </div>
  );
}
