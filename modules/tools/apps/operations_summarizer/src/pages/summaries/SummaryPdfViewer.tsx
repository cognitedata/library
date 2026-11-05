import { ChevronLeft, ChevronRight, ZoomIn, ZoomOut } from "lucide-react";
import { type Dispatch, type SetStateAction, useCallback } from "react";
import { Document, Page } from "react-pdf";
import { Loader } from "@/components/Loader";
import { Button } from "@/components/ui/button";

export interface SummaryPdfViewerProps {
  pdfFile: Blob;
  pdfCurrentPage: number;
  pdfNumPages: number | null;
  pdfScale: number;
  setPdfNumPages: Dispatch<SetStateAction<number | null>>;
  setPdfCurrentPage: Dispatch<SetStateAction<number>>;
  setPdfScale: Dispatch<SetStateAction<number>>;
}

export function SummaryPdfViewer({
  pdfFile,
  pdfCurrentPage,
  pdfNumPages,
  pdfScale,
  setPdfNumPages,
  setPdfCurrentPage,
  setPdfScale,
}: SummaryPdfViewerProps) {
  const handleLoadSuccess = useCallback(
    ({ numPages }: { numPages: number }) => {
      setPdfNumPages(numPages);
    },
    [setPdfNumPages],
  );

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-auto flex justify-center py-6 px-4">
        <div className="border border-border rounded-lg shadow-lg bg-white overflow-auto max-h-full">
          <Document
            file={pdfFile}
            onLoadSuccess={handleLoadSuccess}
            onLoadError={(error) => console.error("PDF load error:", error)}
            loading={
              <div className="flex items-center justify-center p-12">
                <Loader size={24} />
              </div>
            }
          >
            <Page
              pageNumber={pdfCurrentPage}
              scale={pdfScale}
              renderTextLayer={false}
              renderAnnotationLayer={false}
            />
          </Document>
        </div>
      </div>

      <div className="shrink-0 flex justify-center py-3 px-2 sm:px-4 border-t border-border bg-background">
        <div className="flex flex-wrap items-center justify-center gap-2 sm:gap-4 bg-muted/50 border border-border rounded-lg px-2 py-2 sm:px-4 max-w-full">
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPdfCurrentPage((page) => Math.max(1, page - 1))}
              disabled={pdfCurrentPage <= 1}
              className="h-8 w-8 p-0"
              aria-label="Previous PDF page"
            >
              <ChevronLeft className="h-4 w-4" aria-hidden />
            </Button>
            <span className="text-sm text-muted-foreground min-w-[80px] text-center">
              Page {pdfCurrentPage} of {pdfNumPages || "?"}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() =>
                setPdfCurrentPage((page) =>
                  Math.min(pdfNumPages || page, page + 1),
                )
              }
              disabled={pdfCurrentPage >= (pdfNumPages || 1)}
              className="h-8 w-8 p-0"
              aria-label="Next PDF page"
            >
              <ChevronRight className="h-4 w-4" aria-hidden />
            </Button>
          </div>
          <div className="h-4 w-px bg-border" />
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPdfScale((scale) => Math.max(0.5, scale - 0.1))}
              disabled={pdfScale <= 0.5}
              className="h-8 w-8 p-0"
              aria-label="Zoom out PDF"
            >
              <ZoomOut className="h-4 w-4" aria-hidden />
            </Button>
            <span className="text-sm text-muted-foreground min-w-[50px] text-center">
              {Math.round(pdfScale * 100)}%
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPdfScale((scale) => Math.min(2.0, scale + 0.1))}
              disabled={pdfScale >= 2.0}
              className="h-8 w-8 p-0"
              aria-label="Zoom in PDF"
            >
              <ZoomIn className="h-4 w-4" aria-hidden />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
