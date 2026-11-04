import "pdfjs-dist";

/**
 * pdfjs-dist 6 removed PDFDocumentProxy.destroy from public types
 * (cleanup() / loadingTask.destroy() are the replacements).
 * PDFRenderService.clearCache calls loadingTask.destroy(), which remains public.
 */
declare module "pdfjs-dist/types/src/display/api" {
	interface PDFDocumentProxy {
		destroy(): Promise<void>;
	}
}
