import { jsPDF } from "jspdf";
import { renderMarkdownToPdf } from "./markdownRenderer";
import { createPdfLayoutContext, createPdfTextRenderer } from "./textRenderer";
import { DEFAULT_PDF_OPTIONS, type PdfGeneratorOptions } from "./types";

/**
 * Generate a PDF from markdown content.
 * Matches the styling of the original Python reportlab implementation.
 */
export function generatePdfFromMarkdown(
  markdown: string,
  options: PdfGeneratorOptions = {},
): Blob {
  const opts = { ...DEFAULT_PDF_OPTIONS, ...options };
  const fontSize = { ...DEFAULT_PDF_OPTIONS.fontSize, ...opts.fontSize };
  const doc = new jsPDF({
    orientation: "portrait",
    unit: "pt",
    format: "a4",
  });

  const ctx = createPdfLayoutContext(doc, opts.margin, fontSize);
  const renderer = createPdfTextRenderer(ctx);
  renderMarkdownToPdf(markdown, ctx, renderer);

  return doc.output("blob");
}

export type { PdfGeneratorOptions } from "./types";
