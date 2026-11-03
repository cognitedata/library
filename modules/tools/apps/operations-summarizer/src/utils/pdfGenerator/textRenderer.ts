import type { jsPDF } from "jspdf";
import type { PdfLayoutContext } from "./types";

/**
 * Low-level text rendering with inline bold (**text**) support.
 */
export function createPdfTextRenderer(ctx: PdfLayoutContext) {
  const { doc, margin, contentWidth } = ctx;

  const renderTextWithBold = (
    text: string,
    startX: number,
    fontSize: number,
    maxWidth: number,
  ) => {
    doc.setFontSize(fontSize);
    const lineHeightPt = fontSize * 1.3;

    const segments: Array<{ text: string; bold: boolean }> = [];
    let remaining = text;

    while (remaining.length > 0) {
      const boldMatch = remaining.match(/\*\*(.+?)\*\*/);
      if (boldMatch && boldMatch.index !== undefined) {
        if (boldMatch.index > 0) {
          segments.push({
            text: remaining.substring(0, boldMatch.index),
            bold: false,
          });
        }
        segments.push({ text: boldMatch[1], bold: true });
        remaining = remaining.substring(boldMatch.index + boldMatch[0].length);
      } else {
        segments.push({ text: remaining, bold: false });
        break;
      }
    }

    if (segments.length === 1 && !segments[0].bold) {
      doc.setFont("helvetica", "normal");
      const wrappedLines = doc.splitTextToSize(text, maxWidth);
      for (const line of wrappedLines) {
        ctx.checkPageBreak(lineHeightPt);
        doc.text(line, startX, ctx.y);
        ctx.advanceY(lineHeightPt);
      }
      return;
    }

    const tokens: Array<{ text: string; bold: boolean }> = [];
    for (const segment of segments) {
      for (const part of segment.text.split(/(\s+)/)) {
        if (part.length > 0) {
          tokens.push({ text: part, bold: segment.bold });
        }
      }
    }

    const renderedLines: Array<Array<{ text: string; bold: boolean }>> = [];
    let currentLine: Array<{ text: string; bold: boolean }> = [];
    let currentLineWidth = 0;

    for (const token of tokens) {
      doc.setFont("helvetica", token.bold ? "bold" : "normal");
      const tokenWidth = doc.getTextWidth(token.text);

      if (currentLine.length > 0 && currentLineWidth + tokenWidth > maxWidth) {
        renderedLines.push(currentLine);
        if (token.text.trim().length === 0) {
          currentLine = [];
          currentLineWidth = 0;
        } else {
          currentLine = [token];
          currentLineWidth = tokenWidth;
        }
      } else {
        currentLine.push(token);
        currentLineWidth += tokenWidth;
      }
    }
    if (currentLine.length > 0) {
      renderedLines.push(currentLine);
    }

    for (const line of renderedLines) {
      ctx.checkPageBreak(lineHeightPt);
      let cx = startX;
      for (const token of line) {
        doc.setFont("helvetica", token.bold ? "bold" : "normal");
        doc.text(token.text, cx, ctx.y);
        cx += doc.getTextWidth(token.text);
      }
      ctx.advanceY(lineHeightPt);
    }
  };

  const printWrappedText = (
    text: string,
    fontSize: number,
    fontStyle: "normal" | "bold" | "italic" = "normal",
    indent = 0,
  ) => {
    if (text.includes("**")) {
      renderTextWithBold(
        text,
        margin + indent,
        fontSize,
        contentWidth - indent,
      );
      return;
    }

    doc.setFontSize(fontSize);
    doc.setFont("helvetica", fontStyle);

    const effectiveWidth = contentWidth - indent;
    const lines = doc.splitTextToSize(text, effectiveWidth);
    const lineHeightPt = fontSize * 1.3;

    for (const line of lines) {
      ctx.checkPageBreak(lineHeightPt);
      doc.text(line, margin + indent, ctx.y);
      ctx.advanceY(lineHeightPt);
    }
  };

  return { renderTextWithBold, printWrappedText };
}

export type PdfTextRenderer = ReturnType<typeof createPdfTextRenderer>;

export function createPdfLayoutContext(
  doc: jsPDF,
  margin: number,
  fontSize: Required<
    NonNullable<import("./types").PdfGeneratorOptions["fontSize"]>
  >,
): PdfLayoutContext {
  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();
  const contentWidth = pageWidth - 2 * margin;

  const lineHeight = {
    title: fontSize.title * 1.5,
    heading: fontSize.heading * 1.4,
    body: fontSize.body * 1.3,
  };

  const state = { y: margin };

  const checkPageBreak = (neededHeight: number) => {
    if (state.y + neededHeight > pageHeight - margin) {
      doc.addPage();
      state.y = margin;
      return true;
    }
    return false;
  };

  return {
    doc,
    margin,
    contentWidth,
    pageWidth,
    pageHeight,
    fontSize,
    lineHeight,
    get y() {
      return state.y;
    },
    checkPageBreak,
    setY: (nextY: number) => {
      state.y = nextY;
    },
    advanceY: (delta: number) => {
      state.y += delta;
    },
  };
}
