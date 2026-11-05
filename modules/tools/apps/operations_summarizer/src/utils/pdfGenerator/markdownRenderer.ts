import type { PdfTextRenderer } from "./textRenderer";
import type { PdfLayoutContext } from "./types";

/**
 * Parse markdown lines and render them into the PDF document.
 */
export function renderMarkdownToPdf(
  markdown: string,
  ctx: PdfLayoutContext,
  renderer: PdfTextRenderer,
): void {
  const { doc, margin, contentWidth, fontSize, lineHeight } = ctx;
  const { renderTextWithBold, printWrappedText } = renderer;
  const lines = markdown.split("\n");

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmedLine = line.trim();

    if (!trimmedLine) {
      ctx.advanceY(5);
      continue;
    }

    if (trimmedLine.startsWith("# ")) {
      ctx.checkPageBreak(lineHeight.title + 10);
      ctx.advanceY(10);
      doc.setFontSize(fontSize.title);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(26, 26, 46);
      const titleText = trimmedLine.replace(/^#\s+/, "");
      doc.text(titleText, margin, ctx.y);
      ctx.advanceY(lineHeight.title);

      doc.setDrawColor(74, 144, 164);
      doc.setLineWidth(2);
      doc.line(margin, ctx.y, ctx.pageWidth - margin, ctx.y);
      ctx.advanceY(15);
      doc.setTextColor(0, 0, 0);
      continue;
    }

    if (trimmedLine.startsWith("## ")) {
      ctx.checkPageBreak(lineHeight.heading + 15);
      ctx.advanceY(15);
      doc.setFontSize(fontSize.heading);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(26, 26, 46);
      const headerText = trimmedLine.replace(/^##\s+/, "");
      doc.text(headerText, margin, ctx.y);
      ctx.advanceY(lineHeight.heading + 5);
      doc.setTextColor(0, 0, 0);
      continue;
    }

    if (trimmedLine.startsWith("### ")) {
      ctx.checkPageBreak(lineHeight.heading + 10);
      ctx.advanceY(10);
      doc.setFontSize(fontSize.heading - 1);
      doc.setFont("helvetica", "bold");
      const subHeaderText = trimmedLine.replace(/^###\s+/, "");
      doc.text(subHeaderText, margin, ctx.y);
      ctx.advanceY(lineHeight.heading);
      continue;
    }

    if (trimmedLine.startsWith("• ") || trimmedLine.startsWith("- ")) {
      const bulletText = trimmedLine.replace(/^[•-]\s+/, "");
      ctx.checkPageBreak(lineHeight.body);

      doc.setFontSize(fontSize.body);
      doc.setFont("helvetica", "normal");
      doc.text("•", margin + 5, ctx.y);

      renderTextWithBold(
        bulletText,
        margin + 20,
        fontSize.body,
        contentWidth - 20,
      );
      continue;
    }

    if (trimmedLine.startsWith("■ ")) {
      const subBulletText = trimmedLine.replace(/^■\s+/, "");
      ctx.checkPageBreak(lineHeight.body);

      doc.setFontSize(fontSize.body - 2);
      doc.setFont("helvetica", "normal");
      doc.text("■", margin + 25, ctx.y);
      doc.setFontSize(fontSize.body);

      const wrappedText = doc.splitTextToSize(subBulletText, contentWidth - 40);
      for (let j = 0; j < wrappedText.length; j++) {
        if (j > 0) ctx.checkPageBreak(lineHeight.body);
        doc.text(wrappedText[j], margin + 40, ctx.y);
        ctx.advanceY(lineHeight.body);
      }
      continue;
    }

    if (trimmedLine.startsWith("Summary period:")) {
      ctx.checkPageBreak(lineHeight.body + 5);
      doc.setFontSize(fontSize.body);
      doc.setFont("helvetica", "italic");
      doc.text(trimmedLine, margin, ctx.y);
      ctx.advanceY(lineHeight.body + 5);
      doc.setFont("helvetica", "normal");
      continue;
    }

    const sectionMatch = trimmedLine.match(/^([A-Za-z\s]+)\s*\((\d+)\):?$/);
    if (sectionMatch) {
      ctx.checkPageBreak(lineHeight.heading + 10);
      ctx.advanceY(10);
      doc.setFontSize(fontSize.heading);
      doc.setFont("helvetica", "bold");
      doc.text(trimmedLine, margin, ctx.y);
      ctx.advanceY(lineHeight.heading);
      doc.setFont("helvetica", "normal");
      continue;
    }

    if (trimmedLine.startsWith("**")) {
      ctx.checkPageBreak(lineHeight.body);
      renderTextWithBold(trimmedLine, margin, fontSize.body, contentWidth);
      continue;
    }

    printWrappedText(trimmedLine, fontSize.body, "normal", 0);
  }
}
