import { describe, expect, it } from "vitest";
import { generatePdfFromMarkdown } from "./pdfGenerator";

describe("generatePdfFromMarkdown", () => {
  it("returns a non-empty PDF blob for minimal markdown", () => {
    const blob = generatePdfFromMarkdown("# Title\n\nHello world.");
    expect(blob).toBeInstanceOf(Blob);
    expect(blob.size).toBeGreaterThan(0);
  });

  it("merges custom options with defaults", () => {
    const blob = generatePdfFromMarkdown("## Section\n\nBody.", {
      title: "Custom",
      margin: 20,
      fontSize: { title: 14, heading: 11, body: 9 },
    });
    expect(blob.size).toBeGreaterThan(0);
  });

  it("renders heading levels, bullets, sub-bullets, and summary period", () => {
    const md = [
      "# Main Title",
      "## Section A",
      "### Sub",
      "Summary period: 2024-01-01 to 2024-01-31",
      "- bullet one",
      "• bullet two",
      "■ sub bullet",
      "**Bold line** and more",
      "Assets (12):",
      "Regular paragraph with **inline** bold.",
    ].join("\n");
    const blob = generatePdfFromMarkdown(md);
    expect(blob.size).toBeGreaterThan(500);
  });

  it("handles long content across page breaks", () => {
    const lines = Array.from(
      { length: 120 },
      (_, i) => `Line ${i} ${"x".repeat(80)}`,
    );
    const blob = generatePdfFromMarkdown(`# T\n\n${lines.join("\n")}`);
    expect(blob.size).toBeGreaterThan(2000);
  });

  it("handles bullet with leading bold marker", () => {
    const blob = generatePdfFromMarkdown("- **Label:** rest of text here");
    expect(blob.size).toBeGreaterThan(0);
  });

  it("skips blank lines while preserving spacing", () => {
    const blob = generatePdfFromMarkdown(
      "# Title\n\n\nParagraph after blanks.",
    );
    expect(blob.size).toBeGreaterThan(0);
  });

  it("renders multi-line sub-bullets and section labels", () => {
    const md = [
      "Work Orders (3):",
      "■ first sub item that is reasonably long to wrap in the PDF layout",
      "■ second",
      "**Completed Work Orders:** None reported",
    ].join("\n");
    const blob = generatePdfFromMarkdown(md);
    expect(blob.size).toBeGreaterThan(0);
  });

  it("wraps mixed bold/normal tokens that exceed line width", () => {
    const long = `Intro **${"BoldWord".repeat(20)}** trailing ${"word ".repeat(40)}`;
    const blob = generatePdfFromMarkdown(long);
    expect(blob.size).toBeGreaterThan(0);
  });

  it("handles empty markdown input", () => {
    const blob = generatePdfFromMarkdown("");
    expect(blob).toBeInstanceOf(Blob);
    expect(blob.size).toBeGreaterThan(0);
  });
});
