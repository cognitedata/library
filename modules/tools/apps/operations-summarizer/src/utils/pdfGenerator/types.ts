export interface PdfGeneratorOptions {
  title?: string;
  margin?: number;
  fontSize?: {
    title: number;
    heading: number;
    body: number;
  };
}

export const DEFAULT_PDF_OPTIONS: Required<PdfGeneratorOptions> = {
  title: "Operation Summary",
  margin: 40,
  fontSize: {
    title: 16,
    heading: 12,
    body: 10,
  },
};

export interface PdfLayoutContext {
  doc: import("jspdf").jsPDF;
  margin: number;
  contentWidth: number;
  pageWidth: number;
  pageHeight: number;
  fontSize: Required<NonNullable<PdfGeneratorOptions["fontSize"]>>;
  lineHeight: {
    title: number;
    heading: number;
    body: number;
  };
  y: number;
  checkPageBreak: (neededHeight: number) => boolean;
  setY: (nextY: number) => void;
  advanceY: (delta: number) => void;
}
