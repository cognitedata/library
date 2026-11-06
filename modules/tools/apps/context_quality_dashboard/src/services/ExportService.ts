/** The workbook of every unlinked item that a run wrote to Files, as a link the browser can open. */
export interface ExportService {
  /** A short-lived download link for the latest run's workbook, or null when that run wrote none. */
  getUnlinkedWorkbookUrl(): Promise<string | null>;
}
