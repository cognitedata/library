import type { ExportService } from '../ExportService';

/** The demo dataset has no run that wrote a workbook. */
export class MockExportService implements ExportService {
  async getUnlinkedWorkbookUrl(): Promise<string | null> {
    return null;
  }
}
