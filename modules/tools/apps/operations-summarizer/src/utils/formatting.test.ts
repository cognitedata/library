import { describe, expect, it } from "vitest";
import {
  extractDataCounts,
  formatIsoDate,
  generateFilename,
  markdownToHtml,
  paginateItems,
  sanitizeHtml,
  stripHtmlTags,
} from "./formatting";

describe("formatting barrel", () => {
  it("re-exports core utilities", () => {
    expect(typeof sanitizeHtml).toBe("function");
    expect(typeof stripHtmlTags).toBe("function");
    expect(typeof markdownToHtml).toBe("function");
    expect(typeof formatIsoDate).toBe("function");
    expect(typeof extractDataCounts).toBe("function");
    expect(typeof paginateItems).toBe("function");
    expect(typeof generateFilename).toBe("function");
  });
});
