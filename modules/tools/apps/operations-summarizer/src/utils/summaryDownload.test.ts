import { afterEach, describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import type { SummaryInstance } from "@/types/operations";
import {
  downloadAsMarkdown,
  downloadOperationSummaryPdfWithSignedUrlFirst,
  downloadSummaryAsMarkdown,
  downloadSummaryAsPdf,
  extractSummaryContent,
  getSummaryDisplayDate,
  getSummaryPreview,
} from "./summaryDownload";

// withCdfConcurrency waits on a shared queue. Tests run the task immediately.
vi.mock("@/utils/cdfRequest", () => ({
  withCdfConcurrency: <T>(fn: () => Promise<T>): Promise<T> => fn(),
}));

const viewKey = "cdf_cdm/CogniteFile/v1";

function makeSummary(
  overrides: Partial<SummaryInstance> & {
    properties?: Record<string, Record<string, unknown>>;
  },
): SummaryInstance {
  return {
    externalId: "ops_summary_2024-01-01_2024-01-31_abc",
    space: "ops_summaries",
    createdTime: 1704067200000,
    properties: overrides.properties ?? {},
    ...overrides,
  };
}

describe("extractSummaryContent", () => {
  it("extracts name and content from file properties", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          name: "My Summary",
          content: "# Summary\n\nBody text",
          description: "Desc",
          mimeType: "text/markdown",
        },
      },
    });
    const result = extractSummaryContent(summary);
    expect(result.name).toBe("My Summary");
    expect(result.content).toBe("# Summary\n\nBody text");
    expect(result.description).toBe("Desc");
    expect(result.mimeType).toBe("text/markdown");
  });

  it("derives human-readable name from externalId when name missing", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: { content: "only content" },
      },
    });
    const result = extractSummaryContent(summary);
    expect(result.name).toMatch(/^Operations summary,/);
  });

  it("uses description as content when content empty and description looks like markdown", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          description: "# Title\n\nBody",
        },
      },
    });
    const result = extractSummaryContent(summary);
    expect(result.content).toBe("# Title\n\nBody");
    expect(result.description).toBe("");
  });

  it("derives test report display name from externalId pattern", () => {
    const summary = makeSummary({
      externalId: "test_report_2024-02-01_2024-02-28_xyz",
      properties: {
        [viewKey]: { content: "x" },
      },
    });
    const result = extractSummaryContent(summary);
    expect(result.name).toMatch(/^Test report,/);
  });

  it("strips .pdf extension from display name", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          name: "Report Name.pdf",
          content: "c",
        },
      },
    });
    expect(extractSummaryContent(summary).name).toBe("Report Name");
  });

  it("falls back to sourceContext and source for content", () => {
    const fromContext = makeSummary({
      properties: {
        [viewKey]: {
          name: "N",
          sourceContext: "from context",
        },
      },
    });
    expect(extractSummaryContent(fromContext).content).toBe("from context");

    const fromSource = makeSummary({
      properties: {
        [viewKey]: {
          name: "N2",
          source: "from source field",
        },
      },
    });
    expect(extractSummaryContent(fromSource).content).toBe("from source field");
  });

  it("promotes description with bold markers to content", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          description: "Intro **bold** rest",
        },
      },
    });
    const result = extractSummaryContent(summary);
    expect(result.content).toContain("**bold**");
  });
});

describe("getSummaryDisplayDate", () => {
  it("formats createdTime as locale date", () => {
    const summary = makeSummary({ createdTime: 1704067200000 });
    const display = getSummaryDisplayDate(summary);
    expect(display).toMatch(/\d{4}/);
    expect(display.length).toBeGreaterThan(0);
  });
});

describe("downloadAsMarkdown", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("creates object URL, clicks anchor, revokes URL", () => {
    const click = vi.fn();
    const link = { href: "", download: "", click };
    vi.spyOn(document, "createElement").mockReturnValue(link as never);
    vi.spyOn(document.body, "appendChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(document.body, "removeChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:unit-test");
    const revoke = vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => {});

    downloadAsMarkdown("# body", "report");

    expect(click).toHaveBeenCalled();
    expect(revoke).toHaveBeenCalledWith("blob:unit-test");
    expect(link.download).toMatch(/\.md$/);
  });

  it("does not duplicate .md extension when filename already ends with .md", () => {
    const click = vi.fn();
    const link = { href: "", download: "", click };
    vi.spyOn(document, "createElement").mockReturnValue(link as never);
    vi.spyOn(document.body, "appendChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(document.body, "removeChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:md");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

    downloadAsMarkdown("x", "already.md");
    expect(link.download).toBe("already.md");
  });
});

describe("downloadSummaryAsMarkdown", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("extracts content and delegates to downloadAsMarkdown", () => {
    const click = vi.fn();
    const link = { href: "", download: "", click };
    vi.spyOn(document, "createElement").mockReturnValue(link as never);
    vi.spyOn(document.body, "appendChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(document.body, "removeChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:x");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

    const summary = makeSummary({
      properties: {
        [viewKey]: {
          name: "ExportMe",
          content: "# Doc",
        },
      },
    });

    downloadSummaryAsMarkdown(summary);

    expect(click).toHaveBeenCalled();
  });
});

describe("getSummaryPreview", () => {
  it("truncates content and strips markdown", () => {
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          content:
            "# H1\n\nThis is a long body that should be truncated when we ask for a short preview.",
        },
      },
    });
    const preview = getSummaryPreview(summary, 20);
    expect(preview.length).toBeLessThanOrEqual(23);
    expect(preview).not.toMatch(/^#/);
  });
});

describe("downloadOperationSummaryPdfWithSignedUrlFirst", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function pdfSummary(
    overrides: Partial<SummaryInstance> = {},
  ): SummaryInstance {
    return makeSummary({
      properties: {
        [viewKey]: {
          name: "file.pdf",
          content: "",
          mimeType: "application/pdf",
        },
      },
      ...overrides,
    });
  }

  it("calls fallback when node is not a PDF file", async () => {
    const fallback = vi.fn().mockResolvedValue(undefined);
    const post = vi.fn();
    const client = cogniteClientMock({ project: "p", post });
    const summary = makeSummary({
      properties: {
        [viewKey]: {
          name: "notes.md",
          mimeType: "text/markdown",
          content: "# x",
        },
      },
    });

    await downloadOperationSummaryPdfWithSignedUrlFirst(client, summary, {
      fallback,
    });

    expect(fallback).toHaveBeenCalledWith(summary);
    expect(post).not.toHaveBeenCalled();
  });

  it("opens signed URL when getDownloadUrls returns downloadUrl", async () => {
    const fallback = vi.fn();
    const click = vi.fn();
    const link = { href: "", download: "", click };
    vi.spyOn(document, "createElement").mockReturnValue(link as never);
    vi.spyOn(document.body, "appendChild").mockImplementation(
      () => link as never,
    );
    vi.spyOn(document.body, "removeChild").mockImplementation(
      () => link as never,
    );

    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://signed.example/file" }]);
    const client = cogniteClientMock({
      project: "p",
      files: { getDownloadUrls },
    });

    await downloadOperationSummaryPdfWithSignedUrlFirst(client, pdfSummary(), {
      fallback,
    });

    expect(link.href).toBe("https://signed.example/file");
    expect(click).toHaveBeenCalled();
    expect(fallback).not.toHaveBeenCalled();
  });

  it("falls back when downloadUrl is missing", async () => {
    const fallback = vi.fn().mockResolvedValue(undefined);
    const getDownloadUrls = vi.fn().mockResolvedValue([{}]);
    const client = cogniteClientMock({
      project: "p",
      files: { getDownloadUrls },
    });

    await downloadOperationSummaryPdfWithSignedUrlFirst(client, pdfSummary(), {
      fallback,
    });

    expect(fallback).toHaveBeenCalled();
  });

  it("falls back when getDownloadUrls throws", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const fallback = vi.fn().mockResolvedValue(undefined);
    const getDownloadUrls = vi.fn().mockRejectedValue(new Error("cdf"));
    const client = cogniteClientMock({
      project: "p",
      files: { getDownloadUrls },
    });

    await downloadOperationSummaryPdfWithSignedUrlFirst(client, pdfSummary(), {
      fallback,
    });

    expect(fallback).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });
});

describe("downloadSummaryAsPdf", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("logs and returns early when print window cannot open", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(window, "open").mockReturnValue(null);

    const summary = makeSummary({
      properties: {
        [viewKey]: { name: "s", content: "# body" },
      },
    });

    downloadSummaryAsPdf(summary);

    expect(consoleSpy).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });
});
