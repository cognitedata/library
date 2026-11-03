import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { useRobustDownload } from "./useRobustDownload";

// Download helpers touch the DOM and the CDF files API. This test isolates the caller from that path.
vi.mock("@/utils/summaryDownload", () => ({
  extractSummaryContent: vi.fn(() => ({
    content: "# Summary\n\nBody",
    name: "n",
    description: "",
    mimeType: "text/markdown",
  })),
  downloadSummaryAsMarkdown: vi.fn(),
  downloadSummaryAsPdf: vi.fn(),
}));

const summary: SummaryInstance = {
  externalId: "s1",
  space: "ops_summaries",
  createdTime: 1,
  properties: {},
};

describe("useRobustDownload", () => {
  beforeEach(async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {});
    vi.mocked(sd.downloadSummaryAsMarkdown).mockImplementation(() => {});
    vi.mocked(sd.extractSummaryContent).mockReturnValue({
      content: "# Summary\n\nBody",
      name: "n",
      description: "",
      mimeType: "text/markdown",
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("downloadMarkdown calls markdown path", async () => {
    const { downloadSummaryAsMarkdown } =
      await import("@/utils/summaryDownload");
    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      await result.current.downloadMarkdown(summary);
    });

    expect(downloadSummaryAsMarkdown).toHaveBeenCalledWith(summary);
  });

  it("downloadPdf completes after delay", async () => {
    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      const p = result.current.downloadPdf(summary);
      vi.advanceTimersByTime(1100);
      await p;
    });

    expect(result.current.isDownloading).toBe(false);
  });

  it("falls back to markdown when PDF download throws", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {
      throw new Error("pdf failed");
    });
    vi.mocked(sd.downloadSummaryAsMarkdown).mockImplementation(() => {});

    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      await result.current.downloadPdf(summary);
    });

    expect(sd.downloadSummaryAsMarkdown).toHaveBeenCalledWith(summary);
    expect(result.current.downloadError).toContain("markdown version");
  });

  it("combines errors when PDF and markdown fallback both fail", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {
      throw new Error("pdf failed");
    });
    vi.mocked(sd.downloadSummaryAsMarkdown).mockImplementation(() => {
      throw new Error("md failed");
    });

    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      await result.current.downloadPdf(summary);
    });

    expect(result.current.downloadError).toContain("fallback also failed");
  });

  it("rethrows when fallbackToMarkdown is disabled", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {
      throw new Error("pdf failed");
    });

    const { result } = renderHook(() =>
      useRobustDownload({ fallbackToMarkdown: false }),
    );

    await expect(
      act(async () => {
        await result.current.downloadPdf(summary);
      }),
    ).rejects.toThrow("pdf failed");
  });

  it("does not persist error state when preserveState is false", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {
      throw new Error("pdf failed");
    });
    vi.mocked(sd.downloadSummaryAsMarkdown).mockImplementation(() => {});

    const { result } = renderHook(() =>
      useRobustDownload({ preserveState: false }),
    );

    await act(async () => {
      await result.current.downloadPdf(summary);
    });

    expect(result.current.downloadError).toBeNull();
  });

  it("rejects empty summary content for PDF path", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.extractSummaryContent).mockReturnValueOnce({
      content: "  ",
      name: "n",
      description: "",
      mimeType: "text/markdown",
    });

    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      await result.current.downloadPdf(summary);
    });

    expect(result.current.downloadError).toBeTruthy();
  });

  it("retryLastDownload throws when nothing failed before", async () => {
    const { result } = renderHook(() => useRobustDownload());

    await expect(
      act(async () => {
        await result.current.retryLastDownload();
      }),
    ).rejects.toThrow("No previous download");
  });

  it("sets canRetry after PDF failure with successful markdown fallback", async () => {
    const sd = await import("@/utils/summaryDownload");
    vi.mocked(sd.downloadSummaryAsPdf).mockImplementation(() => {
      throw new Error("pdf failed");
    });
    vi.mocked(sd.downloadSummaryAsMarkdown).mockImplementation(() => {});

    const { result } = renderHook(() => useRobustDownload());

    await act(async () => {
      await result.current.downloadPdf(summary);
    });

    expect(result.current.canRetry).toBe(true);
    expect(result.current.lastFailedSummary?.externalId).toBe(
      summary.externalId,
    );
  });
});
