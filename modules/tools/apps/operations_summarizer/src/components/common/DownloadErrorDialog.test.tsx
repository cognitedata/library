import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { DownloadErrorDialog } from "./DownloadErrorDialog";

const summary: SummaryInstance = {
  externalId: "ops_summary_1",
  space: "ops_summaries",
  createdTime: 1,
  properties: {},
};

describe("DownloadErrorDialog", () => {
  it("retries the pdf download, falls back to markdown, and closes", async () => {
    const onRetry = vi.fn().mockResolvedValue(undefined);
    const onDownloadMarkdown = vi.fn().mockResolvedValue(undefined);
    const onClose = vi.fn();
    const onOpenChange = vi.fn();
    render(
      <DownloadErrorDialog
        open
        onOpenChange={onOpenChange}
        error="PDF render failed"
        summary={summary}
        onRetry={onRetry}
        onDownloadMarkdown={onDownloadMarkdown}
        onClose={onClose}
      />,
    );

    expect(screen.getByText(/ops_summary_1/)).toBeInTheDocument();
    fireEvent.click(
      screen.getByRole("button", { name: /try pdf download again/i }),
    );
    await vi.waitFor(() => expect(onRetry).toHaveBeenCalled());

    fireEvent.click(
      screen.getByRole("button", { name: /download as markdown/i }),
    );
    await vi.waitFor(() => expect(onDownloadMarkdown).toHaveBeenCalled());

    fireEvent.click(
      screen.getByRole("button", { name: /cancel and continue browsing/i }),
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it("stays open when a download action throws and hides retry", async () => {
    const onRetry = vi.fn().mockRejectedValue(new Error("nope"));
    const onDownloadMarkdown = vi.fn().mockRejectedValue(new Error("nope"));
    const onClose = vi.fn();
    render(
      <DownloadErrorDialog
        open
        onOpenChange={vi.fn()}
        error="PDF render failed"
        summary={null}
        onRetry={onRetry}
        onDownloadMarkdown={onDownloadMarkdown}
        onClose={onClose}
        canRetry={false}
        isRetrying
      />,
    );
    expect(
      screen.queryByRole("button", { name: /try pdf/i }),
    ).not.toBeInTheDocument();
    fireEvent.click(
      screen.getByRole("button", { name: /download as markdown/i }),
    );
    await vi.waitFor(() => expect(onDownloadMarkdown).toHaveBeenCalled());
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.queryByText(/Failed to download:/)).not.toBeInTheDocument();
  });
});
