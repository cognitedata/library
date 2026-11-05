import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { SummaryDetailModal } from "./SummaryDetailModal";

const pdfState = vi.hoisted(() => ({
  pdfFile: null as Blob | null,
  isLoadingPdf: false,
  pdfError: null as string | null,
  retryPdf: vi.fn(),
}));

const fileState = vi.hoisted(() => ({
  data: "## Summary body" as string | undefined,
  isLoading: false,
  isError: false,
  refetch: vi.fn(),
}));

// File hooks fetch CDF bytes. The modal test supplies hook results directly.
vi.mock("@/hooks/usePdfBlob", () => ({
  usePdfBlob: () => pdfState,
}));

// File hooks fetch CDF bytes. The modal test supplies hook results directly.
vi.mock("@/hooks/useFileContent", () => ({
  useFileContent: () => fileState,
}));

// PodcastTab mounts an audio player. The modal test renders a stand-in.
vi.mock("@/components/podcast/PodcastTab", () => ({
  PodcastTab: ({ summary }: { summary: SummaryInstance }) => (
    <div data-testid="podcast-tab">{summary.externalId}</div>
  ),
}));

// react-pdf loads a PDF.js worker and canvas that jsdom does not provide.
vi.mock("react-pdf", () => ({
  pdfjs: { GlobalWorkerOptions: { workerSrc: "" } },
  Document: ({
    children,
    onLoadSuccess,
  }: {
    children?: React.ReactNode;
    onLoadSuccess?: (info: { numPages: number }) => void;
  }) => {
    onLoadSuccess?.({ numPages: 3 });
    return <div data-testid="mock-pdf-doc">{children}</div>;
  },
  Page: () => <div data-testid="mock-pdf-page" />,
}));

const summary: SummaryInstance = {
  externalId: "ops_summary_2026-01-01_2026-01-07_abc",
  space: "ops_summaries",
  createdTime: Date.parse("2026-01-08T12:00:00Z"),
  properties: {
    "cdf_cdm/CogniteFile/v1": {
      name: "Weekly Ops Summary",
      mimeType: "text/markdown",
      content: "## Summary body",
      description: "## Summary body",
    },
  },
};

const pdfSummary: SummaryInstance = {
  ...summary,
  properties: {
    "cdf_cdm/CogniteFile/v1": {
      name: "report.pdf",
      mimeType: "application/pdf",
    },
  },
};

describe(SummaryDetailModal.name, () => {
  beforeEach(() => {
    pdfState.pdfFile = null;
    pdfState.isLoadingPdf = false;
    pdfState.pdfError = null;
    pdfState.retryPdf.mockClear();
    fileState.data = "## Summary body";
    fileState.isLoading = false;
    fileState.isError = false;
    fileState.refetch.mockClear();
  });

  it("still renders download buttons and summary content with podcast tab", () => {
    const onDownloadMd = vi.fn();
    const onDownloadPdf = vi.fn();

    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={summary}
        onDownloadMd={onDownloadMd}
        onDownloadPdf={onDownloadPdf}
      />,
    );

    expect(screen.getAllByText("Weekly Ops Summary").length).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("button", { name: /Markdown/i }).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("button", { name: /PDF/i }).length,
    ).toBeGreaterThan(0);
    expect(screen.getByRole("tab", { name: /Summary/i })).toBeTruthy();
    expect(screen.getByRole("tab", { name: /Podcast/i })).toBeTruthy();
    expect(screen.queryByTestId("podcast-tab")).toBeNull();
  });

  it("shows the podcast panel when the Podcast tab is selected", () => {
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={summary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("tab", { name: /Podcast/i }));

    expect(screen.getByTestId("podcast-tab").textContent).toBe(
      summary.externalId,
    );
    expect(
      screen
        .getByRole("tab", { name: /Podcast/i })
        .getAttribute("aria-selected"),
    ).toBe("true");
  });

  it("calls download handlers from the action buttons", () => {
    const onDownloadMd = vi.fn();
    const onDownloadPdf = vi.fn();
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={summary}
        onDownloadMd={onDownloadMd}
        onDownloadPdf={onDownloadPdf}
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /Markdown/i })[0]);
    fireEvent.click(screen.getAllByRole("button", { name: /PDF/i })[0]);
    expect(onDownloadMd).toHaveBeenCalledWith(summary);
    expect(onDownloadPdf).toHaveBeenCalledWith(summary);
  });

  it("shows loading state while content is fetching", () => {
    fileState.isLoading = true;
    fileState.data = undefined;
    const remoteSummary: SummaryInstance = {
      ...summary,
      properties: {
        "cdf_cdm/CogniteFile/v1": {
          name: "Weekly Ops Summary",
          mimeType: "text/markdown",
        },
      },
    };
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={remoteSummary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    expect(screen.getByText(/Loading content/i)).toBeInTheDocument();
  });

  it("shows PDF error with retry", () => {
    pdfState.pdfError = "PDF failed";
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={pdfSummary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    expect(screen.getByText("PDF failed")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    expect(pdfState.retryPdf).toHaveBeenCalled();
  });

  it("shows PDF viewer controls when a PDF URL is available", () => {
    pdfState.pdfFile = new Blob(["%PDF"], { type: "application/pdf" });
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={pdfSummary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    expect(screen.getByTestId("mock-pdf-doc")).toBeInTheDocument();
    expect(screen.getByLabelText(/previous pdf page/i)).toBeDisabled();
    fireEvent.click(screen.getByLabelText(/zoom in pdf/i));
    expect(screen.getByText("110%")).toBeInTheDocument();
  });

  it("shows empty content fallback when no markdown is available", () => {
    const empty: SummaryInstance = {
      externalId: "empty",
      space: "ops_summaries",
      createdTime: 1,
      properties: {
        "cdf_cdm/CogniteFile/v1": {
          name: "Empty",
          mimeType: "text/markdown",
          content: "",
        },
      },
    };
    fileState.data = undefined;
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={empty}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    expect(screen.getByText(/No content available/i)).toBeInTheDocument();
  });

  it("retries a failed remote text load", () => {
    fileState.isError = true;
    fileState.data = undefined;
    const remoteSummary: SummaryInstance = {
      ...summary,
      properties: {
        "cdf_cdm/CogniteFile/v1": {
          name: "Weekly Ops Summary",
          mimeType: "text/markdown",
        },
      },
    };
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={remoteSummary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    expect(fileState.refetch).toHaveBeenCalled();
  });

  it("moves between PDF pages and zooms out", () => {
    pdfState.pdfFile = new Blob(["%PDF"], { type: "application/pdf" });
    render(
      <SummaryDetailModal
        isOpen
        onClose={vi.fn()}
        selectedSummary={pdfSummary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByLabelText(/next pdf page/i));
    expect(screen.getByText(/Page 2 of 3/)).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText(/previous pdf page/i));
    expect(screen.getByText(/Page 1 of 3/)).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText(/zoom out pdf/i));
    expect(screen.getByText("90%")).toBeInTheDocument();
  });

  it("closes from the dialog close button", () => {
    const onClose = vi.fn();
    render(
      <SummaryDetailModal
        isOpen
        onClose={onClose}
        selectedSummary={summary}
        onDownloadMd={vi.fn()}
        onDownloadPdf={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /close summary/i }));
    expect(onClose).toHaveBeenCalled();
  });
});
