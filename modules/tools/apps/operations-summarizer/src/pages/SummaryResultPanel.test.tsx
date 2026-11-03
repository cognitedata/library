import { fireEvent, render, screen } from "@testing-library/react";
import type { ComponentProps } from "react";
import { describe, expect, it, vi } from "vitest";
import type { DataTypeName } from "@/types/operations";
import { SummaryResultPanel } from "./SummaryResultPanel";

const defaultProps: ComponentProps<typeof SummaryResultPanel> = {
  generatedSummary: {
    fullText: "# Ops\n\nBody content",
    generatedAt: new Date("2024-01-15T10:00:00Z"),
  },
  periodStart: new Date("2024-01-01T00:00:00Z"),
  periodEnd: new Date("2024-01-31T00:00:00Z"),
  summaryContents: {
    assets: true,
    files: true,
    work_orders: false,
    time_series: false,
    equipment: false,
  },
  savedPdfInfo: null,
  isSaving: false,
  onSave: vi.fn().mockResolvedValue(undefined),
  onDownload: vi.fn(),
  saveError: null,
};

describe("SummaryResultPanel", () => {
  it("renders metadata and markdown content", () => {
    render(<SummaryResultPanel {...defaultProps} />);
    expect(screen.getByText("Operations Summary")).toBeInTheDocument();
    expect(screen.getByText(/Generated:/)).toBeInTheDocument();
    expect(screen.getByText(/Assets/)).toBeInTheDocument();
    expect(screen.getByText(/Files/)).toBeInTheDocument();
  });

  it("calls download and save handlers", () => {
    const onDownload = vi.fn();
    const onSave = vi.fn().mockResolvedValue(undefined);
    render(
      <SummaryResultPanel
        {...defaultProps}
        onDownload={onDownload}
        onSave={onSave}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /download md/i }));
    fireEvent.click(screen.getByRole("button", { name: /save to cognite/i }));
    expect(onDownload).toHaveBeenCalled();
    expect(onSave).toHaveBeenCalled();
  });

  it("disables save and shows Saved badge when already saved", () => {
    render(
      <SummaryResultPanel
        {...defaultProps}
        savedPdfInfo={{ fileName: "ops.pdf", fileId: "f1" }}
      />,
    );
    expect(screen.getByText("Saved")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /saved to cognite/i }),
    ).toBeDisabled();
  });

  it("renders Open in Fusion as a link button when a Fusion URL is present", () => {
    render(
      <SummaryResultPanel
        {...defaultProps}
        savedPdfInfo={{
          fileName: "ops.pdf",
          fileId: "f1",
          fusionUrl: "https://fusion.example.test/file/ops.pdf",
        }}
      />,
    );
    const link = screen.getByRole("link", { name: /open in fusion/i });
    expect(link).toHaveAttribute(
      "href",
      "https://fusion.example.test/file/ops.pdf",
    );
    expect(link).toHaveAttribute("target", "_blank");
  });

  it("shows save error and truncation warning", () => {
    render(
      <SummaryResultPanel
        {...defaultProps}
        saveError="Upload failed"
        generatedSummary={{
          ...defaultProps.generatedSummary,
          dataTypeTruncation: {
            assets: { limit: 100, hadMoreInCdf: true },
          } as Partial<
            Record<DataTypeName, { limit: number; hadMoreInCdf: boolean }>
          >,
        }}
      />,
    );
    expect(screen.getByText("Upload failed")).toBeInTheDocument();
    expect(
      screen.getByText(/Incomplete data for some types/i),
    ).toBeInTheDocument();
  });
});
