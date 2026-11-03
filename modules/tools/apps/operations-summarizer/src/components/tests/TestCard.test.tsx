import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TestCard } from "@/components/tests/TestCard";
import type { SummaryInstance } from "@/types/operations";

// extractSummaryContent falls back to externalId when name is blank.
vi.mock("@/utils/summaryDownload", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@/utils/summaryDownload")>();
  return {
    ...actual,
    extractSummaryContent: (summary: SummaryInstance) =>
      summary.externalId === "no_name"
        ? { name: "", content: "", description: "", mimeType: "" }
        : actual.extractSummaryContent(summary),
  };
});

const VIEW = "cdf_cdm/CogniteFile/v1";

function makeReport(overrides?: Partial<SummaryInstance>): SummaryInstance {
  return {
    externalId: "test_report_2024-01-01_2024-01-31_abc",
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: {
      [VIEW]: {
        name: "January Test",
        content: [
          "Similarity Score: 88/100",
          "Date Range: 2024-01-01 to 2024-01-31",
          "Assets: 12",
        ].join("\n"),
      },
    },
    ...overrides,
  };
}

describe("TestCard", () => {
  it("renders name, period, score, and tested date", () => {
    render(<TestCard testReport={makeReport()} />);
    expect(screen.getByText("January Test")).toBeInTheDocument();
    expect(screen.getByText(/Period:/)).toBeInTheDocument();
    expect(screen.getByText(/88\.0%/)).toBeInTheDocument();
    expect(screen.getByText(/Tested:/)).toBeInTheDocument();
  });

  it('falls back to "Untitled Test" when name is missing', () => {
    render(
      <TestCard
        testReport={{
          externalId: "no_name",
          space: "ops_summaries",
          createdTime: 1_700_000_000_000,
          properties: {},
        }}
      />,
    );
    expect(screen.getByText("Untitled Test")).toBeInTheDocument();
  });

  it("hides action buttons when callbacks are omitted", () => {
    render(<TestCard testReport={makeReport()} />);
    expect(
      screen.queryByRole("button", { name: /\bview\b/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /download test report/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /delete test report/i }),
    ).not.toBeInTheDocument();
  });

  it("invokes action callbacks with the report", () => {
    const report = makeReport();
    const onView = vi.fn();
    const onDownload = vi.fn();
    const onDelete = vi.fn();
    render(
      <TestCard
        testReport={report}
        onView={onView}
        onDownload={onDownload}
        onDelete={onDelete}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /\bview\b/i }));
    fireEvent.click(
      screen.getByRole("button", { name: /download test report/i }),
    );
    fireEvent.click(
      screen.getByRole("button", { name: /delete test report/i }),
    );
    expect(onView).toHaveBeenCalledWith(report);
    expect(onDownload).toHaveBeenCalledWith(report);
    expect(onDelete).toHaveBeenCalledWith(report);
  });
});
