import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TestTable } from "@/components/tests/TestTable";
import type { SummaryInstance } from "@/types/operations";

const VIEW = "cdf_cdm/CogniteFile/v1";

const report: SummaryInstance = {
  externalId: "test_report_2024-01-01_2024-01-31_abc",
  space: "ops_summaries",
  createdTime: 1_700_000_000_000,
  properties: {
    [VIEW]: {
      name: "January Test",
      content: "Similarity Score: 80/100\nDate Range: 2024-01-01 to 2024-01-31",
    },
  },
};

const paging = {
  currentPage: 1,
  totalPages: 3,
  totalItems: 30,
  hasNextPage: true,
  hasPreviousPage: false,
  onNextPage: vi.fn(),
  onPreviousPage: vi.fn(),
};

describe("TestTable", () => {
  it("shows loading skeleton when isLoading", () => {
    render(<TestTable testReports={[report]} {...paging} isLoading />);
    expect(
      screen.getByRole("status", { name: /loading test reports/i }),
    ).toBeInTheDocument();
    expect(screen.queryByText("January Test")).not.toBeInTheDocument();
  });

  it("renders report rows and mobile cards", () => {
    render(<TestTable testReports={[report]} {...paging} />);
    expect(screen.getAllByText("January Test").length).toBeGreaterThan(0);
    expect(screen.getByText("Report")).toBeInTheDocument();
  });

  it("wires view/download/delete callbacks from the table row", () => {
    const onView = vi.fn();
    const onDownload = vi.fn();
    const onDelete = vi.fn();
    render(
      <TestTable
        testReports={[report]}
        {...paging}
        onView={onView}
        onDownload={onDownload}
        onDelete={onDelete}
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /\bview\b/i })[0]);
    fireEvent.click(
      screen.getAllByRole("button", { name: /download test report/i })[0],
    );
    fireEvent.click(
      screen.getAllByRole("button", { name: /delete test report/i })[0],
    );
    expect(onView).toHaveBeenCalledWith(report);
    expect(onDownload).toHaveBeenCalledWith(report);
    expect(onDelete).toHaveBeenCalledWith(report);
  });

  it("calls pagination handlers", () => {
    const onNextPage = vi.fn();
    const onPreviousPage = vi.fn();
    render(
      <TestTable
        testReports={[report]}
        {...paging}
        onNextPage={onNextPage}
        onPreviousPage={onPreviousPage}
        hasPreviousPage
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /next/i })[0]);
    fireEvent.click(screen.getAllByRole("button", { name: /previous/i })[0]);
    expect(onNextPage).toHaveBeenCalled();
    expect(onPreviousPage).toHaveBeenCalled();
  });
});
