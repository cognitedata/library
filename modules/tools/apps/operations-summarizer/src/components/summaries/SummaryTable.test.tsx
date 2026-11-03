import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SummaryTable } from "@/components/summaries/SummaryTable";
import type { SummaryInstance } from "@/types/operations";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({
    project: "p",
    get: vi.fn(),
    post: vi.fn(),
    instances: { list: vi.fn() },
  }),
}));

const VIEW = "cdf_cdm/CogniteFile/v1";

function makeSummary(id: string, name: string): SummaryInstance {
  return {
    externalId: id,
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: { [VIEW]: { name, content: "content" } },
  };
}

const PAGINATION_DEFAULTS = {
  currentPage: 1,
  totalPages: 1,
  totalItems: 1,
  hasNextPage: false,
  hasPreviousPage: false,
  onNextPage: vi.fn(),
  onPreviousPage: vi.fn(),
};

describe("SummaryTable", () => {
  // ── Rendering ─────────────────────────────────────────────────────────────

  it("renders column headers", () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
      />,
    );
    expect(screen.getByText("Name")).toBeInTheDocument();
    expect(screen.getByText("Period")).toBeInTheDocument();
    expect(screen.getByText("Created")).toBeInTheDocument();
    expect(screen.getByText("Actions")).toBeInTheDocument();
  });

  it("renders summary names for each row", () => {
    const summaries = [
      makeSummary("a", "Alpha Report"),
      makeSummary("b", "Beta Report"),
    ];
    render(
      <SummaryTable
        summaries={summaries}
        {...PAGINATION_DEFAULTS}
        totalItems={2}
      />,
    );
    expect(screen.getAllByText("Alpha Report").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Beta Report").length).toBeGreaterThan(0);
  });

  it("renders no rows when summaries list is empty", () => {
    render(
      <SummaryTable summaries={[]} {...PAGINATION_DEFAULTS} totalItems={0} />,
    );
    expect(screen.queryByText(/Alpha Report/)).not.toBeInTheDocument();
  });

  it("renders a loading skeleton and hides content when isLoading is true", () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Should Not Appear")]}
        {...PAGINATION_DEFAULTS}
        isLoading
      />,
    );
    expect(screen.queryByText("Should Not Appear")).not.toBeInTheDocument();
  });

  // ── Conditional button visibility ─────────────────────────────────────────

  it("shows View, Test, and Delete buttons when all callbacks are provided", () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        onView={vi.fn()}
        onTest={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(
      screen.getAllByRole("button", { name: /\bview\b/i }).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("button", { name: /\btest\b/i }).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("button", { name: /delete summary/i }).length,
    ).toBeGreaterThan(0);
  });

  it("hides all action buttons when no callbacks are provided", () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
      />,
    );
    expect(
      screen.queryByRole("button", { name: /\bview\b/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /\btest\b/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /delete summary/i }),
    ).not.toBeInTheDocument();
  });

  // ── Callback invocation ───────────────────────────────────────────────────

  it("calls onView with the correct summary when View is clicked in the table row", () => {
    const summary = makeSummary("a", "Report A");
    const onView = vi.fn();
    render(
      <SummaryTable
        summaries={[summary]}
        {...PAGINATION_DEFAULTS}
        onView={onView}
      />,
    );
    // The table row's View button (desktop) is the first match
    fireEvent.click(screen.getAllByRole("button", { name: /\bview\b/i })[0]);
    expect(onView).toHaveBeenCalledWith(summary);
  });

  it("calls onTest with the correct summary when Test is clicked in the table row", () => {
    const summary = makeSummary("a", "Report A");
    const onTest = vi.fn();
    render(
      <SummaryTable
        summaries={[summary]}
        {...PAGINATION_DEFAULTS}
        onTest={onTest}
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /\btest\b/i })[0]);
    expect(onTest).toHaveBeenCalledWith(summary);
  });

  // ── Delete confirmation flow ───────────────────────────────────────────────

  it("opens the confirmation dialog when a Delete button is clicked", async () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        onDelete={vi.fn()}
      />,
    );
    fireEvent.click(
      screen.getAllByRole("button", { name: /delete summary/i })[0],
    );
    await waitFor(() =>
      expect(
        screen.getByText(/are you sure you want to delete this summary/i),
      ).toBeInTheDocument(),
    );
  });

  it("calls onDelete with the correct summary when deletion is confirmed", async () => {
    const summary = makeSummary("a", "Report A");
    const onDelete = vi.fn().mockResolvedValue(undefined);
    render(
      <SummaryTable
        summaries={[summary]}
        {...PAGINATION_DEFAULTS}
        onDelete={onDelete}
      />,
    );

    fireEvent.click(
      screen.getAllByRole("button", { name: /delete summary/i })[0],
    );
    const confirmBtn = await screen.findByRole("button", { name: /^delete$/i });
    fireEvent.click(confirmBtn);

    await waitFor(() => expect(onDelete).toHaveBeenCalledWith(summary));
  });

  it("does not call onDelete when Cancel is clicked in the dialog", async () => {
    const onDelete = vi.fn();
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        onDelete={onDelete}
      />,
    );

    fireEvent.click(
      screen.getAllByRole("button", { name: /delete summary/i })[0],
    );
    const cancelBtn = await screen.findByRole("button", { name: /cancel/i });
    fireEvent.click(cancelBtn);

    expect(onDelete).not.toHaveBeenCalled();
  });

  it("disables Cancel in the dialog when isDeletePending is true", async () => {
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        onDelete={vi.fn()}
        isDeletePending
      />,
    );

    fireEvent.click(
      screen.getAllByRole("button", { name: /delete summary/i })[0],
    );
    const cancelBtn = await screen.findByRole("button", { name: /cancel/i });
    expect(cancelBtn).toBeDisabled();
  });

  // ── Pagination ─────────────────────────────────────────────────────────────

  it("calls onNextPage when the Next button is clicked", () => {
    const onNextPage = vi.fn();
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        hasNextPage
        totalPages={2}
        onNextPage={onNextPage}
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /next/i })[0]);
    expect(onNextPage).toHaveBeenCalled();
  });

  it("calls onPreviousPage when the Previous button is clicked", () => {
    const onPreviousPage = vi.fn();
    render(
      <SummaryTable
        summaries={[makeSummary("a", "Report A")]}
        {...PAGINATION_DEFAULTS}
        hasPreviousPage
        currentPage={2}
        totalPages={2}
        onPreviousPage={onPreviousPage}
      />,
    );
    fireEvent.click(screen.getAllByRole("button", { name: /previous/i })[0]);
    expect(onPreviousPage).toHaveBeenCalled();
  });
});
