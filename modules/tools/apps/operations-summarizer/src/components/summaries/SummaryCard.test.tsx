import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SummaryCard } from "@/components/summaries/SummaryCard";
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

// extractSummaryContent falls back to externalId when the name property is blank,
// so reaching the "Untitled Summary" branch requires a mock at the utility level.
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

function makeSummary(nameOverride?: string): SummaryInstance {
  return {
    externalId: "ops_summary_test",
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: {
      [VIEW]: {
        name: nameOverride ?? "Daily Operations Report",
        content: "Summary content here",
      },
    },
  };
}

describe("SummaryCard", () => {
  // ── Rendering ────────────────────────────────────────────────────────────

  it("renders the summary name", () => {
    render(<SummaryCard summary={makeSummary()} />);
    expect(screen.getByText("Daily Operations Report")).toBeInTheDocument();
  });

  it('falls back to "Untitled Summary" when extractSummaryContent returns an empty name', () => {
    const summary: SummaryInstance = {
      externalId: "no_name",
      space: "ops_summaries",
      createdTime: 1_700_000_000_000,
      properties: {},
    };
    render(<SummaryCard summary={summary} />);
    expect(screen.getByText("Untitled Summary")).toBeInTheDocument();
  });

  it("renders Period and Created metadata labels", () => {
    render(<SummaryCard summary={makeSummary()} />);
    expect(screen.getByText(/^Period:/)).toBeInTheDocument();
    expect(screen.getByText(/^Created:/)).toBeInTheDocument();
  });

  // ── Conditional button visibility ─────────────────────────────────────────

  it("hides View button when onView is not provided", () => {
    render(<SummaryCard summary={makeSummary()} />);
    expect(
      screen.queryByRole("button", { name: /\bview\b/i }),
    ).not.toBeInTheDocument();
  });

  it("shows View button when onView is provided", () => {
    render(<SummaryCard summary={makeSummary()} onView={vi.fn()} />);
    expect(
      screen.getByRole("button", { name: /\bview\b/i }),
    ).toBeInTheDocument();
  });

  it("hides Test button when onTest is not provided", () => {
    render(<SummaryCard summary={makeSummary()} />);
    expect(
      screen.queryByRole("button", { name: /\btest\b/i }),
    ).not.toBeInTheDocument();
  });

  it("shows Test button when onTest is provided", () => {
    render(<SummaryCard summary={makeSummary()} onTest={vi.fn()} />);
    expect(
      screen.getByRole("button", { name: /\btest\b/i }),
    ).toBeInTheDocument();
  });

  it("hides Delete button when onDelete is not provided", () => {
    render(<SummaryCard summary={makeSummary()} />);
    expect(
      screen.queryByRole("button", { name: /delete summary/i }),
    ).not.toBeInTheDocument();
  });

  it("shows Delete button when onDelete is provided", () => {
    render(<SummaryCard summary={makeSummary()} onDelete={vi.fn()} />);
    expect(
      screen.getByRole("button", { name: /delete summary/i }),
    ).toBeInTheDocument();
  });

  // ── Callback invocation ───────────────────────────────────────────────────

  it("calls onView with the summary instance when View is clicked", () => {
    const summary = makeSummary();
    const onView = vi.fn();
    render(<SummaryCard summary={summary} onView={onView} />);
    fireEvent.click(screen.getByRole("button", { name: /\bview\b/i }));
    expect(onView).toHaveBeenCalledOnce();
    expect(onView).toHaveBeenCalledWith(summary);
  });

  it("calls onTest with the summary instance when Test is clicked", () => {
    const summary = makeSummary();
    const onTest = vi.fn();
    render(<SummaryCard summary={summary} onTest={onTest} />);
    fireEvent.click(screen.getByRole("button", { name: /\btest\b/i }));
    expect(onTest).toHaveBeenCalledOnce();
    expect(onTest).toHaveBeenCalledWith(summary);
  });

  // ── Delete confirmation flow ──────────────────────────────────────────────

  it("opens the confirmation dialog when the Delete button is clicked", async () => {
    render(<SummaryCard summary={makeSummary()} onDelete={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: /delete summary/i }));
    await waitFor(() =>
      expect(
        screen.getByText(/are you sure you want to delete this summary/i),
      ).toBeInTheDocument(),
    );
  });

  it("calls onDelete with the summary instance when deletion is confirmed", async () => {
    const summary = makeSummary();
    const onDelete = vi.fn().mockResolvedValue(undefined);
    render(<SummaryCard summary={summary} onDelete={onDelete} />);

    fireEvent.click(screen.getByRole("button", { name: /delete summary/i }));
    const confirmBtn = await screen.findByRole("button", { name: /^delete$/i });
    fireEvent.click(confirmBtn);

    await waitFor(() => expect(onDelete).toHaveBeenCalledWith(summary));
  });

  it("does not call onDelete when Cancel is clicked in the dialog", async () => {
    const onDelete = vi.fn();
    render(<SummaryCard summary={makeSummary()} onDelete={onDelete} />);

    fireEvent.click(screen.getByRole("button", { name: /delete summary/i }));
    const cancelBtn = await screen.findByRole("button", { name: /cancel/i });
    fireEvent.click(cancelBtn);

    expect(onDelete).not.toHaveBeenCalled();
  });

  it("disables Cancel when isDeletePending is true", async () => {
    render(
      <SummaryCard
        summary={makeSummary()}
        onDelete={vi.fn()}
        isDeletePending
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /delete summary/i }));
    const cancelBtn = await screen.findByRole("button", { name: /cancel/i });
    expect(cancelBtn).toBeDisabled();
  });
});
