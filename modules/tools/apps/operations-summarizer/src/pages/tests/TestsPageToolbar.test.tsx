import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TestsFetchErrorBanner } from "./TestsFetchErrorBanner";
import { TestsHistoryBar } from "./TestsHistoryBar";
import { TestsPageToolbar } from "./TestsPageToolbar";
import { TestsScoreLegend } from "./TestsScoreLegend";

const toolbarProps = {
  pageDisplay: 2,
  totalTests: 12,
  statsUnavailable: false,
  onToggleCreateForm: vi.fn(),
  onRefresh: vi.fn(),
  isRefreshDisabled: false,
  refreshSuccess: false,
  refreshError: false,
  refreshMessage: "Refresh",
  onShowInfo: vi.fn(),
};

describe("TestsPageToolbar", () => {
  it("shows the page total and wires toolbar actions", () => {
    const onToggleCreateForm = vi.fn();
    const onRefresh = vi.fn();
    const onShowInfo = vi.fn();

    render(
      <TestsPageToolbar
        {...toolbarProps}
        onToggleCreateForm={onToggleCreateForm}
        onRefresh={onRefresh}
        onShowInfo={onShowInfo}
        refreshSuccess
        refreshMessage="Updated"
      />,
    );

    expect(screen.getByRole("heading", { name: "Tests" })).toBeInTheDocument();
    expect(screen.getByText("Page 2 · 12 total")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /create new test/i }));
    fireEvent.click(screen.getByRole("button", { name: /updated/i }));
    fireEvent.click(
      screen.getByRole("button", { name: /show testing information/i }),
    );
    expect(onToggleCreateForm).toHaveBeenCalledOnce();
    expect(onRefresh).toHaveBeenCalledOnce();
    expect(onShowInfo).toHaveBeenCalledOnce();
  });

  it("omits the total when stats are unavailable", () => {
    render(
      <TestsPageToolbar {...toolbarProps} statsUnavailable totalTests={12} />,
    );
    expect(screen.getByText("Page 2")).toBeInTheDocument();
    expect(screen.queryByText(/12 total/)).not.toBeInTheDocument();
  });
});

describe("TestsScoreLegend", () => {
  it("lists the score bands", () => {
    render(<TestsScoreLegend />);
    expect(screen.getByText("Excellent")).toBeInTheDocument();
    expect(screen.getByText("Good")).toBeInTheDocument();
    expect(screen.getByText("Needs attention")).toBeInTheDocument();
  });
});

describe("TestsFetchErrorBanner", () => {
  it("lists only the failed sources", () => {
    render(
      <TestsFetchErrorBanner
        isTestReportsError
        isSummariesPickerError={false}
        isTestStatsError
      />,
    );
    expect(
      screen.getByText("Test reports list could not be loaded."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Test statistics could not be loaded."),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/baseline picker could not be loaded/),
    ).not.toBeInTheDocument();
  });
});

describe("TestsHistoryBar", () => {
  const historyProps = {
    statsUnavailable: false,
    testStatsLoading: false,
    hasStreamedStats: false,
    totalTests: 8,
    averageScore: 81.2,
    excellentCount: 3,
    goodCount: 4,
    poorCount: 1,
    minScore: 0,
    maxScore: 100,
    onMinScoreChange: vi.fn(),
    onMaxScoreChange: vi.fn(),
    hasActiveHistoryFilters: false,
    onClearFilters: vi.fn(),
  };

  it("shows loaded statistics", () => {
    render(<TestsHistoryBar {...historyProps} />);
    expect(screen.getByText("8")).toBeInTheDocument();
    expect(screen.getByText("81.2%")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Clear" }),
    ).not.toBeInTheDocument();
  });

  it("shows a loading state before streamed stats arrive", () => {
    render(<TestsHistoryBar {...historyProps} testStatsLoading />);
    expect(screen.getByText("Loading statistics…")).toBeInTheDocument();
  });

  it("shows the unavailable message and clamps score edits", () => {
    const onMinScoreChange = vi.fn();
    const onMaxScoreChange = vi.fn();
    const onClearFilters = vi.fn();
    render(
      <TestsHistoryBar
        {...historyProps}
        statsUnavailable
        hasActiveHistoryFilters
        onMinScoreChange={onMinScoreChange}
        onMaxScoreChange={onMaxScoreChange}
        onClearFilters={onClearFilters}
      />,
    );
    expect(
      screen.getByText("Stats unavailable — refresh to retry."),
    ).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Minimum score"), {
      target: { value: "140" },
    });
    fireEvent.change(screen.getByLabelText("Maximum score"), {
      target: { value: "" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(onMinScoreChange).toHaveBeenCalledWith(100);
    expect(onMaxScoreChange).toHaveBeenCalledWith(100);
    expect(onClearFilters).toHaveBeenCalledOnce();
  });
});
