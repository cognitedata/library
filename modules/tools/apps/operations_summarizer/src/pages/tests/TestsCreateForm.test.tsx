import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type {
  SummaryInstance,
  TestGenerationProgress,
} from "@/types/operations";
import { encodeTestBaselineSelectionKey } from "@/utils/summaryInstanceMapping";
import { TestsCreateForm, type TestsCreateFormProps } from "./TestsCreateForm";

vi.mock("@/utils/summaryListQuery", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@/utils/summaryListQuery")>();
  return {
    ...actual,
    getOperationSummaryPickerMax: () => 25,
  };
});

const progress: TestGenerationProgress = {
  status: "idle",
  message: "",
  progress: 0,
};

function summary(): SummaryInstance {
  return {
    externalId: "ops_summary_2024-01-01_2024-01-31_abc",
    space: "ops",
    createdTime: 1_700_000_000_000,
    properties: {},
  };
}

function props(
  overrides: Partial<TestsCreateFormProps> = {},
): TestsCreateFormProps {
  return {
    summaries: [],
    filteredSummaries: [],
    filterStartDate: "2024-01-01",
    filterEndDate: "2024-01-31",
    onFilterStartDate: vi.fn(),
    onFilterEndDate: vi.fn(),
    selectedSummaryId: "",
    onSelectSummary: vi.fn(),
    selectedSummary: undefined,
    testResult: null,
    selectedDataTypes: [],
    testStartDate: "2024-01-01",
    testEndDate: "2024-01-31",
    isRunning: false,
    generationProgress: progress,
    testError: null,
    onChangeStartDate: vi.fn(),
    onChangeEndDate: vi.fn(),
    onToggleDataType: vi.fn(),
    onSetDataTypes: vi.fn(),
    onClearSelectedSummary: vi.fn(),
    onRunTest: vi.fn(),
    onDismissError: vi.fn(),
    isSaving: false,
    yamlExported: false,
    yamlContent: "",
    onSave: vi.fn(async () => undefined),
    onExportYaml: vi.fn(),
    onCopyYaml: vi.fn(async () => undefined),
    onDownloadYaml: vi.fn(),
    onClearResults: vi.fn(),
    saveError: null,
    onClose: vi.fn(),
    onCreateSummaryFirst: vi.fn(),
    ...overrides,
  };
}

describe("TestsCreateForm", () => {
  it("asks to create a summary when none exist", () => {
    const onCreateSummaryFirst = vi.fn();
    render(<TestsCreateForm {...props({ onCreateSummaryFirst })} />);

    fireEvent.click(
      screen.getByRole("button", { name: "Create Summary First" }),
    );
    expect(onCreateSummaryFirst).toHaveBeenCalledOnce();
    expect(screen.queryByText(/Showing up to/)).not.toBeInTheDocument();
  });

  it("lists filtered baselines and reports picker and date changes", () => {
    const row = summary();
    const onFilterStartDate = vi.fn();
    const onSelectSummary = vi.fn();
    render(
      <TestsCreateForm
        {...props({
          summaries: [row],
          filteredSummaries: [row],
          onFilterStartDate,
          onSelectSummary,
        })}
      />,
    );

    expect(screen.getByText("Select a Baseline Summary")).toBeInTheDocument();
    expect(
      screen.getByText(/Showing up to 25 most recent/),
    ).toBeInTheDocument();

    fireEvent.change(
      screen.getByLabelText("Filter baseline summaries from date"),
      {
        target: { value: "2024-02-01" },
      },
    );
    expect(onFilterStartDate).toHaveBeenCalledWith("2024-02-01");

    fireEvent.change(screen.getByLabelText("Select baseline summary"), {
      target: { value: encodeTestBaselineSelectionKey(row) },
    });
    expect(onSelectSummary).toHaveBeenCalledWith(
      encodeTestBaselineSelectionKey(row),
    );
  });

  it("shows the test config when a baseline is selected", () => {
    const row = summary();
    render(
      <TestsCreateForm
        {...props({
          summaries: [row],
          filteredSummaries: [row],
          selectedSummary: row,
          selectedSummaryId: encodeTestBaselineSelectionKey(row),
        })}
      />,
    );

    expect(screen.getByRole("button", { name: "Change" })).toBeInTheDocument();
    expect(
      screen.queryByText("Select a Baseline Summary"),
    ).not.toBeInTheDocument();
  });

  it("closes the form", () => {
    const onClose = vi.fn();
    render(<TestsCreateForm {...props({ onClose })} />);
    fireEvent.click(
      screen.getByRole("button", { name: "Close create test form" }),
    );
    expect(onClose).toHaveBeenCalledOnce();
  });
});
