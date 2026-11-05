import { fireEvent, render, screen } from "@testing-library/react";
import type { ButtonHTMLAttributes, ComponentProps, ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { getDisplayName } from "@/config/dataTypes";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { TestConfigCard } from "./TestConfigCard";

// Aura reads `import.meta.env` at module load. Vitest does not transform that
// dependency, so the real package throws before the card can render.
vi.mock("@cognite/aura/components", () => ({
  Badge: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
  Button: ({ children, ...props }: ButtonHTMLAttributes<HTMLButtonElement>) => (
    <button type="button" {...props}>
      {children}
    </button>
  ),
  buttonVariants: () => "",
  Card: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  CardContent: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  CardDescription: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  CardFooter: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  CardHeader: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  CardTitle: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  DropdownMenu: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  DropdownMenuTrigger: ({ render }: { render?: ReactNode }) => <>{render}</>,
  DropdownMenuContent: ({ children }: { children?: ReactNode }) => (
    <div>{children}</div>
  ),
  DropdownMenuSeparator: () => <hr />,
  DropdownMenuCheckboxItem: ({
    children,
    disabled,
    onCheckedChange,
  }: {
    children?: ReactNode;
    disabled?: boolean;
    onCheckedChange?: () => void;
  }) => (
    <button
      type="button"
      role="menuitemcheckbox"
      aria-checked={false}
      disabled={disabled}
      onClick={() => onCheckedChange?.()}
    >
      {children}
    </button>
  ),
}));

const VIEW = "cdf_cdm/CogniteFile/v1";

const selectedSummary: SummaryInstance = {
  externalId: "ops_summary_2024-01-01_2024-01-31_x",
  space: "ops_summaries",
  createdTime: 1_700_000_000_000,
  properties: {
    [VIEW]: { name: "S", content: "# Hello\nWorld" },
  },
};

const defaultHandlers = {
  onChangeStartDate: vi.fn(),
  onChangeEndDate: vi.fn(),
  onToggleDataType: vi.fn(),
  onSetDataTypes: vi.fn(),
  onChangeSummary: vi.fn(),
  onRunTest: vi.fn(),
  onDismissError: vi.fn(),
};

function renderCard(
  overrides: Partial<ComponentProps<typeof TestConfigCard>> = {},
) {
  return render(
    <TestConfigCard
      selectedSummary={selectedSummary}
      selectedDataTypes={["assets"] as DataTypeName[]}
      testStartDate="2024-01-01"
      testEndDate="2024-01-31"
      isRunning={false}
      generationProgress={{ status: "idle", message: "", progress: 0 }}
      testError={null}
      {...defaultHandlers}
      {...overrides}
    />,
  );
}

describe("TestConfigCard", () => {
  it("shows progress bar when running", () => {
    renderCard({
      isRunning: true,
      generationProgress: {
        status: "generating",
        message: "Working…",
        progress: 42,
        currentStep: "compare",
      },
    });
    expect(screen.getByRole("progressbar")).toBeInTheDocument();
    expect(screen.getByText("Working…")).toBeInTheDocument();
  });

  it("shows error and calls onDismissError", () => {
    renderCard({ testError: "Something broke" });
    expect(screen.getByText("Something broke")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /dismiss/i }));
    expect(defaultHandlers.onDismissError).toHaveBeenCalled();
  });

  it("disables run when dates or data types missing", () => {
    const { rerender } = render(
      <TestConfigCard
        selectedSummary={selectedSummary}
        selectedDataTypes={[]}
        testStartDate=""
        testEndDate=""
        isRunning={false}
        generationProgress={{ status: "idle", message: "", progress: 0 }}
        testError={null}
        {...defaultHandlers}
      />,
    );
    expect(
      screen.getByRole("button", { name: /run comparison test/i }),
    ).toBeDisabled();
    expect(screen.getByText(/none selected/i)).toBeInTheDocument();

    rerender(
      <TestConfigCard
        selectedSummary={selectedSummary}
        selectedDataTypes={["assets"] as DataTypeName[]}
        testStartDate="2024-01-01"
        testEndDate=""
        isRunning={false}
        generationProgress={{ status: "idle", message: "", progress: 0 }}
        testError={null}
        {...defaultHandlers}
      />,
    );
    expect(
      screen.getByRole("button", { name: /run comparison test/i }),
    ).toBeDisabled();

    rerender(
      <TestConfigCard
        selectedSummary={selectedSummary}
        selectedDataTypes={["assets"] as DataTypeName[]}
        testStartDate="2024-01-01"
        testEndDate="2024-01-31"
        isRunning={false}
        generationProgress={{ status: "idle", message: "", progress: 0 }}
        testError={null}
        {...defaultHandlers}
      />,
    );
    const runBtn = screen.getByRole("button", { name: /run comparison test/i });
    expect(runBtn).not.toBeDisabled();
    fireEvent.click(runBtn);
    expect(defaultHandlers.onRunTest).toHaveBeenCalled();
  });

  it("disables Change summary while running", () => {
    renderCard({ isRunning: true });
    expect(screen.getByRole("button", { name: /^change$/i })).toBeDisabled();
  });

  it("forwards period edits and the change-summary action", () => {
    renderCard();
    fireEvent.change(screen.getByLabelText(/test period start date/i), {
      target: { value: "2024-02-01" },
    });
    fireEvent.change(screen.getByLabelText(/test period end date/i), {
      target: { value: "2024-02-28" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^change$/i }));
    expect(defaultHandlers.onChangeStartDate).toHaveBeenCalledWith(
      "2024-02-01",
    );
    expect(defaultHandlers.onChangeEndDate).toHaveBeenCalledWith("2024-02-28");
    expect(defaultHandlers.onChangeSummary).toHaveBeenCalled();
    expect(screen.getByText("S")).toBeInTheDocument();
  });

  it("keeps date inputs disabled while a test is running", () => {
    renderCard({ isRunning: true });
    expect(screen.getByLabelText(/test period start date/i)).toBeDisabled();
    expect(screen.getByLabelText(/test period end date/i)).toBeDisabled();
    expect(
      screen.queryByRole("button", { name: /run comparison test/i }),
    ).not.toBeInTheDocument();
  });

  it("uses a fallback title and a default progress message", () => {
    renderCard({
      selectedSummary: {
        ...selectedSummary,
        externalId: ".pdf",
        properties: {},
      },
      isRunning: true,
      generationProgress: {
        status: "generating",
        message: "",
        progress: 10,
        currentStep: "compare",
      },
    });
    expect(
      screen.getByRole("heading", { name: "Selected Summary" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Running test...")).toBeInTheDocument();
    expect(screen.getByRole("progressbar")).toHaveAttribute(
      "aria-valuetext",
      expect.stringContaining("compare"),
    );
  });

  it("toggles one data type from the picker", () => {
    renderCard();
    fireEvent.click(
      screen.getByRole("button", { name: getDisplayName("assets") }),
    );
    fireEvent.click(
      screen.getByRole("menuitemcheckbox", { name: getDisplayName("files") }),
    );
    expect(defaultHandlers.onToggleDataType).toHaveBeenCalledWith("files");
  });

  it("sends the full selection when Select All is used", () => {
    renderCard();
    fireEvent.click(
      screen.getByRole("button", { name: getDisplayName("assets") }),
    );
    fireEvent.click(screen.getByRole("button", { name: /select all/i }));
    expect(defaultHandlers.onSetDataTypes).toHaveBeenCalledWith(
      expect.arrayContaining(["assets", "files"]),
    );
  });

  it("keeps the data type picker disabled while a test is running", () => {
    defaultHandlers.onToggleDataType.mockClear();
    renderCard({ isRunning: true });
    expect(
      screen.getByRole("button", { name: getDisplayName("assets") }),
    ).toBeDisabled();
    expect(defaultHandlers.onToggleDataType).not.toHaveBeenCalled();
  });
});
