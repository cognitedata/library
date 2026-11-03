import { fireEvent, render, screen } from "@testing-library/react";
import type {
  ButtonHTMLAttributes,
  ComponentProps,
  HTMLAttributes,
  ReactElement,
  ReactNode,
} from "react";
import { describe, expect, it, vi } from "vitest";
import type { DataTypeName } from "@/types/operations";
import {
  SummariesCreateForm,
  SummariesListFilters,
} from "./SummariesCreateForm";

// Aura evaluates `import.meta.env.VITEST` while loading `@cognite/aura/components`.
// Vitest leaves `import.meta.env` undefined for that dependency, so the real
// package throws before the form can render. These stand-ins keep the controls queryable.
vi.mock("@cognite/aura/components", () => {
  function Button({
    children,
    ...props
  }: ButtonHTMLAttributes<HTMLButtonElement>) {
    return <button {...props}>{children}</button>;
  }

  function Badge({ children, ...props }: HTMLAttributes<HTMLSpanElement>) {
    return <span {...props}>{children}</span>;
  }

  function Card({ children, ...props }: HTMLAttributes<HTMLDivElement>) {
    return <div {...props}>{children}</div>;
  }

  function DropdownMenuCheckboxItem({
    children,
    disabled,
    onCheckedChange,
  }: {
    children?: ReactNode;
    checked?: boolean;
    disabled?: boolean;
    onCheckedChange?: (checked: boolean) => void;
  }) {
    return (
      <button
        type="button"
        role="menuitemcheckbox"
        aria-checked={false}
        disabled={disabled}
        onClick={() => onCheckedChange?.(true)}
      >
        {children}
      </button>
    );
  }

  return {
    Button,
    Badge,
    Card,
    CardContent: Card,
    CardDescription: Card,
    CardFooter: Card,
    CardHeader: Card,
    CardTitle: Card,
    buttonVariants: () => "",
    DropdownMenu: ({ children }: { children?: ReactNode }) => (
      <div>{children}</div>
    ),
    DropdownMenuTrigger: ({ render }: { render: ReactElement }) => render,
    DropdownMenuContent: ({ children }: { children?: ReactNode }) => (
      <div>{children}</div>
    ),
    DropdownMenuSeparator: () => <hr />,
    DropdownMenuCheckboxItem,
  };
});

const summaryContents = {
  assets: true,
  files: true,
  work_orders: false,
  time_series: false,
  equipment: false,
};

const baseFormProps: ComponentProps<typeof SummariesCreateForm> = {
  hasSelectedAgent: true,
  generationError: null,
  periodStart: new Date("2024-01-01T00:00:00Z"),
  periodEnd: new Date("2024-01-08T00:00:00Z"),
  formatDateForInput: (date) => date.toISOString().slice(0, 10),
  onStartDateChange: vi.fn(),
  onEndDateChange: vi.fn(),
  enabledDataTypes: ["assets", "files", "work_orders"],
  summaryContents,
  onToggleContent: vi.fn(),
  daysSelected: 7,
  isGenerating: false,
  onCancel: vi.fn(),
  onGenerate: vi.fn(),
  selectedCount: 2,
  progress: {
    status: "idle",
    message: "",
    progress: 0,
  },
  elapsedSeconds: 0,
  showSummary: false,
  generatedSummary: null,
  savedPdfInfo: null,
  isSaving: false,
  onDownload: vi.fn(),
  saveError: null,
  onClose: vi.fn(),
};

function renderForm(
  overrides: Partial<ComponentProps<typeof SummariesCreateForm>> = {},
) {
  const props = { ...baseFormProps, ...overrides };
  render(<SummariesCreateForm {...props} />);
  return props;
}

describe("SummariesCreateForm", () => {
  it("renders period fields, selected count, and the idle prompt", () => {
    renderForm({ enabledDataTypes: ["assets", "files", "not_a_type"] });

    expect(
      screen.getByRole("heading", { name: "Create new summary" }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("Period start date")).toHaveValue(
      "2024-01-01",
    );
    expect(screen.getByLabelText("Period end date")).toHaveValue("2024-01-08");
    expect(screen.getByText("7d")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Generate (2)" })).toBeEnabled();
    expect(
      screen.getByText(
        "Configure options above and click Generate to create a summary.",
      ),
    ).toBeInTheDocument();
  });

  it("shows agent and generation errors and disables generate", () => {
    renderForm({
      hasSelectedAgent: false,
      generationError: "Function timed out",
      selectedCount: 0,
      daysSelected: 0,
    });

    expect(screen.getByText("No agent selected")).toBeInTheDocument();
    expect(screen.getByText("Function timed out")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Generate (0)" })).toBeDisabled();
  });

  it("disables generate when the period length is not positive", () => {
    renderForm({ daysSelected: -1 });
    expect(screen.getByRole("button", { name: "Generate (2)" })).toBeDisabled();
  });

  it("forwards date input changes and generate and close actions", () => {
    const startValues: string[] = [];
    const endValues: string[] = [];
    const onGenerate = vi.fn();
    const onClose = vi.fn();
    renderForm({
      onGenerate,
      onClose,
      onStartDateChange: (event) => {
        startValues.push(event.target.value);
      },
      onEndDateChange: (event) => {
        endValues.push(event.target.value);
      },
    });

    fireEvent.change(screen.getByLabelText("Period start date"), {
      target: { value: "2024-02-01" },
    });
    fireEvent.change(screen.getByLabelText("Period end date"), {
      target: { value: "2024-02-10" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Generate (2)" }));
    fireEvent.click(
      screen.getByRole("button", { name: "Close create summary form" }),
    );

    expect(startValues).toEqual(["2024-02-01"]);
    expect(endValues).toEqual(["2024-02-10"]);
    expect(onGenerate).toHaveBeenCalledOnce();
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("toggles a data type from the contents control", () => {
    const onToggleContent = vi.fn();
    renderForm({ onToggleContent });

    fireEvent.click(screen.getByRole("button", { name: /2 selected/i }));
    fireEvent.click(screen.getByRole("menuitemcheckbox", { name: "Assets" }));

    expect(onToggleContent).toHaveBeenCalledWith("assets");
  });

  it("shows stop waiting and a timed progress message while generating", () => {
    const onCancel = vi.fn();
    renderForm({
      isGenerating: true,
      onCancel,
      elapsedSeconds: 12,
      progress: {
        status: "generating",
        message: "Fetching work orders",
        progress: 40,
        currentStep: "work orders",
      },
    });

    fireEvent.click(screen.getByRole("button", { name: "Stop waiting" }));
    expect(onCancel).toHaveBeenCalledOnce();
    expect(screen.getByText("Fetching work orders (12s)")).toBeInTheDocument();
    expect(
      screen.queryByText(
        "Configure options above and click Generate to create a summary.",
      ),
    ).not.toBeInTheDocument();
  });

  it("uses the default progress message when none is provided", () => {
    renderForm({
      isGenerating: true,
      progress: { status: "generating", message: "", progress: 10 },
    });
    expect(screen.getByText("Generating in CDF Function…")).toBeInTheDocument();
  });

  it("renders the generated summary and download action", () => {
    const onDownload = vi.fn();
    renderForm({
      showSummary: true,
      onDownload,
      saveError: "Could not save",
      generatedSummary: {
        workOrders: [],
        instances: [],
        fullText: "# Ops\n\nBody content",
        generatedAt: new Date("2024-01-15T10:00:00Z"),
      },
    });

    expect(screen.getByText("Operations Summary")).toBeInTheDocument();
    expect(screen.getByText("Could not save")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /download md/i }));
    expect(onDownload).toHaveBeenCalledOnce();
  });

  it("hides the idle prompt when a summary is shown without content", () => {
    renderForm({ showSummary: true, generatedSummary: null });
    expect(
      screen.queryByText(
        "Configure options above and click Generate to create a summary.",
      ),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Operations Summary")).not.toBeInTheDocument();
  });
});

const baseFilterProps: ComponentProps<typeof SummariesListFilters> = {
  startDate: "2024-03-01",
  endDate: "2024-03-31",
  onStartDate: vi.fn(),
  onEndDate: vi.fn(),
  selectedContentTypes: ["assets"] as DataTypeName[],
  contentTypeFilterEnabled: true,
  enabledDataTypes: ["assets", "files"],
  onToggleContentType: vi.fn(),
  onToggleContentTypeFilter: vi.fn(),
  onSetContentTypes: vi.fn(),
  hasActiveFilters: true,
  onClear: vi.fn(),
  onApply: vi.fn(),
};

describe("SummariesListFilters", () => {
  it("applies and clears a date range", () => {
    const props = { ...baseFilterProps, onApply: vi.fn(), onClear: vi.fn() };
    render(<SummariesListFilters {...props} />);

    fireEvent.change(screen.getByLabelText("Filter summaries from date"), {
      target: { value: "2024-04-01" },
    });
    fireEvent.change(screen.getByLabelText("Filter summaries to date"), {
      target: { value: "2024-04-30" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));

    expect(props.onStartDate).toHaveBeenCalledWith("2024-04-01");
    expect(props.onEndDate).toHaveBeenCalledWith("2024-04-30");
    expect(props.onApply).toHaveBeenCalledOnce();
    expect(props.onClear).toHaveBeenCalledOnce();
  });

  it("disables apply and hides clear when the range is incomplete", () => {
    render(
      <SummariesListFilters
        {...baseFilterProps}
        startDate=""
        endDate=""
        hasActiveFilters={false}
      />,
    );
    expect(screen.getByRole("button", { name: "Apply" })).toBeDisabled();
    expect(
      screen.queryByRole("button", { name: "Clear" }),
    ).not.toBeInTheDocument();
  });

  it("sets every content type when Select All turns the filter on", () => {
    const onToggleContentTypeFilter = vi.fn();
    const onSetContentTypes = vi.fn();
    render(
      <SummariesListFilters
        {...baseFilterProps}
        selectedContentTypes={[]}
        contentTypeFilterEnabled={false}
        hasActiveFilters={false}
        onToggleContentTypeFilter={onToggleContentTypeFilter}
        onSetContentTypes={onSetContentTypes}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Select All" }));

    expect(onToggleContentTypeFilter).toHaveBeenCalledOnce();
    expect(onSetContentTypes).toHaveBeenCalledWith(["assets", "files"]);
  });

  it("clears content types and turns the filter off", () => {
    const onToggleContentTypeFilter = vi.fn();
    const onSetContentTypes = vi.fn();
    render(
      <SummariesListFilters
        {...baseFilterProps}
        selectedContentTypes={["assets", "files"]}
        contentTypeFilterEnabled={true}
        onToggleContentTypeFilter={onToggleContentTypeFilter}
        onSetContentTypes={onSetContentTypes}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Clear All" }));

    expect(onSetContentTypes).toHaveBeenCalledWith([]);
    expect(onToggleContentTypeFilter).toHaveBeenCalledOnce();
  });

  it("enables the content-type filter when the first type is chosen", () => {
    const onToggleContentType = vi.fn();
    const onToggleContentTypeFilter = vi.fn();
    render(
      <SummariesListFilters
        {...baseFilterProps}
        selectedContentTypes={[]}
        contentTypeFilterEnabled={false}
        hasActiveFilters={false}
        onToggleContentType={onToggleContentType}
        onToggleContentTypeFilter={onToggleContentTypeFilter}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /select data types/i }));
    fireEvent.click(screen.getByRole("menuitemcheckbox", { name: "Files" }));

    expect(onToggleContentTypeFilter).toHaveBeenCalledOnce();
    expect(onToggleContentType).toHaveBeenCalledWith("files");
  });
});
