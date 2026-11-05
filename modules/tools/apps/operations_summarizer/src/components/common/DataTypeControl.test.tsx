import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  DataTypeBadges,
  DataTypeControl,
} from "@/components/common/DataTypeControl";

// The dropdown is a portal widget. This test checks the control that wraps it.
vi.mock("@/components/ui/multiselect-dropdown", () => ({
  MultiSelectDropdown: ({
    selectedValues,
    onSelectionChange,
    placeholder,
    disabled,
  }: {
    selectedValues: string[];
    onSelectionChange: (values: string[]) => void;
    placeholder?: string;
    disabled?: boolean;
  }) => (
    <div>
      <span data-testid="trigger-text">
        {selectedValues.length === 0
          ? placeholder
          : `${selectedValues.length} selected`}
      </span>
      <button
        type="button"
        disabled={disabled}
        onClick={() => onSelectionChange([...selectedValues, "files"])}
      >
        add-one
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={() => onSelectionChange(["assets", "files", "work_orders"])}
      >
        bulk-set
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={() => onSelectionChange([])}
      >
        clear-all
      </button>
    </div>
  ),
}));

describe("DataTypeControl", () => {
  it("renders the default label", () => {
    render(<DataTypeControl selectedTypes={["assets"]} onToggle={vi.fn()} />);
    expect(screen.getByText("Data Types:")).toBeInTheDocument();
  });

  it("hides the label when label is empty", () => {
    render(
      <DataTypeControl
        selectedTypes={["assets"]}
        onToggle={vi.fn()}
        label=""
      />,
    );
    expect(screen.queryByText(/Data Types/i)).not.toBeInTheDocument();
  });

  it("calls onToggle for a single selection change", () => {
    const onToggle = vi.fn();
    const onSelectionChange = vi.fn();
    render(
      <DataTypeControl
        selectedTypes={["assets"]}
        onToggle={onToggle}
        onSelectionChange={onSelectionChange}
        availableTypes={["assets", "files", "work_orders"]}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /add-one/i }));
    expect(onToggle).toHaveBeenCalledWith("files");
    expect(onSelectionChange).not.toHaveBeenCalled();
  });

  it("calls onSelectionChange for bulk changes when provided", () => {
    const onToggle = vi.fn();
    const onSelectionChange = vi.fn();
    render(
      <DataTypeControl
        selectedTypes={["assets"]}
        onToggle={onToggle}
        onSelectionChange={onSelectionChange}
        availableTypes={["assets", "files", "work_orders"]}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /bulk-set/i }));
    expect(onSelectionChange).toHaveBeenCalledWith([
      "assets",
      "files",
      "work_orders",
    ]);
    expect(onToggle).not.toHaveBeenCalled();
  });

  it("falls back to individual toggles for bulk clear when onSelectionChange is omitted", () => {
    const onToggle = vi.fn();
    render(
      <DataTypeControl
        selectedTypes={["assets", "files"]}
        onToggle={onToggle}
        availableTypes={["assets", "files"]}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /clear-all/i }));
    expect(onToggle).toHaveBeenCalledWith("assets");
    expect(onToggle).toHaveBeenCalledWith("files");
  });
});

describe("DataTypeBadges", () => {
  it("shows None selected when nothing is selected", () => {
    render(<DataTypeBadges selectedTypes={[]} />);
    expect(screen.getByText("None selected")).toBeInTheDocument();
  });

  it("truncates with a +N badge when over maxVisible", () => {
    render(
      <DataTypeBadges
        selectedTypes={["assets", "files", "work_orders", "time_series"]}
        maxVisible={2}
        availableTypes={["assets", "files", "work_orders", "time_series"]}
      />,
    );
    expect(screen.getByText("+2")).toBeInTheDocument();
  });
});
