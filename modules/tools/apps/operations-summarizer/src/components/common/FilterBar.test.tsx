import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { FilterBar } from "@/components/common/FilterBar";

describe("FilterBar", () => {
  it("renders date inputs and fires change handlers", () => {
    const onStartDateChange = vi.fn();
    const onEndDateChange = vi.fn();
    render(
      <FilterBar
        dateRange={{
          startDate: "2024-01-01",
          endDate: "2024-01-31",
          onStartDateChange,
          onEndDateChange,
        }}
      />,
    );
    expect(screen.getByText("Period:")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/period start date/i), {
      target: { value: "2024-02-01" },
    });
    fireEvent.change(screen.getByLabelText(/period end date/i), {
      target: { value: "2024-02-28" },
    });
    expect(onStartDateChange).toHaveBeenCalledWith("2024-02-01");
    expect(onEndDateChange).toHaveBeenCalledWith("2024-02-28");
  });

  it("shows Clear only when filters are active and handler is provided", () => {
    const onClearFilters = vi.fn();
    const { rerender } = render(
      <FilterBar
        hasActiveFilters={false}
        onClearFilters={onClearFilters}
        onApplyFilters={vi.fn()}
      />,
    );
    expect(
      screen.queryByRole("button", { name: /clear/i }),
    ).not.toBeInTheDocument();

    rerender(
      <FilterBar
        hasActiveFilters
        onClearFilters={onClearFilters}
        onApplyFilters={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /clear/i }));
    expect(onClearFilters).toHaveBeenCalled();
  });

  it("disables Apply when disableApply is true", () => {
    const onApplyFilters = vi.fn();
    render(<FilterBar onApplyFilters={onApplyFilters} disableApply />);
    const apply = screen.getByRole("button", { name: /apply/i });
    expect(apply).toBeDisabled();
    fireEvent.click(apply);
    expect(onApplyFilters).not.toHaveBeenCalled();
  });

  it("calls onApplyFilters when Apply is clicked", () => {
    const onApplyFilters = vi.fn();
    render(<FilterBar onApplyFilters={onApplyFilters} />);
    fireEvent.click(screen.getByRole("button", { name: /apply/i }));
    expect(onApplyFilters).toHaveBeenCalled();
  });

  it("renders custom actions and data type label", () => {
    render(
      <FilterBar
        layout="responsive"
        dataTypes={{
          selectedTypes: ["assets"],
          onToggle: vi.fn(),
          availableTypes: ["assets", "files"],
        }}
        actions={<button type="button">Custom action</button>}
      />,
    );
    expect(screen.getByText("Data types:")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /custom action/i }),
    ).toBeInTheDocument();
  });
});
