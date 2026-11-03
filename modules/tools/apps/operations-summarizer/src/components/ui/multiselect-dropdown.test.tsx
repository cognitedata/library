import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  MultiSelectDropdown,
  MultiSelectWithBadges,
} from "@/components/ui/multiselect-dropdown";

const options = [
  { value: "a", label: "Alpha" },
  { value: "b", label: "Beta" },
  { value: "c", label: "Gamma", disabled: true },
];

describe("MultiSelectDropdown", () => {
  it("shows placeholder, single label, and count text", () => {
    const { rerender } = render(
      <MultiSelectDropdown
        options={options}
        selectedValues={[]}
        onSelectionChange={vi.fn()}
        placeholder="Pick items"
      />,
    );
    expect(screen.getByText("Pick items")).toBeInTheDocument();

    rerender(
      <MultiSelectDropdown
        options={options}
        selectedValues={["a"]}
        onSelectionChange={vi.fn()}
      />,
    );
    expect(screen.getByText("Alpha")).toBeInTheDocument();

    rerender(
      <MultiSelectDropdown
        options={options}
        selectedValues={["a", "b"]}
        onSelectionChange={vi.fn()}
      />,
    );
    expect(screen.getByText("2 selected")).toBeInTheDocument();
  });

  it("toggles an option and supports Select All / Clear All", () => {
    const onSelectionChange = vi.fn();
    render(
      <MultiSelectDropdown
        options={options}
        selectedValues={["a"]}
        onSelectionChange={onSelectionChange}
        showSelectAll
        showClearAll
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /alpha/i }));
    fireEvent.click(screen.getByRole("menuitemcheckbox", { name: /beta/i }));
    expect(onSelectionChange).toHaveBeenCalledWith(["a", "b"]);

    fireEvent.click(screen.getByRole("button", { name: /select all/i }));
    expect(onSelectionChange).toHaveBeenCalledWith(["a", "b"]);

    fireEvent.click(screen.getByRole("button", { name: /clear all/i }));
    expect(onSelectionChange).toHaveBeenCalledWith([]);
  });

  it("shows footer count when selection is non-empty", () => {
    render(
      <MultiSelectDropdown
        options={options}
        selectedValues={["a"]}
        onSelectionChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /alpha/i }));
    expect(screen.getByText("1 of 3 selected")).toBeInTheDocument();
  });
});

describe("MultiSelectWithBadges", () => {
  it("truncates badges with +N when over maxBadges", () => {
    render(
      <MultiSelectWithBadges
        options={options}
        selectedValues={["a", "b"]}
        onSelectionChange={vi.fn()}
        maxBadges={1}
      />,
    );
    expect(screen.getByText("Alpha")).toBeInTheDocument();
    expect(screen.getByText("+1")).toBeInTheDocument();
  });
});
