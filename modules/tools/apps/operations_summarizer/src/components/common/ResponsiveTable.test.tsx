import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  Pagination,
  ResponsiveTable,
} from "@/components/common/ResponsiveTable";

describe("ResponsiveTable", () => {
  it("renders both table and cards children", () => {
    render(
      <ResponsiveTable
        table={<div>Desktop table</div>}
        cards={<div>Mobile cards</div>}
      />,
    );
    expect(screen.getByText("Desktop table")).toBeInTheDocument();
    expect(screen.getByText("Mobile cards")).toBeInTheDocument();
  });
});

describe("Pagination", () => {
  it("hides controls when totalPages is 1 in normal mode", () => {
    const { container } = render(
      <Pagination
        currentPage={1}
        totalPages={1}
        totalItems={5}
        hasNextPage={false}
        hasPreviousPage={false}
        onNextPage={vi.fn()}
        onPreviousPage={vi.fn()}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("shows page label and wires next/previous clicks", () => {
    const onNextPage = vi.fn();
    const onPreviousPage = vi.fn();
    render(
      <Pagination
        currentPage={2}
        totalPages={4}
        totalItems={40}
        hasNextPage
        hasPreviousPage
        onNextPage={onNextPage}
        onPreviousPage={onPreviousPage}
        itemName="summaries"
      />,
    );
    expect(screen.getByText("Page 2 of 4 (40 summaries)")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /previous/i }));
    expect(onNextPage).toHaveBeenCalled();
    expect(onPreviousPage).toHaveBeenCalled();
  });

  it("hides cursor-mode controls when neither prev nor next exists", () => {
    const { container } = render(
      <Pagination
        currentPage={1}
        totalPages={1}
        totalItems={10}
        hasNextPage={false}
        hasPreviousPage={false}
        onNextPage={vi.fn()}
        onPreviousPage={vi.fn()}
        cursorMode
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("shows cursor-mode label and disables Previous on first page", () => {
    render(
      <Pagination
        currentPage={1}
        totalPages={99}
        totalItems={25}
        hasNextPage
        hasPreviousPage={false}
        onNextPage={vi.fn()}
        onPreviousPage={vi.fn()}
        cursorMode
      />,
    );
    expect(
      screen.getByText("Page 1 · more available (25 on this page)"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /previous/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /next/i })).not.toBeDisabled();
  });
});
