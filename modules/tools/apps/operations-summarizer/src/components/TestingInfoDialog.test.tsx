import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TestingInfoDialog } from "@/components/TestingInfoDialog";

describe("TestingInfoDialog", () => {
  it("renders guide content when open", () => {
    render(<TestingInfoDialog open onOpenChange={vi.fn()} />);
    expect(
      screen.getByText("How to Use the Testing Feature"),
    ).toBeInTheDocument();
    expect(screen.getByText("Step-by-Step Guide")).toBeInTheDocument();
    expect(screen.getByText(/Select Summary/i)).toBeInTheDocument();
  });

  it("does not render dialog content when closed", () => {
    render(<TestingInfoDialog open={false} onOpenChange={vi.fn()} />);
    expect(
      screen.queryByText("How to Use the Testing Feature"),
    ).not.toBeInTheDocument();
  });
});
