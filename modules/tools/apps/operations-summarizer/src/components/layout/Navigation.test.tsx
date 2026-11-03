import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Navigation } from "@/components/layout/Navigation";

const navigate = vi.fn();
let currentPage: "summaries" | "tests" | "settings" = "summaries";

// Navigation state lives in a provider. This unit test sets the current page directly.
vi.mock("@/contexts/NavigationContext", () => ({
  useNavigation: () => ({ currentPage, navigate }),
}));

describe("Navigation", () => {
  beforeEach(() => {
    navigate.mockClear();
    currentPage = "summaries";
    document.documentElement.classList.remove("dark");
    document.documentElement.style.colorScheme = "";
  });

  it("marks Summaries as the current page and navigates to Tests", () => {
    render(<Navigation />);
    const summariesNav = screen
      .getAllByRole("button", { name: /^summaries$/i })
      .find((btn) => btn.getAttribute("aria-current") === "page");
    expect(summariesNav).toBeDefined();
    fireEvent.click(screen.getAllByRole("button", { name: /^tests$/i })[0]);
    expect(navigate).toHaveBeenCalledWith("tests");
  });

  it("navigates to summaries when the logo is clicked", () => {
    currentPage = "tests";
    render(<Navigation />);
    fireEvent.click(screen.getByRole("button", { name: /go to summaries/i }));
    expect(navigate).toHaveBeenCalledWith("summaries");
  });

  it("navigates to settings from the settings button", () => {
    render(<Navigation />);
    fireEvent.click(screen.getByRole("button", { name: /^settings$/i }));
    expect(navigate).toHaveBeenCalledWith("settings");
  });

  it("toggles dark and light theme on the document root", () => {
    render(<Navigation />);
    fireEvent.click(screen.getByRole("button", { name: /use dark theme/i }));
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.style.colorScheme).toBe("dark");

    fireEvent.click(screen.getByRole("button", { name: /use light theme/i }));
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(document.documentElement.style.colorScheme).toBe("light");
  });
});
