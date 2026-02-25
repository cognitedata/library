import * as duneAuth from "@cognite/dune";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";

// Mock the @cognite/dune module
vi.mock(import("@cognite/dune"));

describe("App", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders loading state", () => {
    vi.mocked(duneAuth.useDune).mockReturnValue({
      sdk: { project: "test-project" } as any,
      isLoading: true,
    });

    render(<App />);
    expect(screen.getByText("Loading...")).toBeInTheDocument();
  });

  it("renders app with project name", () => {
    vi.mocked(duneAuth.useDune).mockReturnValue({
      sdk: { project: "my-test-project" } as any,
      isLoading: false,
    });

    render(<App />);
    expect(screen.getByText("Welcome to my-test-project")).toBeInTheDocument();
    expect(screen.getByText("Your Dune app is ready.")).toBeInTheDocument();
  });
});

