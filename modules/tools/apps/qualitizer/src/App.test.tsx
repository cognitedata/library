import * as appAuth from "./shared/auth";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";

// Mock the @cognite/dune module
vi.mock(import("./shared/auth"));
vi.mock("./internal/models", () => ({
  DataModelSelector: () => <div>DataModelSelector</div>,
}));
vi.mock("./health-checks", () => ({
  HealthChecks: () => <div>HealthChecks</div>,
}));

describe("App", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders loading state", () => {
    vi.mocked(appAuth.useAppSdk).mockReturnValue({
      sdk: { project: "test-project" } as any,
      isLoading: true,
    });

    render(<App />);
    expect(screen.getByText("Loading...")).toBeInTheDocument();
  });

  it("renders app with project name", () => {
    vi.mocked(appAuth.useAppSdk).mockReturnValue({
      sdk: { project: "my-test-project" } as any,
      isLoading: false,
    });

    render(<App />);
    expect(screen.getByText("HealthChecks")).toBeInTheDocument();
    expect(screen.getByText("Project: my-test-project")).toBeInTheDocument();
  });
});

