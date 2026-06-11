import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useAppSdk } from "@/providers/AppSdkProvider";
import App from "./App";

vi.mock("@/providers/AppSdkProvider");

const renderApp = () => {
  const queryClient = new QueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  );
};

describe("App", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders loading state", () => {
    vi.mocked(useAppSdk).mockReturnValue({
      sdk: null,
      isLoading: true,
      isLocalMode: true,
      projectName: "local",
    });

    renderApp();
    expect(screen.getByText("Loading Dashboard")).toBeInTheDocument();
  });

  it("renders app with project name", () => {
    vi.mocked(useAppSdk).mockReturnValue({
      sdk: null,
      isLoading: false,
      isLocalMode: true,
      projectName: "my-test-project",
    });

    renderApp();
    expect(screen.getByText("my-test-project")).toBeInTheDocument();
    expect(screen.getAllByText("File Annotation Dashboard").length).toBeGreaterThan(0);
  });
});

