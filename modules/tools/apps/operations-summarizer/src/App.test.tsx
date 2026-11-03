import * as appSdk from "@cognite/app-sdk/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig } from "@/config/appConfig";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import App from "./App";

// App boot reads CDF config. The shell test supplies a resolved config.
vi.mock("@/services/appConfigService", () => ({
  createAppConfigServiceFromSdk: () => ({
    load: () => Promise.resolve(parseAppConfig(DEFAULT_NODE_PROPERTIES)),
  }),
}));

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@cognite/app-sdk/react")>();
  return {
    ...actual,
    CogniteSdkProvider: ({ children }: { children: ReactNode }) => children,
    useCogniteSdk: vi.fn(),
  };
});

function WrappedApp({ queryClient }: { queryClient: QueryClient }) {
  return (
    <QueryClientProvider client={queryClient}>
      <CdfClientProvider>
        <App />
      </CdfClientProvider>
    </QueryClientProvider>
  );
}

describe("App", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    vi.clearAllMocks();
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
  });

  it("renders navigation with main nav items", async () => {
    vi.mocked(appSdk.useCogniteSdk).mockReturnValue({
      project: "test-project",
    } as ReturnType<typeof appSdk.useCogniteSdk>);

    render(<WrappedApp queryClient={queryClient} />);
    expect(await screen.findByText("Summaries")).toBeInTheDocument();
    expect(screen.getByText("Tests")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Settings" }),
    ).toBeInTheDocument();
  });

  it("renders Summaries page by default when sdk is available", async () => {
    vi.mocked(appSdk.useCogniteSdk).mockReturnValue({
      project: "my-test-project",
    } as ReturnType<typeof appSdk.useCogniteSdk>);

    render(<WrappedApp queryClient={queryClient} />);
    expect(await screen.findByRole("main")).toBeInTheDocument();
  });
});
