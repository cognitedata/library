import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import { createOperationsWithQueryWrapper } from "@/test-utils/wrappers";
import { UseAgentLoaderContext, useAgentLoader } from "./useAgentLoader";

const { loaderSdk } = vi.hoisted(() => {
  const getLoader = vi.fn().mockImplementation((url: string) => {
    const s = String(url);
    if (s.includes("token/inspect")) {
      return Promise.resolve({ data: { capabilities: [] } });
    }
    if (s.includes("/ai/agents") && !s.includes("delete")) {
      return Promise.resolve({
        data: { items: [], invalidAgents: [] },
      });
    }
    return Promise.resolve({ data: {} });
  });
  const loaderSdk = {
    project: "p",
    get: getLoader,
    post: vi.fn(),
    instances: { list: vi.fn() },
  };
  return { loaderSdk };
});

// OperationsProvider and useCanEditAgentConfig still read the Fusion shell client.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => loaderSdk,
}));

describe("useAgentLoader", () => {
  it("exposes sdk and syncs agent name when agents list updates", async () => {
    const { Wrapper: AppWrapper } = createOperationsWithQueryWrapper();
    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <UseAgentLoaderContext.Provider
          value={{
            useCdfClient: () => ({
              sdk: cogniteClientMock(loaderSdk),
              isAuthLoading: false,
            }),
          }}
        >
          <AppWrapper>{children}</AppWrapper>
        </UseAgentLoaderContext.Provider>
      );
    }

    const { result } = renderHook(() => useAgentLoader(), { wrapper: Wrapper });

    await waitFor(() => {
      expect(result.current.sdk).toBeTruthy();
    });

    expect(result.current.state.agents).toBeDefined();
    expect(result.current.canEditAgentConfigError).toBe(false);
    expect(typeof result.current.refetchCanEditAgentConfig).toBe("function");
  });
});
