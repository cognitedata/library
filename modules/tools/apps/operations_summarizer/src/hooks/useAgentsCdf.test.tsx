import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig, setAppConfig } from "@/config/appConfig";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import { createTestQueryClient } from "@/test-utils/wrappers";
import {
  UseAgentsCdfContext,
  UseEnsureDefaultAgentContext,
  useAgents,
  useEnsureDefaultAgent,
} from "./useAgentsCdf";

const { mockSdk } = vi.hoisted(() => {
  const get = vi.fn();
  const post = vi.fn();
  return {
    mockSdk: {
      project: "test-proj",
      get,
      post,
      instances: {
        list: vi.fn().mockResolvedValue({ items: [] }),
        delete: vi.fn(),
        upsert: vi.fn(),
      },
    },
  };
});

function createAgentsWrapper() {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <UseAgentsCdfContext.Provider
          value={{
            useCdfClient: () => ({
              sdk: cogniteClientMock(mockSdk),
              isAuthLoading: false,
            }),
          }}
        >
          {children}
        </UseAgentsCdfContext.Provider>
      </QueryClientProvider>
    );
  }
  return { Wrapper };
}

describe("useAgentsCdf", () => {
  it("useAgents loads agents via CDFHelper", async () => {
    mockSdk.get.mockResolvedValue({
      data: {
        items: [
          {
            externalId: "ag1",
            name: "Bot",
            description: "d",
            model: "m",
          },
        ],
        invalidAgents: [],
      },
    });

    const { Wrapper } = createAgentsWrapper();

    const { result } = renderHook(() => useAgents(), { wrapper: Wrapper });

    await waitFor(() => {
      expect(result.current.data?.length).toBe(1);
      expect(result.current.data?.[0].externalId).toBe("ag1");
    });
  });

  it("useEnsureDefaultAgent returns existing agent id", async () => {
    mockSdk.get.mockResolvedValue({
      data: {
        items: [
          {
            externalId: "existing",
            name: "Operation Summarizer Agent",
            description: "",
            model: "m",
          },
        ],
        invalidAgents: [],
      },
    });

    const { Wrapper } = createAgentsWrapper();

    const { result: agentsResult } = renderHook(() => useAgents(), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(agentsResult.current.isSuccess).toBe(true));

    const { result } = renderHook(() => useEnsureDefaultAgent(), {
      wrapper: Wrapper,
    });

    const id = await result.current.mutateAsync();
    expect(id).toBe("existing");
    expect(mockSdk.post).not.toHaveBeenCalled();
  });

  it("creates the default agent from config values and does not overwrite it", async () => {
    const config = parseAppConfig(DEFAULT_NODE_PROPERTIES);
    const distinctive = {
      ...config,
      agent: {
        ...config.agent,
        name: "Distinct Agent",
        externalIdSuffix: "_distinct_suffix",
        defaultLanguageModel: "azure/distinct-model",
      },
      prompts: {
        ...config.prompts,
        agentInstructions: "DISTINCT INSTRUCTIONS",
      },
    };
    setAppConfig(distinctive);

    mockSdk.get.mockImplementation((url: string) => {
      const path = String(url);
      if (path.includes("/ai/agents") && !path.includes("availability")) {
        return Promise.resolve({ data: { items: [], invalidAgents: [] } });
      }
      if (path.includes("token/inspect")) {
        return Promise.resolve({
          data: {
            capabilities: [
              {
                agentsAcl: { actions: ["WRITE"] },
                projectScope: { projects: ["test-proj"] },
              },
            ],
          },
        });
      }
      if (path.includes("services/availability")) {
        return Promise.resolve({
          data: { languageModels: [{ name: "azure/distinct-model" }] },
        });
      }
      return Promise.resolve({ data: {} });
    });
    mockSdk.post.mockResolvedValue({ data: {} });

    const { Wrapper } = createAgentsWrapper();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <UseEnsureDefaultAgentContext.Provider
        value={{ readConfig: () => distinctive }}
      >
        <Wrapper>{children}</Wrapper>
      </UseEnsureDefaultAgentContext.Provider>
    );

    const { result } = renderHook(() => useEnsureDefaultAgent(), { wrapper });
    let createdId = "";
    await act(async () => {
      createdId = await result.current.mutateAsync();
    });

    expect(createdId).toBe("test-proj_distinct_suffix");
    expect(mockSdk.post).toHaveBeenCalledWith(
      expect.stringContaining("/ai/agents"),
      expect.objectContaining({
        data: expect.objectContaining({
          items: [
            expect.objectContaining({
              name: "Distinct Agent",
              externalId: "test-proj_distinct_suffix",
              instructions: "DISTINCT INSTRUCTIONS",
              model: "azure/distinct-model",
            }),
          ],
        }),
      }),
    );

    mockSdk.get.mockImplementation((url: string) => {
      const path = String(url);
      if (path.includes("/ai/agents")) {
        return Promise.resolve({
          data: {
            items: [
              {
                externalId: "test-proj_distinct_suffix",
                name: "Distinct Agent",
                description: "",
                model: "azure/distinct-model",
                instructions: "DISTINCT INSTRUCTIONS",
              },
            ],
            invalidAgents: [],
          },
        });
      }
      return Promise.resolve({ data: {} });
    });
    mockSdk.post.mockClear();

    await act(async () => {
      const again = await result.current.mutateAsync();
      expect(again).toBe("test-proj_distinct_suffix");
    });
    expect(mockSdk.post).not.toHaveBeenCalled();
    setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
  });

  it("rejects empty agent instructions", async () => {
    const config = parseAppConfig(DEFAULT_NODE_PROPERTIES);
    const empty = {
      ...config,
      prompts: { ...config.prompts, agentInstructions: "   " },
    };
    const { Wrapper } = createAgentsWrapper();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <UseEnsureDefaultAgentContext.Provider
        value={{ readConfig: () => empty }}
      >
        <Wrapper>{children}</Wrapper>
      </UseEnsureDefaultAgentContext.Provider>
    );
    const { result } = renderHook(() => useEnsureDefaultAgent(), { wrapper });
    await expect(result.current.mutateAsync()).rejects.toThrow(
      /agent instructions are empty/i,
    );
  });

  it("falls back to the first supported model when the configured model is missing", async () => {
    const config = parseAppConfig(DEFAULT_NODE_PROPERTIES);
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    mockSdk.get.mockImplementation((url: string) => {
      const path = String(url);
      if (path.includes("/ai/agents") && !path.includes("availability")) {
        return Promise.resolve({ data: { items: [], invalidAgents: [] } });
      }
      if (path.includes("token/inspect")) {
        return Promise.resolve({
          data: {
            capabilities: [
              {
                agentsAcl: { actions: ["WRITE"] },
                projectScope: { projects: ["test-proj"] },
              },
            ],
          },
        });
      }
      if (path.includes("services/availability")) {
        return Promise.resolve({
          data: { languageModels: [{ name: "azure/fallback-model" }] },
        });
      }
      return Promise.resolve({ data: {} });
    });
    mockSdk.post.mockResolvedValue({ data: {} });
    const { Wrapper } = createAgentsWrapper();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <UseEnsureDefaultAgentContext.Provider
        value={{ readConfig: () => config }}
      >
        <Wrapper>{children}</Wrapper>
      </UseEnsureDefaultAgentContext.Provider>
    );
    const { result } = renderHook(() => useEnsureDefaultAgent(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync();
    });
    expect(mockSdk.post).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        data: expect.objectContaining({
          items: [expect.objectContaining({ model: "azure/fallback-model" })],
        }),
      }),
    );
    expect(info).toHaveBeenCalled();
    info.mockRestore();
  });
});
