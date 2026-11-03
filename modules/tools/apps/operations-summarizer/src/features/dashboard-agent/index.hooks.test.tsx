import { renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { getAppConfig } from "@/config/appConfig";
import { createOperationsWithQueryWrapper } from "@/test-utils/wrappers";
import { useAgentLoader } from "./index";

const helperApi = vi.hoisted(() => ({
  getAgents: vi.fn(),
  canEditAgentConfig: vi.fn(),
}));

// useAgentLoader constructs CDFHelper. The hook test stubs the class so the agents API is not called.
vi.mock("@/services/cdfHelper", () => ({
  CDFHelper: class {
    getAgents = helperApi.getAgents;
    canEditAgentConfig = helperApi.canEditAgentConfig;
  },
}));

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({
    project: "proj-x",
    get: vi.fn(),
    post: vi.fn(),
    instances: { list: vi.fn() },
  }),
}));

describe("dashboard agent hooks", () => {
  it("loads edit permission for the settings view", async () => {
    helperApi.canEditAgentConfig.mockResolvedValue(true);
    helperApi.getAgents.mockResolvedValue({
      agents: [
        {
          externalId: "helper-1",
          name: getAppConfig().agent.name,
          instructions: "Keep it short",
        },
      ],
    });
    const { Wrapper } = createOperationsWithQueryWrapper();
    const { result } = renderHook(() => useAgentLoader(), { wrapper: Wrapper });
    await waitFor(() => {
      expect(result.current.canEditAgentConfig).toBe(true);
    });
    expect(result.current.canEditAgentConfigError).toBe(false);
  });
});
