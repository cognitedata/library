import { describe, expect, it, vi } from "vitest";
import type * as CdfRequest from "@/utils/cdfRequest";
import {
  CDFHelper,
  CogniteApiException,
  extractDateRangeFromSummary,
} from "./cdfHelper";

// withCdfConcurrency waits on a shared queue. Tests run the task immediately.
vi.mock("@/utils/cdfRequest", async (importOriginal) => {
  const actual = await importOriginal<typeof CdfRequest>();
  return {
    withCdfConcurrency: <T>(fn: () => Promise<T>) => fn(),
    getHttpStatusFromError: actual.getHttpStatusFromError,
    getRequestIdFromError: actual.getRequestIdFromError,
  };
});

type MockClient = {
  project: string;
  get: ReturnType<typeof vi.fn>;
  post: ReturnType<typeof vi.fn>;
};

function createClient(overrides: Partial<MockClient> = {}): MockClient {
  return {
    project: "my-project",
    get: vi.fn(),
    post: vi.fn(),
    ...overrides,
  };
}

describe("extractDateRangeFromSummary", () => {
  it("extracts start and end date from ops_summary_{start}_{end}_{timestamp}", () => {
    const result = extractDateRangeFromSummary(
      "ops_summary_2024-01-01_2024-01-31_1704067200000",
    );
    expect(result.startDate).toBe("2024-01-01");
    expect(result.endDate).toBe("2024-01-31");
  });

  it("returns nulls when fewer than 4 parts", () => {
    expect(extractDateRangeFromSummary("ops_summary")).toEqual({
      startDate: null,
      endDate: null,
    });
  });
});

describe("CDFHelper", () => {
  it("getTokenInspect uses GET without alpha header path", async () => {
    const client = createClient();
    client.get.mockResolvedValue({
      data: { capabilities: [] },
    });
    const helper = new CDFHelper(client as never);
    const res = await helper.getTokenInspect();
    expect(res).toEqual({ capabilities: [] });
    expect(client.get).toHaveBeenCalledWith("/api/v1/token/inspect", {
      headers: {},
    });
  });

  it("canEditAgentConfig returns true when WRITE matches project", async () => {
    const client = createClient();
    client.get.mockResolvedValue({
      data: {
        capabilities: [
          {
            agentsAcl: { actions: ["WRITE"] },
            projectScope: { projects: ["my-project"] },
          },
        ],
      },
    });
    const helper = new CDFHelper(client as never);
    await expect(helper.canEditAgentConfig()).resolves.toBe(true);
  });

  it("canEditAgentConfig returns false on API error", async () => {
    const client = createClient();
    client.get.mockRejectedValue(new Error("token inspect failed"));
    const helper = new CDFHelper(client as never);
    await expect(helper.canEditAgentConfig()).resolves.toBe(false);
  });

  it("getAgents maps items and invalidAgents", async () => {
    const client = createClient();
    client.get.mockResolvedValue({
      data: {
        items: [
          {
            externalId: "a1",
            name: "Agent One",
            description: "d",
            model: "m",
            instructions: "be helpful",
          },
        ],
        invalidAgents: ["bad"],
      },
    });
    const helper = new CDFHelper(client as never);
    const { agents, invalidAgents } = await helper.getAgents();
    expect(agents).toEqual([
      {
        externalId: "a1",
        name: "Agent One",
        description: "d",
        model: "m",
        instructions: "be helpful",
      },
    ]);
    expect(invalidAgents).toEqual(["bad"]);
  });

  it("getSupportedModelNames returns sorted model names", async () => {
    const client = createClient();
    client.get.mockResolvedValue({
      data: {
        languageModels: [{ name: "b" }, { name: "a" }],
      },
    });
    const helper = new CDFHelper(client as never);
    await expect(helper.getSupportedModelNames()).resolves.toEqual(["a", "b"]);
  });

  it("getSupportedModelNames returns fallback when request fails", async () => {
    const client = createClient();
    client.get.mockRejectedValue(new Error("fail"));
    const helper = new CDFHelper(client as never);
    const models = await helper.getSupportedModelNames();
    expect(models).toEqual(["cognite/balanced"]);
  });

  it("createAgent posts payload and returns externalId", async () => {
    const client = createClient();
    client.post.mockResolvedValue({ data: {} });
    const helper = new CDFHelper(client as never);
    const id = await helper.createAgent({
      externalId: "ext",
      name: "N",
      description: "D",
      instructions: "I",
      model: "M",
      tools: [],
    });
    expect(id).toBe("ext");
    expect(client.post).toHaveBeenCalled();
  });

  it("agentChat maps string content and optional cursor", async () => {
    const client = createClient();
    client.post.mockResolvedValue({
      data: { response: { messages: [] } },
    });
    const helper = new CDFHelper(client as never);
    await helper.agentChat("agent-1", [{ role: "user", content: "hi" }], "cur");
    const [, options] = client.post.mock.calls[0];
    expect(options.data).toMatchObject({
      agentId: "agent-1",
      cursor: "cur",
    });
  });

  it("chatCompletion uses non-alpha request", async () => {
    const client = createClient();
    client.post.mockResolvedValue({
      data: { choices: [{ message: { content: "ok" } }] },
    });
    const helper = new CDFHelper(client as never);
    const res = await helper.chatCompletion([
      { role: "user", content: "hello" },
    ]);
    expect(res.choices?.[0]?.message?.content).toBe("ok");
    const url = client.post.mock.calls[0][0] as string;
    expect(url).toContain("/ai/chat/completions");
  });

  it("compareSummaries delegates to chatCompletion", async () => {
    const client = createClient();
    client.post.mockResolvedValue({
      data: { choices: [{ message: { content: "comparison" } }] },
    });
    const helper = new CDFHelper(client as never);
    const res = await helper.compareSummaries("A summary", "B summary");
    expect(res.choices?.[0]?.message?.content).toBe("comparison");
    const payload = client.post.mock.calls[0][1] as {
      data: { messages: Array<{ content: string }> };
    };
    expect(payload.data.messages[0].content).toContain("A summary");
    expect(payload.data.messages[0].content).toContain("B summary");
  });

  it("sendCogAiRequest throws CogniteApiException on failure", async () => {
    const client = createClient();
    client.get.mockRejectedValue(new Error("boom"));
    const helper = new CDFHelper(client as never);
    await expect(helper.getTokenInspect()).rejects.toThrow("boom");
  });

  it("sendCogAiRequest preserves HTTP status, request id, and cause on SDK-shaped errors", async () => {
    const client = createClient();
    const inner = {
      status: 403,
      requestId: "req-forbidden",
      message: "Forbidden",
    };
    client.get.mockRejectedValue(inner);
    const helper = new CDFHelper(client as never);
    const thrown = await helper.getTokenInspect().catch((e: unknown) => e);
    expect(thrown).toBeInstanceOf(CogniteApiException);
    const err = thrown as CogniteApiException;
    expect(err.message).toBe("Forbidden");
    expect(err.status).toBe(403);
    expect(err.requestId).toBe("req-forbidden");
    expect(err.cause).toBe(inner);
  });
});
