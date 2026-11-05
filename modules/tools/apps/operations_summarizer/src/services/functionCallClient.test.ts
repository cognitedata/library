import { beforeAll, describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig, setAppConfig } from "@/config/appConfig";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import {
  CogniteFunctionCallClient,
  type FunctionCallClientOptions,
} from "./functionCallClient";

beforeAll(() => {
  setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
});

describe("CogniteFunctionCallClient", () => {
  it("returns the function response after a running call completes", async () => {
    const logs: string[] = [];
    const statuses: string[] = [];
    const { client, get } = clientWith({
      polls: [{ status: "Running" }, { status: "Completed" }],
      logs: [{ message: "line-1" }, { message: "" }, { message: "line-2" }],
      response: { response: { ok: true } },
    });
    const calls = new CogniteFunctionCallClient(client, options());
    const result = await calls.call(
      { prompt: "hi" },
      {
        onStatus: (status) => statuses.push(status),
        onLog: (message) => logs.push(message),
      },
    );
    expect(result).toEqual({ ok: true });
    expect(statuses).toEqual(["Running", "Running", "Completed"]);
    expect(logs).toEqual(["line-1", "line-2"]);
    expect(get).toHaveBeenCalled();
  });

  it("throws when the function external id is missing", async () => {
    const { client, retrieve } = clientWith({});
    retrieve.mockResolvedValue([]);
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).rejects.toThrow(
      "Podcast function not found: fn-ext",
    );
  });

  it("falls back to a one-shot session when token exchange fails", async () => {
    const { client, sessions } = clientWith({
      polls: [{ status: "Completed" }],
      response: { response: 1 },
    });
    sessions.mockRejectedValueOnce(new Error("no exchange"));
    sessions.mockResolvedValueOnce([{ nonce: "once" }]);
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).resolves.toBe(1);
    expect(sessions).toHaveBeenCalledTimes(2);
  });

  it("falls back when token exchange returns no nonce", async () => {
    const { client, sessions } = clientWith({
      polls: [{ status: "Completed" }],
      response: { response: 2 },
    });
    sessions.mockResolvedValueOnce([{}]);
    sessions.mockResolvedValueOnce([{ nonce: "once" }]);
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).resolves.toBe(2);
  });

  it("throws when neither session returns a nonce", async () => {
    const { client, sessions } = clientWith({});
    sessions.mockResolvedValue([{}]);
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).rejects.toThrow(
      "Failed to create a CDF session for the podcast function",
    );
  });

  it("throws when the call start has no id", async () => {
    const { client, post } = clientWith({});
    post.mockResolvedValue({ data: {} });
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).rejects.toThrow(
      "Podcast function call did not return a call id",
    );
  });

  it("throws the function error when the call fails or times out", async () => {
    const failed = clientWith({
      polls: [{ status: "Failed", error: "bad input" }],
    });
    await expect(
      new CogniteFunctionCallClient(failed.client, options()).call({}),
    ).rejects.toThrow("bad input");

    const timed = clientWith({ polls: [{ status: "Timeout" }] });
    await expect(
      new CogniteFunctionCallClient(timed.client, options()).call({}),
    ).rejects.toThrow("Podcast function call timeout");
  });

  it("retries transient poll errors and then surfaces a string error", async () => {
    const { client, get } = clientWith({
      polls: [{ status: "Completed" }],
    });
    get
      .mockRejectedValueOnce(new Error("blip"))
      .mockRejectedValueOnce("offline");
    const calls = new CogniteFunctionCallClient(
      client,
      options({ maxConsecutivePollErrors: 2 }),
    );
    await expect(calls.call({})).rejects.toThrow(
      "Failed to retrieve podcast function status after 2 consecutive attempts: offline",
    );
  });

  it("rethrows an abort raised while retrieving the call", async () => {
    const { client, get } = clientWith({});
    get.mockRejectedValue(
      new DOMException("The operation was aborted", "AbortError"),
    );
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({})).rejects.toMatchObject({ name: "AbortError" });
  });

  it("stops before starting when the signal is already aborted", async () => {
    const { client, retrieve } = clientWith({});
    const controller = new AbortController();
    controller.abort();
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(
      calls.call({}, { signal: controller.signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(retrieve).not.toHaveBeenCalled();
  });

  it("stops between polls when the signal aborts", async () => {
    const controller = new AbortController();
    const { client } = clientWith({
      polls: [{ status: "Running" }],
      onPoll: () => controller.abort(),
    });
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(
      calls.call({}, { signal: controller.signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
  });

  it("times out when the deadline passes before completion", async () => {
    vi.spyOn(Date, "now").mockReturnValueOnce(1_000).mockReturnValue(5_000);
    const { client } = clientWith({});
    const calls = new CogniteFunctionCallClient(
      client,
      options({ timeoutMs: 1_000 }),
    );
    await expect(calls.call({})).rejects.toThrow(
      "Podcast function call timed out after 1s",
    );
    vi.mocked(Date.now).mockRestore();
  });

  it("keeps polling when log retrieval fails", async () => {
    const { client, get } = clientWith({
      polls: [{ status: "Completed" }],
      response: { response: "done" },
    });
    get.mockImplementation(async (url: string) => {
      if (url.endsWith("/logs")) throw new Error("logs down");
      if (url.endsWith("/response")) return { data: { response: "done" } };
      return { data: { id: 4, status: "Completed", functionId: 9 } };
    });
    const onLog = vi.fn();
    const calls = new CogniteFunctionCallClient(client, options());
    await expect(calls.call({}, { onLog })).resolves.toBe("done");
    expect(onLog).not.toHaveBeenCalled();
  });
});

function options(
  overrides: Partial<FunctionCallClientOptions> = {},
): FunctionCallClientOptions {
  return {
    functionExternalId: "fn-ext",
    pollIntervalMs: 1,
    timeoutMs: 60_000,
    maxConsecutivePollErrors: 3,
    label: "Podcast function",
    sleep: async () => {},
    ...overrides,
  };
}

function clientWith(opts: {
  polls?: { status: string; error?: string }[];
  logs?: { message?: string }[];
  response?: { response?: unknown };
  onPoll?: () => void;
}) {
  const retrieve = vi.fn().mockResolvedValue([{ id: 9 }]);
  const sessions = vi.fn().mockResolvedValue([{ nonce: "nonce-1" }]);
  const post = vi.fn().mockResolvedValue({
    data: { id: 4, status: "Running", functionId: 9 },
  });
  const polls = [...(opts.polls ?? [])];
  const get = vi.fn(async (url: string) => {
    if (url.endsWith("/logs")) {
      return { data: { items: opts.logs ?? [] } };
    }
    if (url.endsWith("/response")) {
      return { data: opts.response ?? { response: null } };
    }
    opts.onPoll?.();
    const next = polls.shift() ?? { status: "Completed" };
    return { data: { id: 4, functionId: 9, ...next } };
  });
  const client = cogniteClientMock({
    project: "demo",
    functions: { retrieve },
    sessions: { create: sessions },
    post,
    get,
  });
  return { client, retrieve, sessions, post, get };
}
