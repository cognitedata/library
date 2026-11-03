import { beforeAll, describe, expect, it } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig, setAppConfig } from "@/config/appConfig";
import { getCdfDeleteTaskRunner, getCdfTaskRunner } from "./semaphore";

beforeAll(() => {
  const raw = structuredClone(DEFAULT_NODE_PROPERTIES) as {
    limits: { concurrency: { default: number; delete: number } };
  };
  raw.limits.concurrency.default = 1;
  raw.limits.concurrency.delete = 1;
  setAppConfig(parseAppConfig(raw));
});

describe("getCdfTaskRunner", () => {
  it("returns the same runner and runs a task", async () => {
    const runner = getCdfTaskRunner();
    expect(getCdfTaskRunner()).toBe(runner);
    await expect(runner.schedule(async () => 7)).resolves.toBe(7);
  });

  it("rejects when the task throws", async () => {
    const runner = getCdfTaskRunner();
    await expect(
      runner.schedule(async () => {
        throw new Error("cdf down");
      }),
    ).rejects.toThrow("cdf down");
  });

  it("holds queued work until a slot frees, and drops a cleared queue", async () => {
    const runner = getCdfTaskRunner();
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const started: string[] = [];
    const first = runner.schedule(async () => {
      started.push("first");
      await gate;
      return "first";
    });
    let secondStarted = false;
    const second = runner.schedule(async () => {
      secondStarted = true;
      return "second";
    });
    await flush();
    expect(started).toEqual(["first"]);
    expect(secondStarted).toBe(false);

    runner.clearQueue();
    release();
    await expect(first).resolves.toBe("first");
    await flush();
    expect(secondStarted).toBe(false);
    void second;
  });

  it("aborts a pending task that shares a key", async () => {
    const runner = getCdfTaskRunner();
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const blocking = runner.schedule(async () => {
      await gate;
      return "block";
    });
    const replaced = runner.schedule(async () => "old", { key: "same" });
    const replacedResult = replaced.then(
      () => "resolved",
      (error: unknown) => error,
    );
    const replacement = runner.schedule(async () => "new", { key: "same" });
    await flush();
    await expect(replacedResult).resolves.toMatchObject({ name: "AbortError" });

    release();
    await expect(blocking).resolves.toBe("block");
    await expect(replacement).resolves.toBe("new");
  });
});

describe("getCdfDeleteTaskRunner", () => {
  it("is a separate singleton from the default runner", async () => {
    const deletes = getCdfDeleteTaskRunner();
    expect(deletes).not.toBe(getCdfTaskRunner());
    expect(getCdfDeleteTaskRunner()).toBe(deletes);
    await expect(deletes.schedule(async () => "gone")).resolves.toBe("gone");
  });
});

function flush(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}
