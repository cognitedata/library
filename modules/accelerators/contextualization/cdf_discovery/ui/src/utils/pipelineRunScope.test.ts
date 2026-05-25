import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  defaultPipelineRunScope,
  pipelineRunScopeStorageKey,
  readStoredPipelineRunScope,
  resolvePipelineRunScope,
  storePipelineRunScope,
} from "./pipelineRunScope";

const RESOURCE = "test_pipeline";

function installSessionStorageMock(): void {
  const store = new Map<string, string>();
  vi.stubGlobal("sessionStorage", {
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => {
      store.clear();
    },
  });
}

beforeEach(() => {
  installSessionStorageMock();
});

afterEach(() => {
  sessionStorage.removeItem(pipelineRunScopeStorageKey(RESOURCE));
  vi.unstubAllGlobals();
});

describe("pipelineRunScope", () => {
  it("defaults to incremental when pipeline parameters enable incremental processing", () => {
    expect(
      defaultPipelineRunScope({ incremental_change_processing: true })
    ).toBe("incremental");
    expect(defaultPipelineRunScope({ incremental: true })).toBe("incremental");
    expect(defaultPipelineRunScope({ incremental: false })).toBe("all");
  });

  it("persists and restores run scope per resource", () => {
    expect(readStoredPipelineRunScope(RESOURCE)).toBeNull();
    storePipelineRunScope(RESOURCE, "incremental");
    expect(readStoredPipelineRunScope(RESOURCE)).toBe("incremental");
    expect(resolvePipelineRunScope(RESOURCE, { incremental: false })).toBe("incremental");
  });
});
