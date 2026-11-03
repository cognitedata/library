import type { CogniteClient } from "@cognite/sdk";
import { vi } from "vitest";

export type InstancesListResult = {
  items: unknown[];
  nextCursor?: string;
};

function mockInstancesApi(
  listImpl: (...args: unknown[]) => unknown,
): CogniteClient["instances"] {
  const api = {
    list: vi.fn().mockImplementation((...args: unknown[]) => listImpl(...args)),
    search: vi.fn().mockResolvedValue({ items: [] }),
    query: vi.fn().mockResolvedValue({ items: {} }),
    retrieve: vi.fn().mockResolvedValue({ items: [] }),
    aggregate: vi.fn().mockResolvedValue({ items: [] }),
    upsert: vi.fn().mockResolvedValue({ items: [] }),
    delete: vi.fn().mockResolvedValue({}),
  };
  // @ts-expect-error Vitest stubs subset of InstancesAPI (list + no-op methods)
  return api as CogniteClient["instances"];
}

/**
 * Minimal mock SDK for Vitest: override `instances.list` (and other methods) per test.
 */
export function createMockSdk(
  overrides: Partial<{
    instancesList: (
      ...args: unknown[]
    ) => Promise<InstancesListResult> | InstancesListResult;
    get: (...args: unknown[]) => Promise<unknown>;
    post: (...args: unknown[]) => Promise<unknown>;
    project: string;
  }> = {},
): Pick<CogniteClient, "instances" | "get" | "post"> & { project: string } {
  const instancesList =
    overrides.instancesList ??
    (async () => ({ items: [] }) satisfies InstancesListResult);

  return {
    project: overrides.project ?? "test-project",
    get:
      overrides.get ??
      vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: overrides.post ?? vi.fn().mockResolvedValue({ data: {} }),
    instances: mockInstancesApi((...args: unknown[]) => {
      const out = instancesList(...args);
      return Promise.resolve(out);
    }),
  } as Pick<CogniteClient, "instances" | "get" | "post"> & { project: string };
}
