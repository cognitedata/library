import type { CogniteClient, QueryResponse } from "@cognite/sdk";
import type { CDFHelper } from "@/services/cdfHelper";

type VitestFn = ReturnType<typeof import("vitest").vi.fn>;

/**
 * Lets tests pass partial API stubs (`instances.list`, `files.getDownloadUrls`)
 * without implementing every SDK method. Extra keys are overlaid at runtime.
 */
export type CogniteClientMockInput = {
  project: string;
  instances?: Partial<CogniteClient["instances"]>;
} & Record<string, unknown>;

export function cogniteClientMock(
  partial: CogniteClientMockInput,
): CogniteClient {
  return Object.assign(
    partialCogniteClient({ project: partial.project }),
    partial,
  );
}

/** Partial client double. Call sites only stub the APIs the unit touches. */
export function partialCogniteClient(
  overrides: Partial<CogniteClient> = {},
): CogniteClient {
  return { project: "test-project", ...overrides } as CogniteClient;
}

/** `agentChat` is a `vi.fn()`; only that API is exercised in these tests. */
export function cdfHelperMock(stub: { agentChat: VitestFn }): CDFHelper {
  // @ts-expect-error Vitest mock is not structurally a full CDFHelper instance
  return stub as CDFHelper;
}

/** Parser tests use slim node rows that omit full `NodeDefinition` fields. */
export function queryResponseMock(body: {
  items: Record<string, unknown[]>;
  nextCursor?: QueryResponse["nextCursor"];
}): QueryResponse {
  return body as QueryResponse;
}

/** Streaming tests only stub `instances.list`. */
export function cogniteClientInstancesMock(body: {
  instances: { list: unknown };
}): Pick<CogniteClient, "instances"> {
  return body as Pick<CogniteClient, "instances">;
}
