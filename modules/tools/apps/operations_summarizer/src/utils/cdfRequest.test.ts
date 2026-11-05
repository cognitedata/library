import { describe, expect, it } from "vitest";
import {
  getHttpStatusFromError,
  getRequestIdFromError,
  withCdfConcurrency,
} from "./cdfRequest";

describe(withCdfConcurrency.name, () => {
  it("runs a task on the default runner", async () => {
    await expect(withCdfConcurrency(async () => "ok")).resolves.toBe("ok");
  });

  it("runs a delete task on the delete runner", async () => {
    await expect(
      withCdfConcurrency(async () => 2, { delete: true }),
    ).resolves.toBe(2);
  });
});

describe(getHttpStatusFromError.name, () => {
  it("reads status from the error or a nested response", () => {
    expect(getHttpStatusFromError({ status: 429 })).toBe(429);
    expect(getHttpStatusFromError({ response: { status: 503 } })).toBe(503);
    expect(getHttpStatusFromError({ cause: { status: 404 } })).toBe(404);
    expect(getHttpStatusFromError("nope")).toBeUndefined();
    expect(getHttpStatusFromError(null)).toBeUndefined();
  });
});

describe(getRequestIdFromError.name, () => {
  it("reads a request id from the error, headers, or extra", () => {
    expect(getRequestIdFromError({ requestId: "  " })).toBeUndefined();
    expect(getRequestIdFromError({ requestId: "req-1" })).toBe("req-1");
    expect(
      getRequestIdFromError({
        response: { headers: { "X-Request-Id": "from-header" } },
      }),
    ).toBe("from-header");
    expect(
      getRequestIdFromError({
        headers: { "x-cdf-request-id": ["cdf-1", "ignored"] },
      }),
    ).toBe("cdf-1");
    expect(
      getRequestIdFromError({
        headers: {
          get(name: string) {
            return name === "x-request-id" ? "from-get" : null;
          },
        },
      }),
    ).toBe("from-get");
    expect(
      getRequestIdFromError({
        headers: {
          get() {
            return ["from-get-array"];
          },
        },
      }),
    ).toBe("from-get-array");
    expect(
      getRequestIdFromError({
        extra: { requestId: "extra-id" },
        cause: { extra: { requestId: "   " } },
      }),
    ).toBe("extra-id");
  });

  it("returns undefined when no request id is present", () => {
    expect(getRequestIdFromError({ headers: { other: 1 } })).toBeUndefined();
    expect(getRequestIdFromError(42)).toBeUndefined();
  });
});
