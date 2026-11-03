import { describe, expect, it, vi } from "vitest";
import { getAppConfig } from "@/config/appConfig";
import { computeUserRetryDelayMs, sleep } from "./cdfRetry";

describe(sleep.name, () => {
  it("resolves after the delay", async () => {
    vi.useFakeTimers();
    const pending = sleep(25);
    await vi.advanceTimersByTimeAsync(25);
    await pending;
    vi.useRealTimers();
  });
});

describe(computeUserRetryDelayMs.name, () => {
  it("honors a Retry-After header in seconds", () => {
    expect(
      computeUserRetryDelayMs({
        attempt: 0,
        error: { response: { headers: { "Retry-After": "2" } } },
      }),
    ).toBe(2000);
  });

  it("reads retry-after from the error headers object", () => {
    expect(
      computeUserRetryDelayMs({
        attempt: 1,
        error: { headers: { "x-retry-after": "1.5" } },
      }),
    ).toBe(1500);
  });

  it("caps a large Retry-After at the configured max delay", () => {
    const maxDelayMs = getAppConfig().limits.retry.maxDelayMs;
    expect(
      computeUserRetryDelayMs({
        attempt: 0,
        error: { headers: { "retry-after": "99999" } },
      }),
    ).toBe(maxDelayMs);
  });

  it("uses exponential backoff with jitter when Retry-After is missing or invalid", () => {
    const baseMs = 1000;
    vi.spyOn(Math, "random").mockReturnValue(1);
    const delay = computeUserRetryDelayMs({
      attempt: 2,
      baseMs,
      error: { headers: { "retry-after": "0" } },
    });
    expect(delay).toBe(baseMs * 2 ** 2);
    vi.mocked(Math.random).mockReturnValue(0);
    expect(computeUserRetryDelayMs({ attempt: 1, baseMs, error: null })).toBe(
      baseMs,
    );
    vi.restoreAllMocks();
  });

  it("caps exponential delay at the configured maximum", () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    const maxDelayMs = getAppConfig().limits.retry.maxDelayMs;
    expect(computeUserRetryDelayMs({ attempt: 20, baseMs: 1000 })).toBe(
      maxDelayMs,
    );
    vi.restoreAllMocks();
  });
});
