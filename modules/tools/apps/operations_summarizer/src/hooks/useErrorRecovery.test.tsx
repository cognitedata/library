import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { OperationsProvider } from "@/contexts/OperationsSummarizerContext";
import { useErrorRecovery } from "./useErrorRecovery";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => null,
}));

function wrapper({ children }: { children: ReactNode }) {
  return (
    <CdfClientProvider>
      <OperationsProvider>{children}</OperationsProvider>
    </CdfClientProvider>
  );
}

describe("useErrorRecovery", () => {
  it("handleError for summary-generation updates recoverable state", () => {
    const { result } = renderHook(() => useErrorRecovery(), { wrapper });

    act(() => {
      result.current.handleError(
        new Error("network timeout"),
        "summary-generation",
      );
    });

    expect(result.current.errorMessage).toBeTruthy();
    expect(result.current.hasRecoverableError).toBe(true);
    expect(result.current.canRetry).toBe(true);
  });

  it("recoverFromError clears summary-generation context flags", () => {
    const { result } = renderHook(() => useErrorRecovery(), { wrapper });

    act(() => {
      result.current.handleError(new Error("x"), "summary-generation");
    });
    act(() => {
      result.current.recoverFromError();
    });

    expect(result.current.errorMessage).toBeNull();
  });

  it("retryLastOperation throws when not recoverable", async () => {
    const { result } = renderHook(() => useErrorRecovery(), { wrapper });

    act(() => {
      result.current.handleError(new Error("400 validation"), "other-op");
    });

    await act(async () => {
      await expect(result.current.retryLastOperation()).rejects.toThrow(
        "No retryable operation available",
      );
    });
  });

  it("clearError resets local error", () => {
    const { result } = renderHook(() => useErrorRecovery(), { wrapper });

    act(() => {
      result.current.handleError(new Error("e"), "other-op");
    });
    act(() => {
      result.current.clearError();
    });

    expect(result.current.errorMessage).toBeNull();
  });

  it("429 is recoverable for non-summary operations", () => {
    const { result } = renderHook(() => useErrorRecovery(), { wrapper });

    act(() => {
      result.current.handleError({ status: 429 }, "fetch-data");
    });

    expect(result.current.hasRecoverableError).toBe(true);
    expect(result.current.canRetry).toBe(true);
  });

  it("retryLastOperation waits using backoff before resolving", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);

    const { result } = renderHook(
      () => useErrorRecovery({ initialBackoffMs: 1000 }),
      { wrapper },
    );

    act(() => {
      result.current.handleError(
        new Error("network timeout"),
        "summary-generation",
      );
    });

    await act(async () => {
      const p = result.current.retryLastOperation();
      await vi.advanceTimersByTimeAsync(499);
      const race = Promise.race([
        p.then(() => "done"),
        Promise.resolve().then(() => "tick"),
      ]);
      expect(await race).toBe("tick");
      await vi.advanceTimersByTimeAsync(2);
      await p;
    });

    vi.restoreAllMocks();
    vi.useRealTimers();
  });
});
