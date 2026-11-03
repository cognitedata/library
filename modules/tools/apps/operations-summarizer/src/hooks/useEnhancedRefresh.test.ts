import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useEnhancedRefresh } from "./useEnhancedRefresh";

describe("useEnhancedRefresh", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("runs onRefresh and shows success then clears", async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() =>
      useEnhancedRefresh({
        onRefresh,
        feedbackDuration: 100,
      }),
    );

    await act(async () => {
      await result.current.refresh();
    });

    expect(onRefresh).toHaveBeenCalled();
    expect(result.current.showSuccess).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(150);
    });

    expect(result.current.showSuccess).toBe(false);
  });

  it("sets showError when onRefresh rejects", async () => {
    const onRefresh = vi.fn().mockRejectedValue(new Error("fail"));
    const { result } = renderHook(() =>
      useEnhancedRefresh({
        onRefresh,
        feedbackDuration: 50,
        errorMessage: "Custom fail",
      }),
    );

    await act(async () => {
      await result.current.refresh();
    });

    expect(result.current.showError).toBe(true);
    expect(result.current.message.length).toBeGreaterThan(0);

    await act(async () => {
      vi.advanceTimersByTime(100);
    });
  });
});
