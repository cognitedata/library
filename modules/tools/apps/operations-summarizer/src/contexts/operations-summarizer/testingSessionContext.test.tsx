import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import {
  TestingSessionContext,
  useTestingSessionContext,
} from "@/contexts/operations-summarizer/testingSessionContext";
import {
  pickTestingSession,
  type TestingSessionContextValue,
} from "@/contexts/operations-summarizer/testingSessionSlice";
import { createInitialOperationsState } from "@/contexts/operations-summarizer/types";

describe("useTestingSessionContext", () => {
  it("throws when rendered outside OperationsProvider", () => {
    expect(() => renderHook(() => useTestingSessionContext())).toThrow(
      "useTestingSessionContext must be used within OperationsProvider",
    );
  });

  it("returns an idle testing session from its provider", () => {
    const resetTestState = vi.fn();
    const value: TestingSessionContextValue = {
      ...pickTestingSession(createInitialOperationsState()),
      dispatch: vi.fn(),
      resetTestState,
    };
    const wrapper = ({ children }: { children: ReactNode }) => (
      <TestingSessionContext.Provider value={value}>
        {children}
      </TestingSessionContext.Provider>
    );

    const { result } = renderHook(() => useTestingSessionContext(), {
      wrapper,
    });

    expect(result.current.isTesting).toBe(false);
    expect(result.current.testError).toBeNull();
    expect(result.current.testResults).toBeNull();
    act(() => {
      result.current.resetTestState();
    });
    expect(resetTestState).toHaveBeenCalledOnce();
  });
});
