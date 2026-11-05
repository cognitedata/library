import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import {
  SummaryGenerationContext,
  useSummaryGenerationContext,
} from "@/contexts/operations-summarizer/summaryGenerationContext";
import {
  pickSummaryGenerationSession,
  type SummaryGenerationContextValue,
} from "@/contexts/operations-summarizer/summaryGenerationSlice";
import { createInitialOperationsState } from "@/contexts/operations-summarizer/types";

describe("useSummaryGenerationContext", () => {
  it("throws when rendered outside OperationsProvider", () => {
    expect(() => renderHook(() => useSummaryGenerationContext())).toThrow(
      "useSummaryGenerationContext must be used within OperationsProvider",
    );
  });

  it("returns summary generation state and actions from its provider", () => {
    const toggleSummaryContent = vi.fn();
    const resetSummaryState = vi.fn();
    const value: SummaryGenerationContextValue = {
      ...pickSummaryGenerationSession(createInitialOperationsState()),
      dispatch: vi.fn(),
      setSummaryPeriod: vi.fn(),
      toggleSummaryContent,
      resetSummaryState,
    };
    const wrapper = ({ children }: { children: ReactNode }) => (
      <SummaryGenerationContext.Provider value={value}>
        {children}
      </SummaryGenerationContext.Provider>
    );

    const { result } = renderHook(() => useSummaryGenerationContext(), {
      wrapper,
    });

    expect(result.current.isGenerating).toBe(false);
    expect(result.current.generationError).toBeNull();
    expect(result.current.summaryContents.assets).toBe(true);
    act(() => {
      result.current.toggleSummaryContent("files");
      result.current.resetSummaryState();
    });
    expect(toggleSummaryContent).toHaveBeenCalledWith("files");
    expect(resetSummaryState).toHaveBeenCalledOnce();
  });
});
