import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { createOperationsWithQueryWrapper } from "@/test-utils/wrappers";
import { useTestingState } from "./useTestingState";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => null,
}));

const { Wrapper } = createOperationsWithQueryWrapper();

describe("useTestingState", () => {
  it("dispatches test configuration updates", () => {
    const { result } = renderHook(() => useTestingState(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setSelectedSummaryId("sum-1");
      result.current.setTestStartDate("2024-01-01");
      result.current.setTestEndDate("2024-01-31");
      result.current.setSelectedDataTypes(["assets"]);
    });

    expect(result.current.selectedSummaryId).toBe("sum-1");
    expect(result.current.testStartDate).toBe("2024-01-01");
    expect(result.current.selectedDataTypes).toEqual(["assets"]);
  });

  it("updates run and yaml state", () => {
    const { result } = renderHook(() => useTestingState(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setIsRunning(true);
      result.current.setYamlExported(true);
      result.current.setYamlContent("a: 1");
      result.current.setTestError("err");
    });

    expect(result.current.isRunning).toBe(true);
    expect(result.current.yamlExported).toBe(true);
    expect(result.current.yamlContent).toBe("a: 1");
    expect(result.current.testError).toBe("err");
  });
});
