import { describe, expect, it } from "vitest";
import {
  CDF_QUERY_KEYS,
  useAgents,
  useDeleteSummary,
  useFileContent,
  usePdfBlob,
} from "./useCdfData";

describe("useCdfData barrel", () => {
  it("re-exports query keys and hooks", () => {
    expect(CDF_QUERY_KEYS.agents).toEqual(["agents"]);
    expect(typeof useAgents).toBe("function");
    expect(typeof useFileContent).toBe("function");
    expect(typeof usePdfBlob).toBe("function");
    expect(typeof useDeleteSummary).toBe("function");
  });
});
