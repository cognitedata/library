import { describe, expect, it } from "vitest";
import { useAgentLoader } from "./index";

describe("dashboard-agent barrel", () => {
  it("re-exports the agent loader", () => {
    expect(typeof useAgentLoader).toBe("function");
  });
});
