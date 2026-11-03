import { describe, expect, it } from "vitest";
import {
  creptGenerationProgress,
  progressFromSummaryLog,
} from "./summaryGenerationProgress";

describe(progressFromSummaryLog.name, () => {
  it("maps startup, agent, data type, and save lines", () => {
    expect(
      progressFromSummaryLog("OPERATIONS SUMMARIZER v2 STARTED")?.progress,
    ).toBe(12);
    expect(
      progressFromSummaryLog("Ensuring agent exists...")?.currentStep,
    ).toBe("Agent");
    expect(progressFromSummaryLog("Processing work_orders...")).toEqual({
      message: "Summarizing work orders",
      progress: 56,
      currentStep: "work orders",
    });
    expect(progressFromSummaryLog("Saving summary to CDF...")?.progress).toBe(
      92,
    );
  });

  it("ignores lines that are not progress milestones", () => {
    expect(progressFromSummaryLog("  -> 4 work_orders")).toBeNull();
  });
});

describe(creptGenerationProgress.name, () => {
  it("starts near the beginning and keeps increasing", () => {
    expect(creptGenerationProgress(0)).toBe(12);
    expect(creptGenerationProgress(15)).toBeGreaterThan(
      creptGenerationProgress(0),
    );
    expect(creptGenerationProgress(15)).toBeLessThan(88);
    expect(creptGenerationProgress(10_000)).toBe(88);
  });
});
