import { describe, expect, it } from "vitest";
import { connectEndMenuOptionsForSourceType } from "./connectEndMenuOptions";

describe("connectEndMenuOptions", () => {
  it("offers query and fusion stages from start", () => {
    const opts = connectEndMenuOptionsForSourceType("etlStart");
    const stages = new Set(opts.map((o) => o.payload.stage));
    expect(stages.has("query_view")).toBe(true);
    expect(stages.has("spark_transform")).toBe(true);
    expect(stages.has("subworkflow")).toBe(true);
  });

  it("appends query stages after transform nodes", () => {
    const opts = connectEndMenuOptionsForSourceType("etlTransform");
    const queryStages = opts.filter((o) => o.id.includes("-query-"));
    expect(queryStages.map((o) => o.payload.stage)).toEqual([
      "query_view",
      "query_raw",
      "query_classic",
      "query_sql",
      "query_records",
    ]);
    expect(opts.some((o) => o.payload.stage === "filter")).toBe(true);
  });

  it("appends query stages after fusion spark transform", () => {
    const opts = connectEndMenuOptionsForSourceType("etlSparkTransform");
    expect(opts.some((o) => o.payload.stage === "query_view")).toBe(true);
    expect(opts.some((o) => o.payload.stage === "spark_transform")).toBe(false);
  });

  it("does not offer transform handlers after fusion nodes", () => {
    const opts = connectEndMenuOptionsForSourceType("etlSparkTransform");
    expect(opts.some((o) => o.payload.handlerId === "trim_whitespace")).toBe(false);
    expect(opts.some((o) => o.payload.stage === "score")).toBe(false);
  });

  it("offers transform handlers after query nodes", () => {
    const opts = connectEndMenuOptionsForSourceType("etlQueryView");
    expect(opts.some((o) => o.payload.handlerId === "trim_whitespace")).toBe(true);
    expect(opts.some((o) => o.payload.stage === "score")).toBe(true);
  });

  it("does not offer transform handlers after save nodes", () => {
    const opts = connectEndMenuOptionsForSourceType("etlSaveView");
    expect(opts.some((o) => o.payload.handlerId === "trim_whitespace")).toBe(false);
  });
});
