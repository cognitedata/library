import { describe, expect, it } from "vitest";
import {
  isMultiStepTransformConfig,
  materializeTransformSteps,
  parseTransformNodeConfig,
  serializeTransformNodeConfig,
} from "./transformNodeConfigModel";

describe("transformNodeConfigModel", () => {
  it("materializes legacy single-handler config as one step", () => {
    const cfg = {
      description: "t",
      handler_id: "trim_whitespace",
      output_field: "aliases",
      trim_whitespace: { mode: "ends_only" },
    };
    const steps = materializeTransformSteps(cfg);
    expect(steps).toHaveLength(1);
    expect(steps[0]?.handler_id).toBe("trim_whitespace");
  });

  it("round-trips multi-step ordered config", () => {
    const raw = {
      description: "chain",
      execution: { mode: "ordered" },
      steps: [
        { handler_id: "trim_whitespace", output_field: "a" },
        { handler_id: "split_join", output_field: "aliases" },
      ],
    };
    expect(isMultiStepTransformConfig(raw)).toBe(true);
    const parsed = parseTransformNodeConfig(raw);
    expect(parsed.steps).toHaveLength(2);
    const out = serializeTransformNodeConfig({ ...parsed, multiStep: true });
    expect(out.execution).toEqual({ mode: "ordered" });
    expect(out.steps).toHaveLength(2);
  });

  it("flattens single-step when not multi-step mode", () => {
    const parsed = parseTransformNodeConfig({
      handler_id: "trim_whitespace",
      output_field: "x",
    });
    const out = serializeTransformNodeConfig({ ...parsed, multiStep: false });
    expect(out.handler_id).toBe("trim_whitespace");
    expect(out.steps).toBeUndefined();
  });
});
