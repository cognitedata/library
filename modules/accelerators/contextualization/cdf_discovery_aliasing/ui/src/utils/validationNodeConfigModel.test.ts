import { describe, expect, it } from "vitest";
import {
  parseValidationNodeConfig,
  serializeValidationNodeConfig,
} from "./validationNodeConfigModel";

describe("validationNodeConfigModel", () => {
  it("migrates validation_rule_definitions into steps", () => {
    const cfg = {
      description: "Asset blacklist",
      validation_rule_definitions: {
        blacklist: {
          name: "blacklist",
          enabled: true,
          priority: 10,
          match: { keywords: ["test"], expressions: [] },
          confidence_modifier: { mode: "explicit", value: 0 },
        },
      },
    };
    const parsed = parseValidationNodeConfig(cfg);
    expect(parsed.steps).toHaveLength(1);
    expect(parsed.steps[0]?.name).toBe("blacklist");
    const out = serializeValidationNodeConfig({
      ...parsed,
      description: parsed.description,
    });
    expect(out.description).toBe("Asset blacklist");
    expect(Array.isArray(out.steps)).toBe(true);
    expect((out.steps as unknown[]).length).toBe(1);
    expect(out.validation_rule_definitions).toBeUndefined();
  });
});
