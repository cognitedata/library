import { describe, expect, it } from "vitest";
import {
  DISCOVERY_TRANSFORM_HANDLER_IDS,
  TRANSFORM_HANDLER_DEFINITIONS,
  transformHandlerDefinition,
} from "./handlerRegistry";

describe("TRANSFORM_HANDLER_DEFINITIONS", () => {
  it("covers every discovery transform handler id with a nameKey", () => {
    expect(TRANSFORM_HANDLER_DEFINITIONS.length).toBe(DISCOVERY_TRANSFORM_HANDLER_IDS.length);
    for (const id of DISCOVERY_TRANSFORM_HANDLER_IDS) {
      const def = transformHandlerDefinition(id);
      expect(def).toBeDefined();
      expect(def!.nameKey).toMatch(/^transforms\.handlerName\./);
    }
  });
});
