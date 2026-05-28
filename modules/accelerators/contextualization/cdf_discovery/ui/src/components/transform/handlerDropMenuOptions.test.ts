import { describe, expect, it } from "vitest";
import {
  handlerDropMenuGroupedOptionsForStage,
  palettePayloadNeedsHandlerPick,
  transformHandlerPaletteItems,
} from "./handlerDropMenuOptions";
import { materializeEtlStageAtPosition } from "./paletteDropOnEdge";

describe("handlerDropMenuOptions", () => {
  it("requires handler pick for transform stage without handlerId", () => {
    expect(
      palettePayloadNeedsHandlerPick({ kind: "etl_stage", stage: "transform" })
    ).toBe(true);
    expect(
      palettePayloadNeedsHandlerPick({
        kind: "etl_stage",
        stage: "transform",
        handlerId: "trim_whitespace",
      })
    ).toBe(false);
    expect(
      palettePayloadNeedsHandlerPick({ kind: "etl_stage", stage: "filter" })
    ).toBe(false);
  });

  it("lists transform handler groups by category", () => {
    const groups = handlerDropMenuGroupedOptionsForStage("transform");
    expect(groups?.map((g) => g.id)).toEqual(["string", "structure", "derive"]);
    expect(groups?.[0]?.options.some((o) => o.handlerId === "regex_substitution")).toBe(true);
    expect(groups?.[0]?.options.some((o) => o.handlerId === "trim_whitespace")).toBe(true);
  });

  it("exposes flat transform handler palette items", () => {
    const items = transformHandlerPaletteItems();
    expect(items.length).toBeGreaterThan(10);
    expect(items.every((o) => o.payload.handlerId)).toBe(true);
    expect(items.some((o) => o.handlerId === "trim_whitespace")).toBe(true);
  });
});

describe("materializeEtlStageAtPosition with handler", () => {
  it("seeds transform config and locks handler from palette", () => {
    const mat = materializeEtlStageAtPosition(
      { kind: "etl_stage", stage: "transform", handlerId: "trim_whitespace" },
      { x: 0, y: 0 },
      new Set()
    );
    expect(mat).not.toBeNull();
    const cfg = (mat!.node.data as { config?: Record<string, unknown> }).config;
    expect(cfg?.handler_id).toBe("trim_whitespace");
    expect((mat!.node.data as { palette_handler_locked?: boolean }).palette_handler_locked).toBe(
      true
    );
  });
});
