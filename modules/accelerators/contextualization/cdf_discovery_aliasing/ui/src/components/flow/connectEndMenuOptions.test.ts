import { describe, expect, it } from "vitest";
import { connectEndMenuOptionsForSourceType } from "./connectEndMenuOptions";

describe("connectEndMenuOptionsForSourceType", () => {
  it("returns no connect-end palette targets for persistence nodes (out may wire only to End)", () => {
    expect(connectEndMenuOptionsForSourceType("discoveryViewSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("discoveryRawSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("discoveryClassicSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("discoveryAliasPersistence")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("discoveryInvertedIndex")).toEqual([]);
  });

  it("still offers transform from view query", () => {
    const opts = connectEndMenuOptionsForSourceType("discoveryViewQuery");
    expect(opts.some((o) => o.payload.kind === "discovery" && o.payload.stage === "transform")).toBe(true);
  });
});
