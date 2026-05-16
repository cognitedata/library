import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { resolveSubflowParentAfterDrag } from "./subflowDropAssociation";

describe("resolveSubflowParentAfterDrag", () => {
  it("returns null (subflow containment removed)", () => {
    const sf: Node = {
      id: "sf1",
      type: "keaSubgraph",
      position: { x: 100, y: 100 },
      data: { label: "S" },
      style: { width: 380, height: 260 },
    };
    const dropped: Node = {
      id: "n1",
      type: "keaExtraction",
      position: { x: 200, y: 180 },
      data: { label: "E", handler_id: "h" },
    };
    expect(resolveSubflowParentAfterDrag([sf, dropped], dropped)).toBeNull();
  });
});
