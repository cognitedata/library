import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { collectSubflowFrameAndHubIds, removeSubflowFrameAndLiftChildren } from "./subflowDeleteLift";

describe("removeSubflowFrameAndLiftChildren", () => {
  it("lifts workflow children to root and removes frame and hubs", () => {
    const sfId = "sf1";
    const hubIn = `${sfId}_hub_in`;
    const hubOut = `${sfId}_hub_out`;
    const sf: Node = {
      id: sfId,
      type: "keaSubflow",
      position: { x: 0, y: 0 },
      data: {
        label: "G",
        subflow_hub_input_id: hubIn,
        subflow_hub_output_id: hubOut,
        subflow_ports: { inputs: [{ id: "in" }], outputs: [{ id: "out" }] },
      },
      style: { width: 400, height: 300 },
    };
    const hubInNode: Node = {
      id: hubIn,
      type: "keaSubflowGraphIn",
      parentId: sfId,
      position: { x: 20, y: 50 },
      data: {},
    };
    const hubOutNode: Node = {
      id: hubOut,
      type: "keaSubflowGraphOut",
      parentId: sfId,
      position: { x: 300, y: 50 },
      data: {},
    };
    const child: Node = {
      id: "c",
      type: "keaExtraction",
      parentId: sfId,
      position: { x: 120, y: 80 },
      data: { label: "E", handler_id: "h" },
    };
    const nodes: Node[] = [sf, hubInNode, hubOutNode, child];
    const removed = collectSubflowFrameAndHubIds(nodes, sfId);
    expect(removed.has(sfId)).toBe(true);
    expect(removed.has(hubIn)).toBe(true);
    expect(removed.has(hubOut)).toBe(true);

    const out = removeSubflowFrameAndLiftChildren(nodes, sfId);
    expect(out.find((n) => n.id === sfId)).toBeUndefined();
    expect(out.find((n) => n.id === hubIn)).toBeUndefined();
    expect(out.find((n) => n.id === hubOut)).toBeUndefined();
    const lifted = out.find((n) => n.id === "c");
    expect(lifted?.parentId).toBeUndefined();
    expect(lifted?.position.x).toBeGreaterThan(100);
    expect(lifted?.position.y).toBeGreaterThan(50);
  });
});
