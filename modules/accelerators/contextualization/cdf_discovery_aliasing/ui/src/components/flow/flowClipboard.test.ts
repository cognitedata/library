import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  buildFlowClipboardPayload,
  parseFlowClipboardText,
  pasteFlowClipboard,
  serializeFlowClipboardPayload,
} from "./flowClipboard";

describe("flowClipboard", () => {
  const nodes: Node[] = [
    { id: "start", type: "keaStart", position: { x: 0, y: 0 }, data: { label: "Start" } },
    { id: "q1", type: "keaViewQuery", position: { x: 100, y: 0 }, data: { label: "Query", config: {} } },
    { id: "end", type: "keaEnd", position: { x: 300, y: 0 }, data: { label: "End" } },
  ];
  const edges: Edge[] = [
    { id: "e1", source: "start", target: "q1" },
    { id: "e2", source: "q1", target: "end" },
  ];

  it("builds payload without start/end and only internal edges", () => {
    const payload = buildFlowClipboardPayload(nodes, edges, [nodes[1]!]);
    expect(payload?.nodes).toHaveLength(1);
    expect(payload?.nodes[0]?.id).toBe("q1");
    expect(payload?.edges).toHaveLength(0);
  });

  it("round-trips through clipboard text", () => {
    const payload = buildFlowClipboardPayload(nodes, edges, [nodes[1]!]);
    expect(payload).not.toBeNull();
    const text = serializeFlowClipboardPayload(payload!);
    expect(parseFlowClipboardText(text)?.nodes[0]?.id).toBe("q1");
  });

  it("pastes with new ids, offset, and remapped edges", () => {
    const payload = buildFlowClipboardPayload(nodes, edges, [nodes[1]!])!;
    const result = pasteFlowClipboard(nodes, edges, payload);
    expect(result).not.toBeNull();
    expect(result!.newNodeIds).toHaveLength(1);
    expect(result!.newNodeIds[0]).not.toBe("q1");
    const pasted = result!.nodes.find((n) => n.id === result!.newNodeIds[0]);
    expect(pasted?.position).toEqual({ x: 148, y: 48 });
    expect(result!.nodes).toHaveLength(4);
  });

  it("skips start/end when they are the only selection", () => {
    expect(buildFlowClipboardPayload(nodes, edges, [nodes[0]!, nodes[2]!])).toBeNull();
  });
});
