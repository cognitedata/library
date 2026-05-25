import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  buildTransformFlowClipboardPayload,
  parseTransformFlowClipboardText,
  pasteTransformFlowClipboard,
  serializeTransformFlowClipboardPayload,
} from "./transformFlowClipboard";

describe("transformFlowClipboard", () => {
  const nodes: Node[] = [
    { id: "start", type: "etlStart", position: { x: 0, y: 0 }, data: { label: "Start", kind: "start" } },
    {
      id: "q1",
      type: "etlQueryView",
      position: { x: 100, y: 0 },
      data: { label: "Query", kind: "query_view", config: {} },
    },
    { id: "end", type: "etlEnd", position: { x: 300, y: 0 }, data: { label: "End", kind: "end" } },
  ];
  const edges: Edge[] = [
    { id: "e1", source: "start", target: "q1" },
    { id: "e2", source: "q1", target: "end" },
  ];

  it("builds payload without start/end and only internal edges", () => {
    const payload = buildTransformFlowClipboardPayload(nodes, edges, [nodes[1]!]);
    expect(payload?.nodes).toHaveLength(1);
    expect(payload?.nodes[0]?.id).toBe("q1");
    expect(payload?.edges).toHaveLength(0);
  });

  it("round-trips through clipboard text", () => {
    const payload = buildTransformFlowClipboardPayload(nodes, edges, [nodes[1]!]);
    expect(payload).not.toBeNull();
    const text = serializeTransformFlowClipboardPayload(payload!);
    expect(parseTransformFlowClipboardText(text)?.nodes[0]?.id).toBe("q1");
  });

  it("pastes with new ids and offset", () => {
    const payload = buildTransformFlowClipboardPayload(nodes, edges, [nodes[1]!])!;
    const result = pasteTransformFlowClipboard(nodes, edges, payload);
    expect(result).not.toBeNull();
    expect(result!.newNodeIds).toHaveLength(1);
    expect(result!.newNodeIds[0]).not.toBe("q1");
    const pasted = result!.nodes.find((n) => n.id === result!.newNodeIds[0]);
    expect(pasted?.position).toEqual({ x: 148, y: 48 });
    expect(result!.nodes).toHaveLength(4);
  });

  it("skips start/end when they are the only selection", () => {
    expect(buildTransformFlowClipboardPayload(nodes, edges, [nodes[0]!, nodes[2]!])).toBeNull();
  });
});
