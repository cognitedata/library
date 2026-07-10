import type { Connection, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { isValidEtlFlowConnection } from "./etlFlowConnections";

function node(id: string, type: string): Node {
  return { id, type, position: { x: 0, y: 0 }, data: {} };
}

describe("isValidEtlFlowConnection", () => {
  const nodes = [
    node("save", "etlSaveView"),
    node("end", "etlEnd"),
    node("q", "etlQueryView"),
  ];
  const getNode = (id: string) => nodes.find((n) => n.id === id);

  it("allows save out only to etlEnd", () => {
    const conn: Connection = { source: "save", target: "end" };
    expect(isValidEtlFlowConnection(conn, getNode)).toBe(true);
  });

  it("rejects save out to non-end nodes", () => {
    const conn: Connection = { source: "save", target: "q" };
    expect(isValidEtlFlowConnection(conn, getNode)).toBe(false);
  });

  it("rejects fusion out to transform nodes", () => {
    const nodes = [
      node("spark", "etlSparkTransform"),
      node("trim", "etlTransform"),
    ];
    const getFusionNode = (id: string) => nodes.find((n) => n.id === id);
    expect(
      isValidEtlFlowConnection({ source: "spark", target: "trim" }, getFusionNode)
    ).toBe(false);
  });

  it("allows query out to transform nodes", () => {
    const nodes = [node("q", "etlQueryView"), node("trim", "etlTransform")];
    const getQ = (id: string) => nodes.find((n) => n.id === id);
    expect(isValidEtlFlowConnection({ source: "q", target: "trim" }, getQ)).toBe(true);
  });

  it("allows start out to fusion nodes", () => {
    const fusionNodes = [
      node("start", "etlStart"),
      node("spark", "etlSparkTransform"),
      node("sub", "etlSubworkflow"),
    ];
    const getFusionNode = (id: string) => fusionNodes.find((n) => n.id === id);
    expect(
      isValidEtlFlowConnection({ source: "start", target: "spark" }, getFusionNode)
    ).toBe(true);
    expect(
      isValidEtlFlowConnection({ source: "start", target: "sub" }, getFusionNode)
    ).toBe(true);
  });

  it("allows multiple edges to workflow_fanout_plan in__input_a", () => {
    const nodes = [
      node("ctx1", "etlQueryView"),
      node("ctx2", "etlQueryView"),
      node("plan", "etlWorkflowFanoutPlan"),
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const edges = [
      {
        id: "e1",
        source: "ctx1",
        target: "plan",
        sourceHandle: "out",
        targetHandle: "in__input_a",
      },
    ];
    expect(
      isValidEtlFlowConnection(
        { source: "ctx2", target: "plan", targetHandle: "in__input_a" },
        getNode,
        edges
      )
    ).toBe(true);
  });

  it("rejects a second edge to workflow_fanout_plan in__input_b", () => {
    const nodes = [
      node("files1", "etlQueryView"),
      node("files2", "etlQueryView"),
      node("plan", "etlWorkflowFanoutPlan"),
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const edges = [
      {
        id: "e1",
        source: "files1",
        target: "plan",
        sourceHandle: "out",
        targetHandle: "in__input_b",
      },
    ];
    expect(
      isValidEtlFlowConnection(
        { source: "files2", target: "plan", targetHandle: "in__input_b" },
        getNode,
        edges
      )
    ).toBe(false);
  });
});
