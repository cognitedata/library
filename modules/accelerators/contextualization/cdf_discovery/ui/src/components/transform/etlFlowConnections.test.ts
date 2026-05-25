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
});
