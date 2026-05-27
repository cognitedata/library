import { describe, expect, it } from "vitest";
import type { TreeNode } from "../types/discoveryNodes";
import {
  canDragCdfResourceToTransformCanvas,
  cdfResourceDragPayloadFromNode,
  cdfResourceDropStage,
} from "./cdfResourceDrop";

describe("cdfResourceDrop", () => {
  it("maps function tree nodes to function_ref config", () => {
    const node: TreeNode = {
      id: "fusion:integration:functions:item:fn_etl_join",
      label: "Join",
      kind: "function",
      has_children: false,
      meta: { external_id: "fn_etl_join", id: "fn_etl_join" },
    };
    expect(canDragCdfResourceToTransformCanvas(node)).toBe(true);
    const payload = cdfResourceDragPayloadFromNode(node);
    expect(payload?.kind).toBe("cdf_function");
    const drop = cdfResourceDropStage(payload!);
    expect(drop.stage).toBe("function_ref");
    expect(drop.config.function_external_id).toBe("fn_etl_join");
  });

  it("maps transformation and workflow nodes", () => {
    const tr: TreeNode = {
      id: "t1",
      label: "My tr",
      kind: "transformation",
      has_children: false,
      meta: { external_id: "tr_asset_load" },
    };
    expect(cdfResourceDropStage(cdfResourceDragPayloadFromNode(tr)!).stage).toBe(
      "transformation_ref"
    );

    const wf: TreeNode = {
      id: "w1",
      label: "Child wf",
      kind: "workflow",
      has_children: false,
      meta: { external_id: "wf_child", version: "3" },
    };
    const wfDrop = cdfResourceDropStage(cdfResourceDragPayloadFromNode(wf)!);
    expect(wfDrop.stage).toBe("subworkflow");
    expect(wfDrop.config.workflow_external_id).toBe("wf_child");
    expect(wfDrop.config.workflow_version).toBe("3");
  });

  it("rejects transformations without external id", () => {
    const node: TreeNode = {
      id: "t2",
      label: "No ext",
      kind: "transformation",
      has_children: false,
      meta: { id: 99 },
    };
    expect(cdfResourceDragPayloadFromNode(node)).toBeNull();
  });
});
