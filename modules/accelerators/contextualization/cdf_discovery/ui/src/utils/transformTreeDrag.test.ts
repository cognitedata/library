import { describe, expect, it } from "vitest";
import {
  beginTransformTreeDrag,
  endTransformTreeDrag,
  peekTransformTreeDragPayload,
  resolveTransformTreeDropTarget,
  transformTreeDropAccepts,
} from "./transformTreeDrag";
import { TRANSFORM_ROOT, TRANSFORM_TEMPLATES } from "./treeNodeIds";

describe("resolveTransformTreeDropTarget", () => {
  it("accepts templates folder and template items", () => {
    expect(resolveTransformTreeDropTarget({ id: TRANSFORM_TEMPLATES, kind: "folder" })).toBe(
      "templates"
    );
    expect(
      resolveTransformTreeDropTarget({
        id: "transform:template:my_tpl",
        kind: "etl_template",
      })
    ).toBe("templates");
  });

  it("accepts transform root and pipeline items", () => {
    expect(resolveTransformTreeDropTarget({ id: TRANSFORM_ROOT, kind: "folder" })).toBe(
      "pipelines"
    );
    expect(
      resolveTransformTreeDropTarget({
        id: "transform:pipeline:my_pipe",
        kind: "etl_pipeline",
      })
    ).toBe("pipelines");
  });
});

describe("transformTreeDropAccepts", () => {
  it("allows pipeline onto templates target", () => {
    expect(
      transformTreeDropAccepts("templates", {
        kind: "etl_pipeline",
        pipelineId: "p1",
        label: "P1",
      })
    ).toBe(true);
  });

  it("allows template onto pipelines target", () => {
    expect(
      transformTreeDropAccepts("pipelines", {
        kind: "etl_template",
        templateId: "t1",
        label: "T1",
      })
    ).toBe(true);
  });
});

describe("transform tree drag session", () => {
  it("stores payload until cleared", () => {
    beginTransformTreeDrag({ kind: "etl_pipeline", pipelineId: "p1", label: "P1" });
    expect(peekTransformTreeDragPayload()?.pipelineId).toBe("p1");
    endTransformTreeDrag();
    expect(peekTransformTreeDragPayload()).toBeNull();
  });
});
