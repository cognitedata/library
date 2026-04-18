import { useCallback, useEffect, useRef, useState } from "react";
import {
  addEdge,
  Background,
  BackgroundVariant,
  Controls,
  type Connection,
  type Edge,
  MiniMap,
  type Node,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { createNodeFromPalette } from "./createNodeFromPalette";
import { matchValidationReuseOnDrop } from "./matchValidationReuseOnDrop";
import { canvasToFlowEdges, canvasToFlowNodes, flowToCanvasDocument, type FlowEdgeData } from "./flowDocumentBridge";
import { FlowNodeInspector } from "./FlowNodeInspector";
import { FlowPalette, getPaletteDropPayload } from "./FlowPalette";
import { seedCanvasFromScope } from "./seedCanvasFromScope";
import { mergeSourceViewEntityTypeIntoExtractionRule } from "./workflowScopePatch";
import { useFlowPanelLayout } from "./useFlowPanelLayout";
import { layoutFlowNodes } from "./autoLayoutFlow";
import { FlowNodeEditorModal } from "./FlowNodeEditorModal";
import { KEA_FLOW_NODE_TYPES } from "./flowNodeRegistry";
import { TreeContextMenuPortal, useTreeContextMenuState, type TreeCtxMenuItem } from "../TreeContextMenu";

/** React Flow types for `confidence_match_rules` layout nodes. */
const MATCH_RULE_NODE_TYPES = new Set<string>([
  "keaMatchValidationRuleSourceView",
  "keaMatchValidationRuleExtraction",
  "keaMatchValidationRuleAliasing",
]);

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  initialDocument: WorkflowCanvasDocument;
  /** Increment when canvas is reloaded from disk so internal nodes reset. */
  reloadNonce: number;
  workflowScopeDoc: Record<string, unknown>;
  /** Patch workflow scope (e.g. source view filters, rule scope_filters) while editing the flow. */
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onChange: (doc: WorkflowCanvasDocument) => void;
  /** Optional: push canvas document into scope YAML (e.g. match-rule order); may no-op. */
  onSyncScopeFromCanvas?: (canvas: WorkflowCanvasDocument) => void;
};

function FlowCanvasBody({
  t,
  initialDocument,
  reloadNonce,
  workflowScopeDoc,
  onPatchWorkflowScope,
  onChange,
  onSyncScopeFromCanvas,
}: Props) {
  const { screenToFlowPosition, getNode, fitView } = useReactFlow();

  const flowCtxMenu = useTreeContextMenuState();

  const patchWorkflowScopeRef = useRef(onPatchWorkflowScope);
  patchWorkflowScopeRef.current = onPatchWorkflowScope;
  const [nodes, setNodes, onNodesChange] = useNodesState(canvasToFlowNodes(initialDocument.nodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(canvasToFlowEdges(initialDocument.edges));

  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;
  const onSyncScopeFromCanvasRef = useRef(onSyncScopeFromCanvas);
  onSyncScopeFromCanvasRef.current = onSyncScopeFromCanvas;

  const skipEmitRef = useRef(false);
  const latestInitialRef = useRef(initialDocument);
  latestInitialRef.current = initialDocument;

  useEffect(() => {
    const doc = latestInitialRef.current;
    setNodes(canvasToFlowNodes(doc.nodes));
    setEdges(canvasToFlowEdges(doc.edges));
    skipEmitRef.current = true;
  }, [reloadNonce, setNodes, setEdges]);

  useEffect(() => {
    if (skipEmitRef.current) {
      skipEmitRef.current = false;
      return;
    }
    const doc = flowToCanvasDocument(nodes, edges);
    onChangeRef.current(doc);
    onSyncScopeFromCanvasRef.current?.(doc);
  }, [nodes, edges]);

  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [editorModalNode, setEditorModalNode] = useState<Node | null>(null);

  const isValidConnection = useCallback(
    (c: Connection | Edge) => {
      const st = getNode(c.source)?.type;
      const tt = getNode(c.target)?.type;
      if (!st || !tt) return false;

      if (st === "keaEnd") return false;

      if (tt === "keaEnd") {
        return (
          st === "keaExtraction" ||
          st === "keaAliasing" ||
          st === "keaValidation" ||
          MATCH_RULE_NODE_TYPES.has(st) ||
          st === "keaAliasPersistence" ||
          st === "keaReferenceIndex"
        );
      }

      if (tt === "keaReferenceIndex") return st === "keaExtraction";

      if (tt === "keaAliasPersistence") {
        return st === "keaAliasing" || st === "keaValidation" || st === "keaExtraction";
      }

      if (st === "keaReferenceIndex") return tt === "keaEnd";

      if (st === "keaAliasPersistence") return tt === "keaEnd";

      if (st === "keaStart") {
        return tt === "keaSourceView" || tt === "keaExtraction";
      }

      if (tt === "keaSourceView") return st === "keaStart";

      if (st === "keaSourceView") {
        if (tt === "keaAliasing") return false;
        return tt === "keaExtraction" || MATCH_RULE_NODE_TYPES.has(tt);
      }

      // Heads from extraction / source view / aliasing — or match-rule → match-rule (tree: sequence / parallel_group).
      if (MATCH_RULE_NODE_TYPES.has(tt)) {
        return (
          st === "keaSourceView" ||
          st === "keaExtraction" ||
          st === "keaAliasing" ||
          MATCH_RULE_NODE_TYPES.has(st)
        );
      }

      if (MATCH_RULE_NODE_TYPES.has(st)) {
        return tt === "keaEnd" || MATCH_RULE_NODE_TYPES.has(tt);
      }

      return true;
    },
    [getNode]
  );

  const onConnect = useCallback(
    (params: Connection) => {
      setEdges((eds) => {
        const srcType = getNode(params.source)?.type;
        const tgtType = getNode(params.target)?.type;
        let edgeKind: FlowEdgeData["kind"] = "data";
        if (srcType === "keaAliasing" && tgtType === "keaAliasing") {
          const existingAliasingChainOut = eds.some((e) => {
            if (e.source !== params.source) return false;
            if (e.target === params.target) return false;
            return getNode(e.target)?.type === "keaAliasing";
          });
          edgeKind = existingAliasingChainOut ? "parallel_group" : "sequence";
        } else if (
          srcType &&
          tgtType &&
          MATCH_RULE_NODE_TYPES.has(srcType) &&
          MATCH_RULE_NODE_TYPES.has(tgtType)
        ) {
          const existingMatchChainOut = eds.some((e) => {
            if (e.source !== params.source) return false;
            if (e.target === params.target) return false;
            const t = getNode(e.target)?.type;
            return Boolean(t && MATCH_RULE_NODE_TYPES.has(t));
          });
          edgeKind = existingMatchChainOut ? "parallel_group" : "sequence";
        }
        return addEdge(
          {
            ...params,
            data: { kind: edgeKind } satisfies FlowEdgeData,
          },
          eds
        );
      });
      const src = getNode(params.source);
      const tgt = getNode(params.target);
      if (src?.type === "keaSourceView" && tgt?.type === "keaExtraction") {
        const sData = (src.data ?? {}) as Record<string, unknown>;
        const tData = (tgt.data ?? {}) as Record<string, unknown>;
        const refS = sData.ref;
        const refT = tData.ref;
        const svIdx =
          refS && typeof refS === "object" && !Array.isArray(refS)
            ? (refS as Record<string, unknown>).source_view_index
            : undefined;
        const ruleName =
          refT && typeof refT === "object" && !Array.isArray(refT)
            ? (refT as Record<string, unknown>).extraction_rule_name
            : undefined;
        if (typeof svIdx === "number" && ruleName != null && String(ruleName).trim()) {
          patchWorkflowScopeRef.current((doc) =>
            mergeSourceViewEntityTypeIntoExtractionRule(doc, svIdx, String(ruleName).trim())
          );
        }
      }
    },
    [setEdges, getNode]
  );

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const payload = getPaletteDropPayload(e);
      if (!payload) return;
      const pos = screenToFlowPosition({ x: e.clientX, y: e.clientY });
      const reuse = matchValidationReuseOnDrop(payload, pos, nodes, edges, workflowScopeDoc);
      if (reuse.action === "reuse") {
        const head = getNode(reuse.headId);
        if (head) setSelectedNode(head);
        const connectFromId = reuse.connectFromId;
        if (connectFromId) {
          setEdges((eds) => {
            if (eds.some((x) => x.source === connectFromId && x.target === reuse.headId)) {
              return eds;
            }
            return addEdge(
              {
                id: `e_${connectFromId}_${reuse.headId}_${Date.now()}`,
                source: connectFromId,
                target: reuse.headId,
                data: { kind: "data" } satisfies FlowEdgeData,
              },
              eds
            );
          });
        }
        return;
      }
      const node = createNodeFromPalette(payload, pos);
      setNodes((nds) => nds.concat(node));
    },
    [screenToFlowPosition, setNodes, setEdges, nodes, edges, workflowScopeDoc, getNode]
  );

  const patchNode = useCallback(
    (nodeId: string, data: Record<string, unknown>) => {
      setNodes((nds) => nds.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n)));
    },
    [setNodes]
  );

  const patchEdge = useCallback(
    (edgeId: string, kind: FlowEdgeData["kind"]) => {
      setEdges((eds) =>
        eds.map((edge) =>
          edge.id === edgeId ? { ...edge, data: { ...((edge.data ?? {}) as FlowEdgeData), kind } } : edge
        )
      );
    },
    [setEdges]
  );

  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
    setSelectedEdge(null);
  }, []);

  const onEdgeClick = useCallback((_event: React.MouseEvent, edge: Edge) => {
    setSelectedEdge(edge);
    setSelectedNode(null);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
    setSelectedEdge(null);
  }, []);

  const onNodeDoubleClick = useCallback((e: React.MouseEvent, node: Node) => {
    e.preventDefault();
    setEditorModalNode(node);
    setSelectedNode(node);
    setSelectedEdge(null);
  }, []);

  const handleSeed = useCallback(() => {
    const doc = seedCanvasFromScope(workflowScopeDoc);
    setNodes(canvasToFlowNodes(doc.nodes));
    setEdges(canvasToFlowEdges(doc.edges));
    onChangeRef.current(doc);
  }, [workflowScopeDoc, setNodes, setEdges]);

  const handleAutoLayout = useCallback(() => {
    setNodes((nds) => layoutFlowNodes(nds, edges));
    window.setTimeout(() => fitView({ padding: 0.15 }), 0);
  }, [edges, setNodes, fitView]);

  const removeNodeById = useCallback(
    (nodeId: string) => {
      setNodes((nds) => nds.filter((n) => n.id !== nodeId));
      setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId));
      setSelectedNode((sn) => (sn?.id === nodeId ? null : sn));
      setSelectedEdge(null);
    },
    [setNodes, setEdges]
  );

  const removeEdgeById = useCallback(
    (edgeId: string) => {
      setEdges((eds) => eds.filter((e) => e.id !== edgeId));
      setSelectedEdge((se) => (se?.id === edgeId ? null : se));
    },
    [setEdges]
  );

  const onPaneContextMenu = useCallback(
    (e: React.MouseEvent | MouseEvent) => {
      flowCtxMenu.open(e, [
        {
          id: "fit",
          label: t("flow.ctxMenuFitView"),
          onSelect: () => fitView({ padding: 0.15 }),
        },
        {
          id: "auto-layout",
          label: t("flow.ctxMenuAutoLayout"),
          onSelect: () => handleAutoLayout(),
        },
        { id: "seed", label: t("flow.seedFromScope"), onSelect: () => handleSeed() },
      ]);
    },
    [flowCtxMenu, t, fitView, handleAutoLayout, handleSeed]
  );

  const onFlowNodeContextMenu = useCallback(
    (e: React.MouseEvent, node: Node) => {
      setSelectedNode(node);
      setSelectedEdge(null);
      if (node.type === "keaStart" || node.type === "keaEnd") return;
      flowCtxMenu.open(e, [
        {
          id: "remove-node",
          label: t("flow.ctxMenuRemoveNode"),
          danger: true,
          onSelect: () => removeNodeById(node.id),
        },
      ]);
    },
    [flowCtxMenu, t, removeNodeById]
  );

  const onFlowEdgeContextMenu = useCallback(
    (e: React.MouseEvent, edge: Edge) => {
      setSelectedEdge(edge);
      setSelectedNode(null);
      const fd = (edge.data ?? {}) as FlowEdgeData;
      const cur: NonNullable<FlowEdgeData["kind"]> =
        fd.kind === "sequence" || fd.kind === "parallel_group" ? fd.kind : "data";
      const kinds = ["data", "sequence", "parallel_group"] as const;
      const labelFor = (k: (typeof kinds)[number]) =>
        k === "data" ? t("flow.edgeKindData") : k === "sequence" ? t("flow.edgeKindSequence") : t("flow.edgeKindParallel");
      const items: TreeCtxMenuItem[] = [
        ...kinds.map((k) => ({
          id: `kind-${k}`,
          label: labelFor(k),
          disabled: cur === k,
          onSelect: () => patchEdge(edge.id, k),
        })),
        {
          id: "remove-edge",
          label: t("flow.ctxMenuRemoveEdge"),
          danger: true,
          onSelect: () => removeEdgeById(edge.id),
        },
      ];
      flowCtxMenu.open(e, items);
    },
    [flowCtxMenu, t, patchEdge, removeEdgeById]
  );

  const panel = useFlowPanelLayout();

  return (
    <div className="kea-flow-shell">
      <div
        className={`kea-flow-shell__left${panel.leftCollapsed ? " kea-flow-shell__left--collapsed" : ""}`}
        style={
          panel.leftCollapsed
            ? { flex: `0 0 ${panel.collapsedStripPx}px`, width: panel.collapsedStripPx }
            : { flex: `0 0 ${panel.leftWidth}px`, width: panel.leftWidth, minWidth: panel.leftMin, maxWidth: panel.leftMax }
        }
      >
        {panel.leftCollapsed ? (
          <button
            type="button"
            className="kea-flow-shell__reveal kea-flow-shell__reveal--left"
            aria-expanded={false}
            aria-label={t("flow.expandLeftPanel")}
            title={t("flow.expandLeftPanel")}
            onClick={panel.expandLeft}
          >
            ›
          </button>
        ) : (
          <>
            <div className="kea-flow-shell__panel-bar">
              <span className="kea-flow-shell__panel-bar-title">{t("flow.leftPanelTitle")}</span>
              <button
                type="button"
                className="kea-btn kea-btn--sm kea-flow-shell__panel-bar-btn"
                aria-expanded
                aria-label={t("flow.collapseLeftPanel")}
                title={t("flow.collapseLeftPanel")}
                onClick={panel.collapseLeft}
              >
                ‹
              </button>
            </div>
            <FlowPalette t={t} scopeDocument={workflowScopeDoc} />
          </>
        )}
      </div>
      {!panel.leftCollapsed && (
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label={t("flow.resizePanels")}
          className="kea-flow-shell__resize"
          onMouseDown={panel.onResizeLeftStart}
        />
      )}
      <div className="kea-flow-main">
        <div className="kea-flow-toolbar">
          <button type="button" className="kea-btn kea-btn--sm" onClick={handleSeed}>
            {t("flow.seedFromScope")}
          </button>
          <span className="kea-hint" style={{ marginLeft: "0.5rem" }}>
            {t("flow.canvasHint")}
          </span>
        </div>
        <div className="kea-flow-canvas-wrap">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            isValidConnection={isValidConnection}
            onConnect={onConnect}
            onDrop={onDrop}
            onDragOver={onDragOver}
            nodeTypes={KEA_FLOW_NODE_TYPES}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            onNodeDoubleClick={onNodeDoubleClick}
            onPaneContextMenu={onPaneContextMenu}
            onNodeContextMenu={onFlowNodeContextMenu}
            onEdgeContextMenu={onFlowEdgeContextMenu}
            fitView
            minZoom={0.2}
            maxZoom={1.5}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
            <Controls />
            <MiniMap zoomable pannable className="kea-flow-minimap" />
          </ReactFlow>
        </div>
      </div>
      {!panel.rightCollapsed && (
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label={t("flow.resizePanels")}
          className="kea-flow-shell__resize"
          onMouseDown={panel.onResizeRightStart}
        />
      )}
      <div
        className={`kea-flow-shell__right${panel.rightCollapsed ? " kea-flow-shell__right--collapsed" : ""}`}
        style={
          panel.rightCollapsed
            ? { flex: `0 0 ${panel.collapsedStripPx}px`, width: panel.collapsedStripPx }
            : {
                flex: `0 0 ${panel.rightWidth}px`,
                width: panel.rightWidth,
                minWidth: panel.rightMin,
                maxWidth: panel.rightMax,
              }
        }
      >
        {panel.rightCollapsed ? (
          <button
            type="button"
            className="kea-flow-shell__reveal kea-flow-shell__reveal--right"
            aria-expanded={false}
            aria-label={t("flow.expandRightPanel")}
            title={t("flow.expandRightPanel")}
            onClick={panel.expandRight}
          >
            ‹
          </button>
        ) : (
          <>
            <div className="kea-flow-shell__panel-bar kea-flow-shell__panel-bar--end">
              <button
                type="button"
                className="kea-btn kea-btn--sm kea-flow-shell__panel-bar-btn"
                aria-expanded
                aria-label={t("flow.collapseRightPanel")}
                title={t("flow.collapseRightPanel")}
                onClick={panel.collapseRight}
              >
                ›
              </button>
              <span className="kea-flow-shell__panel-bar-title">{t("flow.rightPanelTitle")}</span>
            </div>
            <FlowNodeInspector
              t={t}
              selectedNode={selectedNode}
              selectedEdge={selectedEdge}
              workflowDoc={workflowScopeDoc}
              onPatchWorkflowScope={onPatchWorkflowScope}
              onPatchNode={patchNode}
              onPatchEdge={patchEdge}
            />
          </>
        )}
      </div>
      <TreeContextMenuPortal menu={flowCtxMenu.menu} onClose={flowCtxMenu.close} classPrefix="kea" />
      {editorModalNode && (
        <FlowNodeEditorModal
          node={editorModalNode}
          workflowDoc={workflowScopeDoc}
          onPatchWorkflowScope={onPatchWorkflowScope}
          onClose={() => setEditorModalNode(null)}
          t={t}
        />
      )}
    </div>
  );
}

export function WorkflowFlowPanel(props: Props) {
  return (
    <ReactFlowProvider>
      <FlowCanvasBody {...props} />
    </ReactFlowProvider>
  );
}

