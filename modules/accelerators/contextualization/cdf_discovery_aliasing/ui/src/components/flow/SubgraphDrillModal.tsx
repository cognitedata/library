import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
  type RefObject,
} from "react";
import { createPortal, flushSync } from "react-dom";
import {
  Background,
  BackgroundVariant,
  Controls,
  type Connection,
  type Edge,
  type FinalConnectionState,
  MiniMap,
  type Node,
  type NodeChange,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useOnSelectionChange,
  useReactFlow,
} from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import {
  emptyWorkflowCanvasDocument,
  isSubflowGraphHubRfType,
  isWorkflowCanvasNodeEnabled,
  normalizeWorkflowCanvasHandleOrientation,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
  type SubflowPortsConfig,
  type WorkflowCanvasDocument,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { appendNodeAndResolveSubflowParent, resolveSubflowParentsAfterGroupDrag } from "./subflowDropAssociation";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import { FlowNodeEditorModal } from "./FlowNodeEditorModal";
import { FlowNodeInspector } from "./FlowNodeInspector";
import { FlowPalette, type PaletteDragPayload } from "./FlowPalette";
import { DISCOVERY_FLOW_NODE_TYPES } from "./flowNodeRegistry";
import { materializePaletteDrop } from "./materializePaletteDrop";
import { useFlowPanelLayout } from "./useFlowPanelLayout";
import { layoutFlowNodes } from "./autoLayoutFlow";
import { TreeContextMenuPortal, useTreeContextMenuState, type TreeCtxMenuItem } from "../TreeContextMenu";
import { applyFlowNodeRemovals } from "./applyFlowNodeRemovals";
import { clampNodeInsideParentSubflowFrame } from "./subflowGroupClamp";
import { resolveGroupableSelectionNodes } from "./subflowMembership";
import { isValidDiscoveryFlowConnection } from "./subgraphFlowConnections";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import { resolveAdoptIntoSubgraphAfterDrag } from "./adoptNodesIntoSubgraph";
import {
  ensureSubgraphInnerBoundaryCanvasDocument,
  pruneSubgraphInnerPortEdges,
  syncSubgraphInnerHubPortData,
} from "./subgraphInnerBoundaryHubs";
import {
  canPromoteInnerSubtreeToOwningGraph,
  promoteSubgraphInnerSubtreeToParentWorkflow,
} from "./promoteSubgraphInnerNodeToParent";
import {
  connectEndMenuGroupedOptionsForSourceType,
  connectEndMenuOptionsForSourceType,
  formatConnectEndMenuOptionLabel,
  formatConnectEndMenuOptionTooltip,
} from "./connectEndMenuOptions";
import { discoveryValidationRuleLayoutRfTypes, discoveryWorkflowDisableableRfTypes } from "./flowConstants";
import { applyWorkflowCanvasEnablementPatch } from "./flowNodeEnabled";
import { appendDiscoveryConnectionEdge, dedupeEdgesByHandles } from "./flowEdgeHelpers";
import { applyPaletteCanvasDrop } from "./paletteDropOnEdge";
import { upstreamDownstreamAnimatedEdgeIds } from "./flowSelectionEdgeAnimation";
import { useFlowCanvasHistory, type FlowCanvasSnapshot } from "./useFlowCanvasHistory";
import { useFlowClipboard } from "./useFlowClipboard";
import { readCompileWorkflowDagMode } from "../../utils/workflowCompileMode";
import { alignSelectedFlowNodes, type AlignFlowSelectionMode } from "./alignSelectedNodes";
import { FlowSelectionAlignButtons } from "./FlowSelectionAlignButtons";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function cloneWorkflowCanvasDocument(doc: WorkflowCanvasDocument): WorkflowCanvasDocument {
  return JSON.parse(JSON.stringify(doc)) as WorkflowCanvasDocument;
}

type ConnectEndMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
  sourceNodeId: string;
  sourceHandleId: string | null;
};

type InnerProps = {
  t: TFn;
  hydrateNonce: number;
  outerSubgraphNodeId: string;
  initialDoc: WorkflowCanvasDocument;
  framePorts: SubflowPortsConfig;
  hubInHint: string;
  hubOutHint: string;
  defaultHandleOrientation: WorkflowCanvasHandleOrientation;
  workflowScopeDoc: Record<string, unknown>;
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onEnsureSubgraphBoundary?: (nodeId: string, hubInId: string, hubOutId: string) => void;
  /** Persist ports for the subgraph being edited (its node is not on this inner canvas). */
  onApplyPortsForOuterSubgraph?: (ports: SubflowPortsConfig) => void;
  onSave: (doc: WorkflowCanvasDocument) => void;
  onClose: () => void;
  innerFlowRootRef: RefObject<HTMLDivElement | null>;
  schemaSpace?: string;
  nestDepth?: number;
  /** Move inner node(s) to the graph that owns ``outerSubgraphNodeId`` (main canvas or parent drill). */
  onPromoteInnerSubtreeToOwningGraph?: (rootInnerNodeId: string) => void;
  /** Parent sets this so backdrop click can discard edits like the Cancel button. */
  cancelHandlerRef: MutableRefObject<(() => void) | null>;
  onActivityHint?: (message: string) => void;
};

function SubgraphDrillCanvas({
  t,
  hydrateNonce,
  outerSubgraphNodeId,
  initialDoc,
  framePorts,
  hubInHint,
  hubOutHint,
  defaultHandleOrientation,
  workflowScopeDoc,
  onPatchWorkflowScope,
  onEnsureSubgraphBoundary,
  onApplyPortsForOuterSubgraph,
  onSave,
  onClose,
  innerFlowRootRef,
  schemaSpace,
  nestDepth = 0,
  onPromoteInnerSubtreeToOwningGraph,
  cancelHandlerRef,
  onActivityHint,
}: InnerProps) {
  const { theme } = useAppSettings();
  const { getNode, getNodes, getEdges, fitView, screenToFlowPosition, getZoom } = useReactFlow();
  const rfSelectionRef = useRef<Node[]>([]);
  const [alignableSelectionCount, setAlignableSelectionCount] = useState(0);
  const [selectedRfNodeIds, setSelectedRfNodeIds] = useState<string[]>([]);
  useOnSelectionChange({
    onChange: useCallback(({ nodes: sel }) => {
      rfSelectionRef.current = sel;
      setSelectedRfNodeIds(sel.map((n) => n.id));
      setAlignableSelectionCount(sel.filter((n) => n.type !== "discoveryStart" && n.type !== "discoveryEnd").length);
    }, []),
  });

  const [handleOrientation, setHandleOrientation] = useState<WorkflowCanvasHandleOrientation>(() =>
    normalizeWorkflowCanvasHandleOrientation(
      (initialDoc.handle_orientation ?? defaultHandleOrientation) as WorkflowCanvasHandleOrientation
    )
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(canvasToFlowNodes(initialDoc.nodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(canvasToFlowEdges(initialDoc.edges));
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [editorModalNode, setEditorModalNode] = useState<Node | null>(null);
  const [nestedDrillNodeId, setNestedDrillNodeId] = useState<string | null>(null);
  const [nestedHydrateNonce, setNestedHydrateNonce] = useState(0);
  const [connectEndMenu, setConnectEndMenu] = useState<ConnectEndMenuState | null>(null);
  const [connectEndMenuGroupId, setConnectEndMenuGroupId] = useState<string | null>(null);
  const nestedCancelHandlerRef = useRef<(() => void) | null>(null);

  const compileDagMode = useMemo(() => readCompileWorkflowDagMode(workflowScopeDoc), [workflowScopeDoc]);

  const flowCtxMenu = useTreeContextMenuState();
  const panel = useFlowPanelLayout();

  const historySuspendRef = useRef(false);
  const flowHistory = useFlowCanvasHistory({
    nodes,
    edges,
    setNodes,
    setEdges,
    handleOrientation,
    setHandleOrientation,
    suspendRef: historySuspendRef,
    flowRootRef: innerFlowRootRef,
  });

  const flowClipboard = useFlowClipboard({
    nodes,
    edges,
    setNodes,
    setEdges,
    rfSelectionRef,
    flowRootRef: innerFlowRootRef,
  });

  const patchWorkflowScopeRef = useRef(onPatchWorkflowScope);
  patchWorkflowScopeRef.current = onPatchWorkflowScope;
  const onSaveRef = useRef(onSave);
  onSaveRef.current = onSave;
  const skipEmitRef = useRef(false);
  /** Post-hydration inner canvas; Cancel restores parent to this snapshot. */
  const sessionBaselineRef = useRef<WorkflowCanvasDocument | null>(null);

  const initialDocRef = useRef(initialDoc);
  initialDocRef.current = initialDoc;
  const framePortsRef = useRef(framePorts);
  framePortsRef.current = framePorts;
  const hubInRef = useRef(hubInHint);
  hubInRef.current = hubInHint;
  const hubOutRef = useRef(hubOutHint);
  hubOutRef.current = hubOutHint;
  const onEnsureRef = useRef(onEnsureSubgraphBoundary);
  onEnsureRef.current = onEnsureSubgraphBoundary;

  useEffect(() => {
    const raw = initialDocRef.current;
    const ensured = ensureSubgraphInnerBoundaryCanvasDocument(
      raw,
      framePortsRef.current,
      hubInRef.current,
      hubOutRef.current
    );
    const ho = normalizeWorkflowCanvasHandleOrientation(
      (ensured.doc.handle_orientation ?? defaultHandleOrientation) as WorkflowCanvasHandleOrientation
    );
    setHandleOrientation(ho);
    const snap: FlowCanvasSnapshot = {
      nodes: canvasToFlowNodes(ensured.doc.nodes),
      edges: canvasToFlowEdges(ensured.doc.edges),
      handleOrientation: ho,
    };
    flowHistory.reset(snap);
    setNodes(snap.nodes);
    setEdges(snap.edges);
    sessionBaselineRef.current = cloneWorkflowCanvasDocument(ensured.doc);
    skipEmitRef.current = true;
    if (ensured.mutatedBoundaryMeta) {
      onEnsureRef.current?.(outerSubgraphNodeId, ensured.hubInId, ensured.hubOutId);
    }
    const tid = window.setTimeout(() => fitView({ padding: 0.12 }), 0);
    return () => window.clearTimeout(tid);
  }, [
    hydrateNonce,
    outerSubgraphNodeId,
    setNodes,
    setEdges,
    fitView,
    flowHistory.reset,
    defaultHandleOrientation,
  ]);

  const workflowCanvas = useMemo(
    () => flowToCanvasDocument(nodes, edges, { handleOrientation }),
    [nodes, edges, handleOrientation]
  );

  const edgesRef = useRef(edges);
  edgesRef.current = edges;
  const handleOrientationRef = useRef(handleOrientation);
  handleOrientationRef.current = handleOrientation;

  const patchWorkflowCanvas = useCallback(
    (
      patch:
        | WorkflowCanvasDocument
        | ((prev: WorkflowCanvasDocument) => WorkflowCanvasDocument)
    ) => {
      setNodes((nds) => {
        const prev = flowToCanvasDocument(nds, edgesRef.current, {
          handleOrientation: handleOrientationRef.current,
        });
        const next = typeof patch === "function" ? patch(prev) : patch;
        setEdges(canvasToFlowEdges(next.edges));
        return canvasToFlowNodes(next.nodes);
      });
    },
    [setNodes, setEdges]
  );

  const onActivityHintRef = useRef(onActivityHint);
  onActivityHintRef.current = onActivityHint;

  const toggleNodeEnabled = useCallback(
    (nodeId: string) => {
      patchWorkflowCanvas((prev) =>
        applyWorkflowCanvasEnablementPatch(prev, nodeId, undefined, {
          t,
          onHint: onActivityHintRef.current,
        })
      );
    },
    [patchWorkflowCanvas, t]
  );

  useEffect(() => {
    if (skipEmitRef.current) {
      skipEmitRef.current = false;
      return;
    }
    const doc = flowToCanvasDocument(nodes, edges, { handleOrientation });
    onSaveRef.current(doc);
  }, [nodes, edges, handleOrientation]);

  const handleCancel = useCallback(() => {
    const b = sessionBaselineRef.current;
    if (b) {
      skipEmitRef.current = true;
      onSaveRef.current(cloneWorkflowCanvasDocument(b));
    }
    setConnectEndMenu(null);
    setConnectEndMenuGroupId(null);
    setNestedDrillNodeId(null);
    setEditorModalNode(null);
    onClose();
  }, [onClose]);

  useEffect(() => {
    cancelHandlerRef.current = handleCancel;
    return () => {
      cancelHandlerRef.current = null;
    };
  }, [cancelHandlerRef, handleCancel]);

  const isValidConnection = useCallback(
    (c: Connection | Edge) => isValidDiscoveryFlowConnection(getNode, c, discoveryValidationRuleLayoutRfTypes, compileDagMode),
    [getNode, compileDagMode]
  );

  const onConnect = useCallback(
    (params: Connection) => {
      setEdges((eds) => appendDiscoveryConnectionEdge(getNode, eds, params));
    },
    [setEdges, getNode]
  );

  const onConnectEnd = useCallback(
    (event: MouseEvent | TouchEvent, connectionState: FinalConnectionState) => {
      const cs = connectionState;
      if (!cs.fromNode || !cs.fromHandle) return;
      if (cs.fromHandle.type !== "source") return;
      if (cs.isValid === true) return;
      const leaf = (event as unknown as { target?: EventTarget | null }).target;
      if (!(leaf instanceof Element) || !leaf.closest(".react-flow__pane")) return;
      const coords =
        "changedTouches" in event && event.changedTouches?.length
          ? { x: event.changedTouches[0]!.clientX, y: event.changedTouches[0]!.clientY }
          : { x: (event as MouseEvent).clientX, y: (event as MouseEvent).clientY };
      const srcNode = getNode(cs.fromNode.id);
      const st = srcNode?.type ?? (cs.fromNode as Node).type;
      const handleId = cs.fromHandle.id ?? "out";
      const opts = connectEndMenuOptionsForSourceType(st, handleId, compileDagMode);
      if (opts.length === 0) return;
      const flow = screenToFlowPosition(coords);
      setConnectEndMenu({
        screen: coords,
        flow,
        sourceNodeId: cs.fromNode.id,
        sourceHandleId: handleId,
      });
      setConnectEndMenuGroupId(null);
    },
    [getNode, screenToFlowPosition, compileDagMode]
  );

  const commitConnectEndMenu = useCallback(
    (payload: PaletteDragPayload) => {
      if (!connectEndMenu) return;
      const sourceNode = getNode(connectEndMenu.sourceNodeId);
      if (!sourceNode) {
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        return;
      }
      const result = materializePaletteDrop({
        payload,
        position: connectEndMenu.flow,
        nodes,
        edges,
        workflowScopeDoc,
        patchWorkflowScope: (fn) => patchWorkflowScopeRef.current(fn),
        t,
        allowValidationRuleLayoutReuse: false,
        connectFromRfNode: sourceNode,
      });
      if (result.outcome !== "create") return;
      const { node, extraEdges } = result;
      const conn: Connection = {
        source: connectEndMenu.sourceNodeId,
        sourceHandle: connectEndMenu.sourceHandleId ?? "out",
        target: node.id,
        targetHandle: "in",
      };
      flushSync(() => {
        setNodes((nds) => appendNodeAndResolveSubflowParent(nds, node));
      });
      setEdges((eds) => {
        const merged = dedupeEdgesByHandles([...eds, ...extraEdges]);
        return appendDiscoveryConnectionEdge(getNode, merged, conn);
      });
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setSelectedNode(node);
      setSelectedEdge(null);
    },
    [connectEndMenu, nodes, edges, workflowScopeDoc, t, getNode, setNodes, setEdges]
  );

  useEffect(() => {
    if (!connectEndMenu) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
      }
    };
    const onDocPointerDown = (e: PointerEvent) => {
      const tgt = e.target;
      if (tgt instanceof Element && tgt.closest(".discovery-flow-connect-end-menu")) return;
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
    };
    document.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onDocPointerDown, true);
    return () => {
      document.removeEventListener("keydown", onKey);
      document.removeEventListener("pointerdown", onDocPointerDown, true);
    };
  }, [connectEndMenu]);

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      applyPaletteCanvasDrop({
        event: e,
        screenToFlowPosition,
        getNode,
        getEdges,
        zoom: getZoom(),
        nodes,
        edges,
        workflowScopeDoc,
        patchWorkflowScope: (fn) => patchWorkflowScopeRef.current(fn),
        t,
        schemaSpace,
        compileDagMode,
        allowValidationRuleLayoutReuse: true,
        setNodes,
        setEdges,
        onSelectNodeId: (nodeId) => {
          const n = getNode(nodeId);
          if (n) {
            setSelectedNode(n);
            setSelectedEdge(null);
          }
        },
      });
    },
    [
      screenToFlowPosition,
      setNodes,
      setEdges,
      nodes,
      edges,
      workflowScopeDoc,
      getNode,
      getEdges,
      getZoom,
      t,
      schemaSpace,
      compileDagMode,
    ]
  );

  const patchNode = useCallback(
    (nodeId: string, data: Record<string, unknown>) => {
      setNodes((nds) => nds.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n)));
    },
    [setNodes]
  );

  const patchNestedInnerInLocalNodes = useCallback((nestedSubgraphId: string, inner: WorkflowCanvasDocument) => {
    setNodes((nds) => {
      const sg = nds.find((x) => x.id === nestedSubgraphId && x.type === "discoverySubgraph");
      if (!sg) return nds;
      const data = (sg.data ?? {}) as WorkflowCanvasNodeData;
      const frame: SubflowPortsConfig =
        data.subflow_ports?.inputs?.length || data.subflow_ports?.outputs?.length
          ? (data.subflow_ports as SubflowPortsConfig)
          : { inputs: [{ id: "in", label: "in" }], outputs: [{ id: "out", label: "out" }] };
      const hubIn = String(data.subflow_hub_input_id ?? "").trim();
      const hubOut = String(data.subflow_hub_output_id ?? "").trim();
      const ensured = ensureSubgraphInnerBoundaryCanvasDocument(inner, frame, hubIn, hubOut);
      const metaPatch: Partial<WorkflowCanvasNodeData> = ensured.mutatedBoundaryMeta
        ? { subflow_hub_input_id: ensured.hubInId, subflow_hub_output_id: ensured.hubOutId }
        : {};
      return nds.map((n) => {
        if (n.id !== nestedSubgraphId || n.type !== "discoverySubgraph") return n;
        return {
          ...n,
          data: {
            ...((n.data ?? {}) as WorkflowCanvasNodeData),
            inner_canvas: ensured.doc,
            ...metaPatch,
          } as Record<string, unknown>,
        };
      });
    });
  }, [setNodes]);

  const patchNestedBoundaryHubIds = useCallback((nestedSubgraphId: string, hubInId: string, hubOutId: string) => {
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id !== nestedSubgraphId || n.type !== "discoverySubgraph") return n;
        const data = (n.data ?? {}) as WorkflowCanvasNodeData;
        return {
          ...n,
          data: {
            ...data,
            subflow_hub_input_id: hubInId,
            subflow_hub_output_id: hubOutId,
          } as Record<string, unknown>,
        };
      })
    );
  }, [setNodes]);

  const applySubflowPorts = useCallback(
    (subflowId: string, ports: SubflowPortsConfig) => {
      if (subflowId === outerSubgraphNodeId && onApplyPortsForOuterSubgraph) {
        onApplyPortsForOuterSubgraph(ports);
        return;
      }
      const cur = getNodes().find((n) => n.id === subflowId);
      if (!cur || cur.type !== "discoverySubgraph") return;
      const prev = ((cur?.data ?? {}) as WorkflowCanvasNodeData).subflow_ports;
      const hubIn = String((cur?.data as WorkflowCanvasNodeData | undefined)?.subflow_hub_input_id ?? "").trim();
      const hubOut = String((cur?.data as WorkflowCanvasNodeData | undefined)?.subflow_hub_output_id ?? "").trim();

      setNodes((nds) =>
        nds.map((n) => {
          if (n.id !== subflowId) return n;
          const data = (n.data ?? {}) as WorkflowCanvasNodeData;
          let nextData: WorkflowCanvasNodeData = { ...data, subflow_ports: ports };
          if (n.type === "discoverySubgraph" && data.inner_canvas && hubIn && hubOut) {
            let inner = syncSubgraphInnerHubPortData(data.inner_canvas, hubIn, hubOut, ports);
            if (prev) {
              const removedIn = prev.inputs.filter((p) => !ports.inputs.some((q) => q.id === p.id)).map((p) => p.id);
              const removedOut = prev.outputs.filter((p) => !ports.outputs.some((q) => q.id === p.id)).map((p) => p.id);
              inner = pruneSubgraphInnerPortEdges(inner, hubIn, hubOut, removedIn, removedOut);
            }
            nextData = { ...nextData, inner_canvas: inner };
          }
          return { ...n, data: nextData as Record<string, unknown> };
        })
      );

      if (!prev) return;
      const removedIn = prev.inputs.filter((p) => !ports.inputs.some((q) => q.id === p.id)).map((p) => p.id);
      const removedOut = prev.outputs.filter((p) => !ports.outputs.some((q) => q.id === p.id)).map((p) => p.id);
      if (removedIn.length === 0 && removedOut.length === 0) return;

      setEdges((eds) =>
        eds.filter((e) => {
          if (e.target === subflowId) {
            const pid = parsePortIdFromSubflowTargetHandle(e.targetHandle ?? undefined);
            if (pid && removedIn.includes(pid)) return false;
          }
          if (e.source === subflowId) {
            const pid = parsePortIdFromSubflowSourceHandle(e.sourceHandle ?? undefined);
            if (pid && removedOut.includes(pid)) return false;
          }
          if (hubIn && e.source === hubIn) {
            const pid = parsePortIdFromSubflowSourceHandle(e.sourceHandle ?? undefined);
            if (pid && removedIn.includes(pid)) return false;
          }
          if (hubOut && e.target === hubOut) {
            const pid = parsePortIdFromSubflowTargetHandle(e.targetHandle ?? undefined);
            if (pid && removedOut.includes(pid)) return false;
          }
          return true;
        })
      );
    },
    [getNodes, setNodes, setEdges, outerSubgraphNodeId, onApplyPortsForOuterSubgraph]
  );

  const boundaryHubInNode = nodes.find((x) => x.type === "discoverySubflowGraphIn");
  const boundaryHubOutNode = nodes.find((x) => x.type === "discoverySubflowGraphOut");
  const boundaryHubInId = hubInHint.trim() || boundaryHubInNode?.id || "";
  const boundaryHubOutId = hubOutHint.trim() || boundaryHubOutNode?.id || "";

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

  const openNestedDrill = useCallback((id: string) => {
    setEditorModalNode(null);
    setNestedDrillNodeId(id);
    setNestedHydrateNonce((n) => n + 1);
  }, []);

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

  const onNodeDragStop = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const nds0 = getNodes();
      const eds0 = getEdges();
      const sub = resolveSubflowParentsAfterGroupDrag(nds0, node.id);
      const afterSub = sub ?? nds0;
      const adopt = resolveAdoptIntoSubgraphAfterDrag(afterSub, eds0, node.id, handleOrientation);
      if (adopt) {
        setNodes(adopt.nodes);
        setEdges(adopt.edges);
        return;
      }
      if (sub) setNodes(sub);
    },
    [getNodes, getEdges, setNodes, setEdges, handleOrientation]
  );

  const onNodeDoubleClick = useCallback(
    (e: React.MouseEvent, node: Node) => {
      e.preventDefault();
      if (node.type === "discoverySubgraph") {
        openNestedDrill(node.id);
        setSelectedNode(node);
        setSelectedEdge(null);
        return;
      }
      setEditorModalNode(node);
      setSelectedNode(node);
      setSelectedEdge(null);
    },
    [openNestedDrill]
  );

  const handleAutoLayout = useCallback(() => {
    setNodes((nds) => layoutFlowNodes(nds, edges, handleOrientation, workflowScopeDoc));
    window.setTimeout(() => fitView({ padding: 0.15 }), 0);
  }, [edges, setNodes, fitView, handleOrientation, workflowScopeDoc]);

  const applySelectionAlign = useCallback(
    (mode: AlignFlowSelectionMode) => {
      setNodes((nds) => {
        const next = alignSelectedFlowNodes(nds, rfSelectionRef.current, mode);
        if (!next) return nds;
        const movableIds = rfSelectionRef.current
          .filter((n) => n.type !== "discoveryStart" && n.type !== "discoveryEnd")
          .map((n) => n.id);
        let clamped = next;
        for (const id of movableIds) {
          clamped = clampNodeInsideParentSubflowFrame(clamped, id);
        }
        return clamped;
      });
    },
    [setNodes]
  );

  const onHandleOrientationChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const next = normalizeWorkflowCanvasHandleOrientation(e.target.value);
      setHandleOrientation(next);
      setNodes((nds) => layoutFlowNodes(nds, edges, next, workflowScopeDoc));
      window.setTimeout(() => fitView({ padding: 0.15 }), 0);
    },
    [edges, setNodes, fitView, workflowScopeDoc]
  );

  const removeNodesByIds = useCallback(
    (nodeIds: string[]) => {
      const result = applyFlowNodeRemovals(
        getNodes(),
        getEdges(),
        nodeIds,
        handleOrientation,
        (msg) => window.confirm(msg),
        t("flow.confirmSubgraphDeleteLift")
      );
      if (!result) return;
      setNodes(result.nodes);
      setEdges(result.edges);
      setSelectedNode((sn) => (sn && result.clearedSelectionNodeIds.has(sn.id) ? null : sn));
      setSelectedEdge(null);
    },
    [getNodes, getEdges, setNodes, setEdges, t, handleOrientation]
  );

  const removeNodeById = useCallback((nodeId: string) => removeNodesByIds([nodeId]), [removeNodesByIds]);

  const removeEdgeById = useCallback(
    (edgeId: string) => {
      setEdges((eds) => eds.filter((e) => e.id !== edgeId));
      setSelectedEdge((se) => (se?.id === edgeId ? null : se));
    },
    [setEdges]
  );

  const selectionAnimatedEdgeIds = useMemo(
    () => upstreamDownstreamAnimatedEdgeIds(edges, selectedRfNodeIds),
    [edges, selectedRfNodeIds]
  );
  const edgesForRf = useMemo(
    () => edges.map((e) => ({ ...e, animated: selectionAnimatedEdgeIds.has(e.id) })),
    [edges, selectionAnimatedEdgeIds]
  );

  const onNodesChangeWrapped = useCallback(
    (changes: NodeChange[]) => {
      const removals = changes.filter((c): c is Extract<NodeChange, { type: "remove" }> => c.type === "remove");
      const rest = changes.filter((c) => c.type !== "remove");
      if (rest.length) onNodesChange(rest);
      if (removals.length === 0) return;
      const ids = [...new Set(removals.map((r) => r.id))];
      flushSync(() => {
        removeNodesByIds(ids);
      });
    },
    [onNodesChange, removeNodesByIds]
  );

  const onPaneContextMenu = useCallback(
    (e: React.MouseEvent | MouseEvent) => {
      flowCtxMenu.open(e, [
        {
          id: "copy",
          label: t("flow.ctxMenuCopy"),
          onSelect: () => flowClipboard.copySelection(),
        },
        {
          id: "paste",
          label: t("flow.ctxMenuPaste"),
          onSelect: () => void flowClipboard.pasteClipboard(),
        },
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
      ]);
    },
    [flowCtxMenu, t, fitView, handleAutoLayout, flowClipboard]
  );

  const onFlowNodeContextMenu = useCallback(
    (e: React.MouseEvent, node: Node) => {
      setSelectedNode(node);
      setSelectedEdge(null);
      if (node.type === "discoveryStart" || node.type === "discoveryEnd") return;
      if (isSubflowGraphHubRfType(node.type)) return;
      const nds = getNodes();
      const groupableSelected = resolveGroupableSelectionNodes(nds, node, rfSelectionRef.current);
      const showGroupActions = groupableSelected.length >= 1;
      const items: TreeCtxMenuItem[] = [
        {
          id: "copy",
          label: t("flow.ctxMenuCopy"),
          onSelect: () => flowClipboard.copySelection(),
        },
        {
          id: "paste",
          label: t("flow.ctxMenuPaste"),
          onSelect: () => void flowClipboard.pasteClipboard(),
        },
      ];
      if (showGroupActions) {
        items.push({
          id: "collapse-subgraph",
          label: t("flow.ctxMenuCollapseSelectionToSubgraph"),
          onSelect: () => {
            const nds2 = getNodes();
            const eds = getEdges();
            const sel = resolveGroupableSelectionNodes(nds2, node, rfSelectionRef.current);
            const res = collapseSelectionToSubgraph(nds2, eds, sel, handleOrientation);
            if (!res) return;
            setNodes(res.nodes);
            setEdges(res.edges);
            setSelectedNode(null);
            setSelectedEdge(null);
          },
        });
      }
      if (
        boundaryHubInId &&
        boundaryHubOutId &&
        onPromoteInnerSubtreeToOwningGraph &&
        canPromoteInnerSubtreeToOwningGraph(nds, boundaryHubInId, boundaryHubOutId, node.id)
      ) {
        items.push({
          id: "promote-to-owning-graph",
          label: t("flow.ctxMenuPromoteNodeToOwningGraph"),
          onSelect: () => {
            onPromoteInnerSubtreeToOwningGraph(node.id);
            setSelectedNode(null);
            setSelectedEdge(null);
          },
        });
      }
      if (node.type === "discoverySubgraph" && subgraphHasLiftableInnerContent(nds, node.id)) {
        items.push({
          id: "flatten-subgraph",
          label: t("flow.ctxMenuFlattenSubgraph"),
          onSelect: () => {
            const nds2 = getNodes();
            const eds = getEdges();
            const lifted = liftSubgraphInnerToParentWorkflow(nds2, eds, node.id, handleOrientation);
            if (!lifted) return;
            setNodes(lifted.nodes);
            setEdges(lifted.edges);
            setSelectedNode(null);
            setSelectedEdge(null);
          },
        });
      }
      if (node.type && discoveryWorkflowDisableableRfTypes.has(node.type)) {
        const cn = workflowCanvas.nodes.find((n) => n.id === node.id);
        const enabled = cn
          ? isWorkflowCanvasNodeEnabled(cn)
          : (node.data as WorkflowCanvasNodeData).canvas_node_enabled !== false;
        items.push({
          id: "toggle-enabled",
          label: enabled ? t("flow.ctxMenuDisableNode") : t("flow.ctxMenuEnableNode"),
          onSelect: () => toggleNodeEnabled(node.id),
        });
      }
      items.push({
        id: "remove-node",
        label: t("flow.ctxMenuRemoveNode"),
        danger: true,
        onSelect: () => removeNodeById(node.id),
      });
      flowCtxMenu.open(e, items);
    },
    [
      flowCtxMenu,
      t,
      removeNodeById,
      toggleNodeEnabled,
      workflowCanvas,
      getNodes,
      getEdges,
      setNodes,
      setEdges,
      handleOrientation,
      boundaryHubInId,
      boundaryHubOutId,
      onPromoteInnerSubtreeToOwningGraph,
      flowClipboard,
    ]
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

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      if (nestedDrillNodeId) return;
      if (connectEndMenu) {
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        return;
      }
      handleCancel();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [handleCancel, nestedDrillNodeId, connectEndMenu]);

  const nestedNode = nestedDrillNodeId ? (nodes.find((n) => n.id === nestedDrillNodeId) ?? null) : null;
  useEffect(() => {
    if (nestedDrillNodeId && (!nestedNode || nestedNode.type !== "discoverySubgraph")) {
      setNestedDrillNodeId(null);
    }
  }, [nestedDrillNodeId, nestedNode]);

  const zBase = 1250 + nestDepth * 15;
  const menuZ = zBase + 30;

  return (
    <>
      <div
        className="discovery-modal__body discovery-modal__body--scroll discovery-modal__body--subgraph-drill-flow"
        style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}
      >
        <div className="discovery-flow-shell">
          <div
            className={`discovery-flow-shell__left${panel.leftCollapsed ? " discovery-flow-shell__left--collapsed" : ""}`}
            style={
              panel.leftCollapsed
                ? { flex: `0 0 ${panel.collapsedStripPx}px`, width: panel.collapsedStripPx }
                : {
                    flex: `0 0 ${panel.leftWidth}px`,
                    width: panel.leftWidth,
                    minWidth: panel.leftMin,
                    maxWidth: panel.leftMax,
                  }
            }
          >
            {panel.leftCollapsed ? (
              <button
                type="button"
                className="discovery-flow-shell__reveal discovery-flow-shell__reveal--left"
                aria-expanded={false}
                aria-label={t("flow.expandLeftPanel")}
                title={t("flow.expandLeftPanel")}
                onClick={panel.expandLeft}
              >
                ›
              </button>
            ) : (
              <>
                <div className="discovery-flow-shell__panel-bar">
                  <span className="discovery-flow-shell__panel-bar-title">{t("flow.leftPanelTitle")}</span>
                  <button
                    type="button"
                    className="discovery-btn discovery-btn--sm discovery-flow-shell__panel-bar-btn"
                    aria-expanded
                    aria-label={t("flow.collapseLeftPanel")}
                    title={t("flow.collapseLeftPanel")}
                    onClick={panel.collapseLeft}
                  >
                    ‹
                  </button>
                </div>
                <FlowPalette t={t} scopeDocument={workflowScopeDoc} schemaSpace={schemaSpace} />
              </>
            )}
          </div>
          {!panel.leftCollapsed && (
            <div
              role="separator"
              aria-orientation="vertical"
              aria-label={t("flow.resizePanels")}
              className="discovery-flow-shell__resize"
              onMouseDown={panel.onResizeLeftStart}
            />
          )}
          <div className="discovery-flow-main">
            <div className="discovery-flow-toolbar">
              <FlowSelectionAlignButtons
                t={t}
                disabled={alignableSelectionCount < 2}
                onAlign={applySelectionAlign}
              />
              <label className="discovery-flow-toolbar__orientation">
                <span className="discovery-hint" style={{ margin: 0, whiteSpace: "nowrap" }}>
                  {t("flow.handleOrientationLabel")}
                </span>
                <select
                  className="discovery-select"
                  style={{ marginTop: 0, width: "auto", minWidth: "10rem" }}
                  value={handleOrientation}
                  onChange={onHandleOrientationChange}
                  aria-label={t("flow.handleOrientationLabel")}
                >
                  <option value="lr">{t("flow.handleOrientationLr")}</option>
                  <option value="tb">{t("flow.handleOrientationTb")}</option>
                </select>
              </label>
              <span className="discovery-hint" style={{ marginLeft: "0.5rem", flex: "1 1 12rem" }}>
                {t("flow.subgraphDrillHint")}
              </span>
            </div>
            <div className="discovery-flow-canvas-wrap" ref={innerFlowRootRef as RefObject<HTMLDivElement>}>
              <FlowHandleOrientationProvider value={handleOrientation}>
                <ReactFlow
                  colorMode={theme}
                  nodes={nodes}
                  edges={edgesForRf}
                  onNodesChange={onNodesChangeWrapped}
                  onEdgesChange={onEdgesChange}
                  isValidConnection={isValidConnection}
                  onConnect={onConnect}
                  onConnectEnd={onConnectEnd}
                  onDrop={onDrop}
                  onDragOver={onDragOver}
                  nodeTypes={DISCOVERY_FLOW_NODE_TYPES}
                  defaultEdgeOptions={{ animated: false }}
                  deleteKeyCode={["Backspace", "Delete"]}
                  onNodeClick={onNodeClick}
                  onEdgeClick={onEdgeClick}
                  onPaneClick={onPaneClick}
                  onNodeDoubleClick={onNodeDoubleClick}
                  onNodeDragStop={onNodeDragStop}
                  onPaneContextMenu={onPaneContextMenu}
                  onNodeContextMenu={onFlowNodeContextMenu}
                  onEdgeContextMenu={onFlowEdgeContextMenu}
                  fitView
                  minZoom={0.2}
                  maxZoom={1.5}
                  panOnScroll
                  zoomOnScroll
                  zoomOnPinch
                  proOptions={{ hideAttribution: true }}
                >
                  <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
                  <Controls />
                  <MiniMap zoomable pannable className="discovery-flow-minimap" />
                </ReactFlow>
              </FlowHandleOrientationProvider>
              {connectEndMenu && (
                <div
                  className="discovery-flow-connect-end-menu"
                  style={{
                    position: "fixed",
                    left: Math.max(8, connectEndMenu.screen.x),
                    top: Math.max(8, connectEndMenu.screen.y),
                    zIndex: menuZ,
                  }}
                  role="menu"
                  aria-label={t("flow.connectEndMenuAria")}
                >
                  {(() => {
                    const groups = connectEndMenuGroupedOptionsForSourceType(
                      getNode(connectEndMenu.sourceNodeId)?.type,
                      connectEndMenu.sourceHandleId,
                      compileDagMode
                    );
                    const selectedGroup = groups.find((g) => g.id === connectEndMenuGroupId) ?? null;
                    if (!selectedGroup) {
                      return groups.map((g) => (
                        <button
                          key={g.id}
                          type="button"
                          className="discovery-btn discovery-btn--sm discovery-flow-connect-end-menu__item"
                          role="menuitem"
                          onClick={() => setConnectEndMenuGroupId(g.id)}
                        >
                          {g.labelText}
                        </button>
                      ));
                    }
                    return (
                      <>
                        <button
                          type="button"
                          className="discovery-btn discovery-btn--sm discovery-flow-connect-end-menu__item"
                          role="menuitem"
                          onClick={() => setConnectEndMenuGroupId(null)}
                        >
                          ← Back
                        </button>
                        {selectedGroup.options.map((opt) => (
                          <button
                            key={opt.id}
                            type="button"
                            className="discovery-btn discovery-btn--sm discovery-flow-connect-end-menu__item"
                            role="menuitem"
                            title={formatConnectEndMenuOptionTooltip(opt, t)}
                            onClick={() => commitConnectEndMenu(opt.payload)}
                          >
                            {formatConnectEndMenuOptionLabel(opt, t)}
                          </button>
                        ))}
                      </>
                    );
                  })()}
                </div>
              )}
            </div>
          </div>
          {!panel.rightCollapsed && (
            <div
              role="separator"
              aria-orientation="vertical"
              aria-label={t("flow.resizePanels")}
              className="discovery-flow-shell__resize"
              onMouseDown={panel.onResizeRightStart}
            />
          )}
          <div
            className={`discovery-flow-shell__right${panel.rightCollapsed ? " discovery-flow-shell__right--collapsed" : ""}`}
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
                className="discovery-flow-shell__reveal discovery-flow-shell__reveal--right"
                aria-expanded={false}
                aria-label={t("flow.expandRightPanel")}
                title={t("flow.expandRightPanel")}
                onClick={panel.expandRight}
              >
                ‹
              </button>
            ) : (
              <>
                <div className="discovery-flow-shell__panel-bar discovery-flow-shell__panel-bar--end">
                  <button
                    type="button"
                    className="discovery-btn discovery-btn--sm discovery-flow-shell__panel-bar-btn"
                    aria-expanded
                    aria-label={t("flow.collapseRightPanel")}
                    title={t("flow.collapseRightPanel")}
                    onClick={panel.collapseRight}
                  >
                    ›
                  </button>
                  <span className="discovery-flow-shell__panel-bar-title">{t("flow.rightPanelTitle")}</span>
                </div>
                <FlowNodeInspector
                  t={t}
                  selectedNode={selectedNode}
                  selectedEdge={selectedEdge}
                  workflowDoc={workflowScopeDoc}
                  flowNodes={nodes}
                  onPatchWorkflowScope={onPatchWorkflowScope}
                  onPatchNode={patchNode}
                  onPatchEdge={patchEdge}
                  onApplySubflowPorts={applySubflowPorts}
                  onOpenSubgraphDrill={openNestedDrill}
                  drillBoundaryPorts={
                    onApplyPortsForOuterSubgraph
                      ? {
                          targetSubgraphId: outerSubgraphNodeId,
                          ports: framePorts,
                          hubInId: boundaryHubInId,
                          hubOutId: boundaryHubOutId,
                        }
                      : undefined
                  }
                />
              </>
            )}
          </div>
        </div>
      </div>
      <div
        className="discovery-modal__actions"
        style={{
          marginTop: "0.75rem",
          flexShrink: 0,
          display: "flex",
          flexWrap: "wrap",
          gap: "0.5rem",
          justifyContent: "flex-end",
        }}
      >
        <button
          type="button"
          className="discovery-btn"
          onClick={handleCancel}
          disabled={Boolean(nestedDrillNodeId)}
          title={nestedDrillNodeId ? t("flow.subgraphCancelNestedTooltip") : undefined}
        >
          {t("flow.subgraphCancel")}
        </button>
        <button type="button" className="discovery-btn discovery-btn--primary" onClick={onClose}>
          {t("flow.subgraphBack")}
        </button>
      </div>
      <TreeContextMenuPortal menu={flowCtxMenu.menu} onClose={flowCtxMenu.close} classPrefix="discovery" />
      {editorModalNode && (
        <FlowNodeEditorModal
          node={editorModalNode}
          workflowDoc={workflowScopeDoc}
          onPatchWorkflowScope={onPatchWorkflowScope}
          onClose={() => setEditorModalNode(null)}
          t={t}
          schemaSpace={schemaSpace}
          workflowCanvas={workflowCanvas}
          onPatchWorkflowCanvas={patchWorkflowCanvas}
        />
      )}
      {nestedNode && nestedNode.type === "discoverySubgraph" && (
        <SubgraphDrillModal
          t={t}
          open
          nestDepth={nestDepth + 1}
          schemaSpace={schemaSpace}
          node={nestedNode}
          hydrateNonce={nestedHydrateNonce}
          handleOrientation={handleOrientation}
          workflowScopeDoc={workflowScopeDoc}
          onPatchWorkflowScope={onPatchWorkflowScope}
          onClose={() => setNestedDrillNodeId(null)}
          cancelHandlerRef={nestedCancelHandlerRef}
          onActivityHint={onActivityHint}
          onSaveInnerCanvas={(childId, doc) => patchNestedInnerInLocalNodes(childId, doc)}
          onEnsureSubgraphBoundary={(childId, hubInId, hubOutId) =>
            patchNestedBoundaryHubIds(childId, hubInId, hubOutId)
          }
          onApplyPortsForOuterSubgraph={(ports) => {
            applySubflowPorts(nestedNode.id, ports);
            setNestedHydrateNonce((n) => n + 1);
          }}
          onPromoteInnerSubtreeToOwningGraph={(nestedSubgraphId, rootInnerId) => {
            const nds2 = getNodes();
            const eds2 = getEdges();
            const res = promoteSubgraphInnerSubtreeToParentWorkflow(
              nds2,
              eds2,
              nestedSubgraphId,
              rootInnerId,
              handleOrientation
            );
            if (!res) return;
            setNodes(res.nodes);
            setEdges(res.edges);
            setNestedDrillNodeId(null);
            setSelectedNode(null);
            setSelectedEdge(null);
          }}
        />
      )}
    </>
  );
}

type Props = {
  t: TFn;
  open: boolean;
  node: Node | null;
  hydrateNonce: number;
  handleOrientation: WorkflowCanvasHandleOrientation;
  workflowScopeDoc: Record<string, unknown>;
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onClose: () => void;
  onSaveInnerCanvas: (nodeId: string, doc: WorkflowCanvasDocument) => void;
  onEnsureSubgraphBoundary?: (nodeId: string, hubInId: string, hubOutId: string) => void;
  /** When drilling into a subgraph, persist its frame ports (node is on the parent canvas, not this inner RF). */
  onApplyPortsForOuterSubgraph?: (ports: SubflowPortsConfig) => void;
  schemaSpace?: string;
  nestDepth?: number;
  /** Promote inner node(s) from ``subgraphNodeId``'s inner canvas to the owning graph (main or parent drill). */
  onPromoteInnerSubtreeToOwningGraph?: (subgraphNodeId: string, rootInnerNodeId: string) => void;
  /** When omitted, an internal ref is used so backdrop / Escape can discard edits. */
  cancelHandlerRef?: MutableRefObject<(() => void) | null>;
  onActivityHint?: (message: string) => void;
};

export function SubgraphDrillModal({
  t,
  open,
  node,
  hydrateNonce,
  handleOrientation,
  workflowScopeDoc,
  onPatchWorkflowScope,
  onClose,
  onSaveInnerCanvas,
  onEnsureSubgraphBoundary,
  onApplyPortsForOuterSubgraph,
  schemaSpace,
  nestDepth = 0,
  onPromoteInnerSubtreeToOwningGraph,
  cancelHandlerRef: cancelHandlerRefProp,
  onActivityHint,
}: Props) {
  const innerFlowRootRef = useRef<HTMLDivElement>(null);
  const fallbackCancelRef = useRef<(() => void) | null>(null);
  const cancelHandlerRef = cancelHandlerRefProp ?? fallbackCancelRef;
  const wfData = (node?.data ?? {}) as WorkflowCanvasNodeData;
  const initialDoc =
    wfData.inner_canvas && wfData.inner_canvas.nodes && Array.isArray(wfData.inner_canvas.nodes)
      ? wfData.inner_canvas
      : emptyWorkflowCanvasDocument();
  const defaultHo = normalizeWorkflowCanvasHandleOrientation(
    (initialDoc.handle_orientation ?? handleOrientation) as WorkflowCanvasHandleOrientation
  );

  const onSave = useCallback(
    (doc: WorkflowCanvasDocument) => {
      if (!node) return;
      onSaveInnerCanvas(node.id, doc);
    },
    [node, onSaveInnerCanvas]
  );

  if (!open || !node || node.type !== "discoverySubgraph") return null;

  const framePorts: SubflowPortsConfig =
    wfData.subflow_ports?.inputs?.length || wfData.subflow_ports?.outputs?.length
      ? (wfData.subflow_ports as SubflowPortsConfig)
      : { inputs: [{ id: "in", label: "in" }], outputs: [{ id: "out", label: "out" }] };

  const zBase = 1250 + nestDepth * 15;

  return createPortal(
    <div
      className="discovery-modal-backdrop discovery-modal-backdrop--subgraph-drill"
      style={{ zIndex: zBase }}
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) cancelHandlerRef.current?.();
      }}
    >
      <div
        className="discovery-modal discovery-modal--subgraph-drill"
        role="dialog"
        aria-modal="true"
        aria-labelledby="discovery-subgraph-drill-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <h2 id="discovery-subgraph-drill-title" className="discovery-modal__title">
          {t("flow.subgraphDrillTitle")} · {String(wfData.label ?? node.id)}
        </h2>
        <ReactFlowProvider>
          <SubgraphDrillCanvas
            t={t}
            hydrateNonce={hydrateNonce}
            outerSubgraphNodeId={node.id}
            initialDoc={initialDoc}
            framePorts={framePorts}
            hubInHint={String(wfData.subflow_hub_input_id ?? "").trim()}
            hubOutHint={String(wfData.subflow_hub_output_id ?? "").trim()}
            defaultHandleOrientation={defaultHo}
            workflowScopeDoc={workflowScopeDoc}
            onPatchWorkflowScope={onPatchWorkflowScope}
            onEnsureSubgraphBoundary={onEnsureSubgraphBoundary}
            onApplyPortsForOuterSubgraph={onApplyPortsForOuterSubgraph}
            onSave={onSave}
            onClose={onClose}
            innerFlowRootRef={innerFlowRootRef}
            schemaSpace={schemaSpace}
            nestDepth={nestDepth}
            cancelHandlerRef={cancelHandlerRef}
            onActivityHint={onActivityHint}
            onPromoteInnerSubtreeToOwningGraph={
              onPromoteInnerSubtreeToOwningGraph
                ? (rootInnerId) => onPromoteInnerSubtreeToOwningGraph(node.id, rootInnerId)
                : undefined
            }
          />
        </ReactFlowProvider>
      </div>
    </div>,
    document.body
  );
}
