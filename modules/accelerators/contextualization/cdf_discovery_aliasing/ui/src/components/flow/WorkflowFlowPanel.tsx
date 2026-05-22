import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";
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
  normalizeWorkflowCanvasHandleOrientation,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
  type SubflowPortsConfig,
  type WorkflowCanvasDocument,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import {
  applyDiscoveryFlowNodeDisplayClasses,
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import { WorkflowCanvasSearchField, WorkflowCanvasSearchResults } from "./WorkflowCanvasSearch";
import {
  canvasNodeMatchesSearch,
  filterCanvasNodesBySearch,
} from "../../utils/workflowCanvasFlowSearch";
import { appendDiscoveryConnectionEdge, dedupeEdgesByHandles } from "./flowEdgeHelpers";
import { applyPaletteCanvasDrop } from "./paletteDropOnEdge";
import { FlowNodeInspector } from "./FlowNodeInspector";
import { FlowPalette } from "./FlowPalette";
import { useFlowPanelLayout } from "./useFlowPanelLayout";
import { layoutFlowNodes } from "./autoLayoutFlow";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import { FlowNodeEditorModal } from "./FlowNodeEditorModal";
import { DISCOVERY_FLOW_NODE_TYPES } from "./flowNodeRegistry";
import { TreeContextMenuPortal, useTreeContextMenuState, type TreeCtxMenuItem } from "../TreeContextMenu";
import { applyFlowNodeRemovals } from "./applyFlowNodeRemovals";
import { liftSubgraphInnerToParentWorkflow, subgraphHasLiftableInnerContent } from "./liftSubgraphInnerToParent";
import { clampNodeInsideParentSubflowFrame } from "./subflowGroupClamp";
import { promoteSubgraphInnerSubtreeToParentWorkflow } from "./promoteSubgraphInnerNodeToParent";
import { appendNodeAndResolveSubflowParent, resolveSubflowParentsAfterGroupDrag } from "./subflowDropAssociation";
import { resolveGroupableSelectionNodes } from "./subflowMembership";
import { isValidDiscoveryFlowConnection } from "./subgraphFlowConnections";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import { resolveAdoptIntoSubgraphAfterDrag } from "./adoptNodesIntoSubgraph";
import { useFlowCanvasHistory, type FlowCanvasSnapshot } from "./useFlowCanvasHistory";
import { useFlowClipboard } from "./useFlowClipboard";
import {
  ensureSubgraphInnerBoundaryCanvasDocument,
  pruneSubgraphInnerPortEdges,
  syncSubgraphInnerHubPortData,
} from "./subgraphInnerBoundaryHubs";
import { SubgraphDrillModal } from "./SubgraphDrillModal";
import { materializePaletteDrop } from "./materializePaletteDrop";
import { alignSelectedFlowNodes, type AlignFlowSelectionMode } from "./alignSelectedNodes";
import { FlowSelectionAlignButtons } from "./FlowSelectionAlignButtons";
import {
  connectEndMenuGroupedOptionsForSourceType,
  connectEndMenuOptionsForSourceType,
  formatConnectEndMenuOptionLabel,
  formatConnectEndMenuOptionTooltip,
} from "./connectEndMenuOptions";
import { discoveryValidationRuleLayoutRfTypes, discoveryWorkflowDisableableRfTypes } from "./flowConstants";
import { isWorkflowCanvasNodeEnabled } from "../../types/workflowCanvas";
import { applyWorkflowCanvasEnablementPatch } from "./flowNodeEnabled";
import type { PaletteDragPayload } from "./FlowPalette";
import { readCompileWorkflowDagMode } from "../../utils/workflowCompileMode";
import { upstreamDownstreamAnimatedEdgeIds } from "./flowSelectionEdgeAnimation";
import type { WorkflowPreviewRunProgress } from "./WorkflowFlowCanvasPreview";
import { runProgressAnimatedEdgeIds } from "./flowRunProgressEdges";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type ConnectEndMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
  sourceNodeId: string;
  sourceHandleId: string | null;
};

function FocusCanvasNode({ nodeId }: { nodeId: string | null }) {
  const { fitView } = useReactFlow();
  useEffect(() => {
    if (!nodeId) return;
    const id = window.requestAnimationFrame(() => {
      fitView({ nodes: [{ id: nodeId }], padding: 0.45, duration: 280, maxZoom: 1.15 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [nodeId, fitView]);
  return null;
}

type Props = {
  t: TFn;
  initialDocument: WorkflowCanvasDocument;
  /** Increment when canvas is reloaded from disk so internal nodes reset. */
  reloadNonce: number;
  workflowScopeDoc: Record<string, unknown>;
  /** Patch workflow scope (e.g. source view filters, rule scope_filters) while editing the flow. */
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onChange: (doc: WorkflowCanvasDocument) => void;
  /** Optional: push canvas document into scope YAML (e.g. validation-rule order); may no-op. */
  onSyncScopeFromCanvas?: (canvas: WorkflowCanvasDocument) => void;
  /** From module default config — passed to source view CDF pickers in the node editor modal. */
  schemaSpace?: string;
  /** When true, the canvas is view-only (no drag/connect/drop, toolbars and palette disabled). */
  readOnly?: boolean;
  /** Optional: streamed local-run progress (node outline + edge animation), same as canvas preview. */
  runProgress?: WorkflowPreviewRunProgress;
  /** Optional one-line hint when cascade disable/enable affects other nodes (e.g. run log). */
  onActivityHint?: (message: string) => void;
};

function FlowCanvasBody({
  t,
  initialDocument,
  reloadNonce,
  workflowScopeDoc,
  onPatchWorkflowScope,
  onChange,
  onSyncScopeFromCanvas,
  schemaSpace,
  readOnly = false,
  runProgress,
  onActivityHint,
}: Props) {
  const { theme } = useAppSettings();
  const { screenToFlowPosition, getNode, getNodes, getEdges, fitView, getZoom } = useReactFlow();

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

  const flowCtxMenu = useTreeContextMenuState();

  const patchWorkflowScopeRef = useRef(onPatchWorkflowScope);
  patchWorkflowScopeRef.current = onPatchWorkflowScope;
  const [nodes, setNodes, onNodesChange] = useNodesState(canvasToFlowNodes(initialDocument.nodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(canvasToFlowEdges(initialDocument.edges));
  const [handleOrientation, setHandleOrientation] = useState<WorkflowCanvasHandleOrientation>(() =>
    normalizeWorkflowCanvasHandleOrientation(initialDocument.handle_orientation)
  );

  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;
  const onSyncScopeFromCanvasRef = useRef(onSyncScopeFromCanvas);
  onSyncScopeFromCanvasRef.current = onSyncScopeFromCanvas;

  const skipEmitRef = useRef(false);
  const lastEmittedNodesKeyRef = useRef("");
  const lastParentKeyRef = useRef("");
  const edgesRef = useRef(edges);
  edgesRef.current = edges;
  const handleOrientationRef = useRef(handleOrientation);
  handleOrientationRef.current = handleOrientation;
  const latestInitialRef = useRef(initialDocument);
  latestInitialRef.current = initialDocument;

  const historySuspendRef = useRef(false);
  const flowRootRef = useRef<HTMLDivElement>(null);
  const flowHistory = useFlowCanvasHistory({
    nodes,
    edges,
    setNodes,
    setEdges,
    handleOrientation,
    setHandleOrientation,
    suspendRef: historySuspendRef,
    flowRootRef,
  });

  const flowClipboard = useFlowClipboard({
    nodes,
    edges,
    setNodes,
    setEdges,
    rfSelectionRef,
    flowRootRef,
    readOnly,
  });

  useEffect(() => {
    const doc = latestInitialRef.current;
    const ho = normalizeWorkflowCanvasHandleOrientation(doc.handle_orientation);
    setHandleOrientation(ho);
    const snap: FlowCanvasSnapshot = {
      nodes: canvasToFlowNodes(doc.nodes),
      edges: canvasToFlowEdges(doc.edges),
      handleOrientation: ho,
    };
    flowHistory.reset(snap);
    setNodes(snap.nodes);
    setEdges(snap.edges);
    lastEmittedNodesKeyRef.current = JSON.stringify(doc.nodes);
    lastParentKeyRef.current = lastEmittedNodesKeyRef.current;
    skipEmitRef.current = true;
    setSearchQuery("");
    setFocusNodeId(null);
    const fitId = window.requestAnimationFrame(() => {
      fitView({ padding: 0.15, duration: 0 });
    });
    return () => window.cancelAnimationFrame(fitId);
  }, [reloadNonce, setNodes, setEdges, setHandleOrientation, flowHistory.reset, fitView]);

  /**
   * Pull canvas edits made outside React Flow (Configure tab, YAML reload).
   * Do not run when parent is merely stale after a local emit — that would revert in-flight edits.
   */
  useEffect(() => {
    const parentKey = JSON.stringify(initialDocument.nodes);
    if (parentKey === lastParentKeyRef.current) return;
    lastParentKeyRef.current = parentKey;
    if (parentKey === lastEmittedNodesKeyRef.current) return;

    const ho = normalizeWorkflowCanvasHandleOrientation(initialDocument.handle_orientation);
    skipEmitRef.current = true;
    setNodes(canvasToFlowNodes(initialDocument.nodes));
    setEdges(canvasToFlowEdges(initialDocument.edges));
    if (ho !== handleOrientation) setHandleOrientation(ho);
  }, [initialDocument, handleOrientation, setNodes, setEdges]);

  useEffect(() => {
    if (skipEmitRef.current) {
      skipEmitRef.current = false;
      return;
    }
    const doc = flowToCanvasDocument(nodes, edges, { handleOrientation });
    lastEmittedNodesKeyRef.current = JSON.stringify(doc.nodes);
    onChangeRef.current(doc);
    onSyncScopeFromCanvasRef.current?.(doc);
  }, [nodes, edges, handleOrientation]);

  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusNodeId, setFocusNodeId] = useState<string | null>(null);
  const [editorModalNode, setEditorModalNode] = useState<Node | null>(null);
  const [subgraphDrillNodeId, setSubgraphDrillNodeId] = useState<string | null>(null);
  /** Bumps when the subgraph drill opens so inner React Flow hydrates once; avoids resetting on `inner_canvas` ref churn. */
  const [subgraphDrillHydrateNonce, setSubgraphDrillHydrateNonce] = useState(0);

  const openSubgraphDrill = useCallback((id: string) => {
    setEditorModalNode(null);
    setSubgraphDrillNodeId(id);
    setSubgraphDrillHydrateNonce((n) => n + 1);
  }, []);
  const [connectEndMenu, setConnectEndMenu] = useState<ConnectEndMenuState | null>(null);
  const [connectEndMenuGroupId, setConnectEndMenuGroupId] = useState<string | null>(null);

  const searchActive = searchQuery.trim().length > 0;

  const workflowCanvas = useMemo(
    () => flowToCanvasDocument(nodes, edges, { handleOrientation }),
    [nodes, edges, handleOrientation]
  );

  const searchMatches = useMemo(
    () => filterCanvasNodesBySearch(workflowCanvas.nodes, searchQuery),
    [workflowCanvas.nodes, searchQuery]
  );

  const selectCanvasNodeFromSearch = useCallback(
    (nodeId: string) => {
      const rfNode = getNode(nodeId);
      if (rfNode) {
        setSelectedNode(rfNode);
        setSelectedEdge(null);
      }
      setFocusNodeId(nodeId);
    },
    [getNode]
  );

  type WorkflowCanvasPatch =
    | WorkflowCanvasDocument
    | ((prev: WorkflowCanvasDocument) => WorkflowCanvasDocument);

  const patchWorkflowCanvas = useCallback(
    (patch: WorkflowCanvasPatch) => {
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

  const compileDagMode = useMemo(() => readCompileWorkflowDagMode(workflowScopeDoc), [workflowScopeDoc]);

  useEffect(() => {
    if (!readOnly) return;
    setEditorModalNode(null);
    setSubgraphDrillNodeId(null);
    setConnectEndMenu(null);
    setConnectEndMenuGroupId(null);
  }, [readOnly]);

  const isValidConnection = useCallback(
    (c: Connection | Edge) => isValidDiscoveryFlowConnection(getNode, c, discoveryValidationRuleLayoutRfTypes, compileDagMode),
    [getNode, compileDagMode]
  );

  const onConnect = useCallback(
    (params: Connection) => {
      if (readOnly) return;
      setEdges((eds) => appendDiscoveryConnectionEdge(getNode, eds, params));
    },
    [setEdges, getNode, readOnly]
  );

  const onConnectEnd = useCallback(
    (event: MouseEvent | TouchEvent, connectionState: FinalConnectionState) => {
      if (readOnly) return;
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
    [getNode, screenToFlowPosition, readOnly, compileDagMode]
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

  const onDragOver = useCallback(
    (e: React.DragEvent) => {
      if (readOnly) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = "copy";
    },
    [readOnly]
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      if (readOnly) return;
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
      readOnly,
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

  const promoteInnerSubtreeToOwningGraph = useCallback(
    (subgraphNodeId: string, rootInnerNodeId: string) => {
      const nds = getNodes();
      const eds = getEdges();
      const res = promoteSubgraphInnerSubtreeToParentWorkflow(
        nds,
        eds,
        subgraphNodeId,
        rootInnerNodeId,
        handleOrientation
      );
      if (!res) return;
      setNodes(res.nodes);
      setEdges(res.edges);
      setSelectedNode(null);
      setSelectedEdge(null);
      setEditorModalNode(null);
    },
    [getNodes, getEdges, setNodes, setEdges, handleOrientation]
  );

  const patchSubgraphInnerCanvas = useCallback(
    (nodeId: string, inner: WorkflowCanvasDocument) => {
      setNodes((nds) => {
        const sg = nds.find((x) => x.id === nodeId && x.type === "discoverySubgraph");
        const data = (sg?.data ?? {}) as WorkflowCanvasNodeData;
        const frame: SubflowPortsConfig =
          data.subflow_ports?.inputs?.length || data.subflow_ports?.outputs?.length
            ? (data.subflow_ports as SubflowPortsConfig)
            : { inputs: [{ id: "in", label: "in" }], outputs: [{ id: "out", label: "out" }] };
        const hubInHint = String(data.subflow_hub_input_id ?? "").trim();
        const hubOutHint = String(data.subflow_hub_output_id ?? "").trim();
        const ensured = ensureSubgraphInnerBoundaryCanvasDocument(inner, frame, hubInHint, hubOutHint);
        const metaPatch: Partial<WorkflowCanvasNodeData> = ensured.mutatedBoundaryMeta
          ? { subflow_hub_input_id: ensured.hubInId, subflow_hub_output_id: ensured.hubOutId }
          : {};
        return nds.map((n) => {
          if (n.id !== nodeId || n.type !== "discoverySubgraph") return n;
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
    },
    [setNodes]
  );

  const patchSubgraphBoundaryHubIds = useCallback(
    (nodeId: string, hubInId: string, hubOutId: string) => {
      setNodes((nds) =>
        nds.map((n) => {
          if (n.id !== nodeId || n.type !== "discoverySubgraph") return n;
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
    },
    [setNodes]
  );

  const applySubflowPorts = useCallback(
    (subflowId: string, ports: SubflowPortsConfig) => {
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
    [getNodes, setNodes, setEdges]
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
    setFocusNodeId(null);
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

  const onNodeDoubleClick = useCallback((e: React.MouseEvent, node: Node) => {
    e.preventDefault();
    if (readOnly) return;
    if (node.type === "discoverySubgraph") {
      openSubgraphDrill(node.id);
      setSelectedNode(node);
      setSelectedEdge(null);
      return;
    }
    setEditorModalNode(node);
    setSelectedNode(node);
    setSelectedEdge(null);
  }, [openSubgraphDrill, readOnly]);

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
  const runProgressAnimatedIds = useMemo(
    () =>
      runProgress
        ? runProgressAnimatedEdgeIds(
            workflowCanvas.edges,
            runProgress.runActiveCanvasNodeIds,
            runProgress.runCompletedCanvasNodeIds
          )
        : new Set<string>(),
    [workflowCanvas.edges, runProgress]
  );
  const edgesForRf = useMemo(
    () =>
      edges.map((e) => ({
        ...e,
        animated: selectionAnimatedEdgeIds.has(e.id) || runProgressAnimatedIds.has(e.id),
      })),
    [edges, selectionAnimatedEdgeIds, runProgressAnimatedIds]
  );

  const runExecutingSet = useMemo(
    () => new Set(runProgress?.executingCanvasNodeIds ?? []),
    [runProgress?.executingCanvasNodeIds]
  );
  const runCompletedSet = useMemo(
    () => new Set(runProgress?.runCompletedCanvasNodeIds ?? []),
    [runProgress?.runCompletedCanvasNodeIds]
  );
  const runFailedSet = useMemo(
    () => new Set(runProgress?.failedCanvasNodeIds ?? []),
    [runProgress?.failedCanvasNodeIds]
  );
  const nodesForRf = useMemo(
    () =>
      nodes.map((n) => {
        const cn = workflowCanvas.nodes.find((node) => node.id === n.id);
        const matches = cn ? canvasNodeMatchesSearch(cn, searchQuery) : true;
        return applyDiscoveryFlowNodeDisplayClasses(n, {
          runFailed: runFailedSet.has(n.id),
          executing: runExecutingSet.has(n.id),
          completed: runCompletedSet.has(n.id),
          dimmed: searchActive && !matches,
        });
      }),
    [nodes, workflowCanvas.nodes, searchQuery, searchActive, runFailedSet, runExecutingSet, runCompletedSet]
  );

  const onNodesChangeWrapped = useCallback(
    (changes: NodeChange[]) => {
      const removals = changes.filter((c): c is Extract<NodeChange, { type: "remove" }> => c.type === "remove");
      const rest = changes.filter((c) => c.type !== "remove");
      if (rest.length) onNodesChange(rest);
      if (readOnly || removals.length === 0) return;
      const ids = [...new Set(removals.map((r) => r.id))];
      flushSync(() => {
        removeNodesByIds(ids);
      });
    },
    [onNodesChange, readOnly, removeNodesByIds]
  );

  const onPaneContextMenu = useCallback(
    (e: React.MouseEvent | MouseEvent) => {
      if (readOnly) return;
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
    [flowCtxMenu, t, fitView, handleAutoLayout, readOnly, flowClipboard]
  );

  const onFlowNodeContextMenu = useCallback(
    (e: React.MouseEvent, node: Node) => {
      if (readOnly) return;
      setSelectedNode(node);
      setSelectedEdge(null);
      if (node.type === "discoveryStart" || node.type === "discoveryEnd") return;
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
      readOnly,
      flowClipboard,
    ]
  );

  const onFlowEdgeContextMenu = useCallback(
    (e: React.MouseEvent, edge: Edge) => {
      if (readOnly) return;
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
    [flowCtxMenu, t, patchEdge, removeEdgeById, readOnly]
  );

  const panel = useFlowPanelLayout();

  return (
    <div className="discovery-flow-shell">
      <div
        className={`discovery-flow-shell__left${panel.leftCollapsed ? " discovery-flow-shell__left--collapsed" : ""}`}
        style={
          panel.leftCollapsed
            ? { flex: `0 0 ${panel.collapsedStripPx}px`, width: panel.collapsedStripPx }
            : { flex: `0 0 ${panel.leftWidth}px`, width: panel.leftWidth, minWidth: panel.leftMin, maxWidth: panel.leftMax }
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
            <div
              className="discovery-flow-shell__palette-body"
              style={
                readOnly
                  ? { pointerEvents: "none", opacity: 0.65 }
                  : undefined
              }
              aria-hidden={readOnly ? true : undefined}
            >
              <FlowPalette
                t={t}
                scopeDocument={workflowScopeDoc}
                schemaSpace={schemaSpace}
                readOnly={readOnly}
              />
            </div>
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
            disabled={readOnly || alignableSelectionCount < 2}
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
              disabled={readOnly}
            >
              <option value="lr">{t("flow.handleOrientationLr")}</option>
              <option value="tb">{t("flow.handleOrientationTb")}</option>
            </select>
          </label>
          {workflowCanvas.nodes.length > 0 && (
            <WorkflowCanvasSearchField
              t={t}
              searchQuery={searchQuery}
              onSearchQueryChange={setSearchQuery}
            />
          )}
          <span className="discovery-hint" style={{ marginLeft: "0.5rem", flex: "1 1 12rem" }}>
            {t("flow.canvasHint")}
          </span>
        </div>
        {workflowCanvas.nodes.length > 0 && (
          <WorkflowCanvasSearchResults
            t={t}
            searchQuery={searchQuery}
            searchMatches={searchMatches}
            selectedNodeId={selectedNode?.id ?? null}
            onSelectNode={selectCanvasNodeFromSearch}
          />
        )}
        <div
          className="discovery-flow-canvas-wrap"
          ref={flowRootRef}
          data-local-run-active={runProgress?.busy ? "true" : undefined}
        >
          <FlowHandleOrientationProvider value={handleOrientation}>
            <ReactFlow
              colorMode={theme}
              nodes={nodesForRf}
              edges={edgesForRf}
              onNodesChange={onNodesChangeWrapped}
              onEdgesChange={onEdgesChange}
              isValidConnection={isValidConnection}
              onConnect={readOnly ? undefined : onConnect}
              onConnectEnd={readOnly ? undefined : onConnectEnd}
              onDrop={readOnly ? undefined : onDrop}
              onDragOver={readOnly ? undefined : onDragOver}
              nodeTypes={DISCOVERY_FLOW_NODE_TYPES}
              defaultEdgeOptions={{ animated: false }}
              deleteKeyCode={readOnly ? null : ["Backspace", "Delete"]}
              onNodeClick={onNodeClick}
              onEdgeClick={onEdgeClick}
              onPaneClick={onPaneClick}
              onNodeDoubleClick={onNodeDoubleClick}
              onNodeDragStop={onNodeDragStop}
              onPaneContextMenu={onPaneContextMenu}
              onNodeContextMenu={onFlowNodeContextMenu}
              onEdgeContextMenu={onFlowEdgeContextMenu}
              nodesDraggable={!readOnly}
              nodesConnectable={!readOnly}
              edgesReconnectable={!readOnly}
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
              <FocusCanvasNode nodeId={focusNodeId} />
            </ReactFlow>
          </FlowHandleOrientationProvider>
          {connectEndMenu && (
            <div
              className="discovery-flow-connect-end-menu"
              style={{
                position: "fixed",
                left: Math.max(8, connectEndMenu.screen.x),
                top: Math.max(8, connectEndMenu.screen.y),
                zIndex: 40,
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
            <div style={{ position: "relative", flex: "1 1 auto", minHeight: 0 }}>
              <FlowNodeInspector
                t={t}
                selectedNode={selectedNode}
                selectedEdge={selectedEdge}
                workflowDoc={workflowScopeDoc}
                flowNodes={nodes}
                workflowCanvas={workflowCanvas}
                onPatchWorkflowCanvas={patchWorkflowCanvas}
                onPatchWorkflowScope={onPatchWorkflowScope}
                onPatchNode={patchNode}
                onPatchEdge={patchEdge}
                onApplySubflowPorts={applySubflowPorts}
                onOpenSubgraphDrill={(id) => openSubgraphDrill(id)}
                onActivityHint={onActivityHint}
              />
              {readOnly ? (
                <div
                  aria-hidden
                  title=""
                  style={{
                    position: "absolute",
                    inset: 0,
                    zIndex: 2,
                    cursor: "not-allowed",
                  }}
                />
              ) : null}
            </div>
          </>
        )}
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
      <SubgraphDrillModal
        t={t}
        open={Boolean(subgraphDrillNodeId)}
        node={subgraphDrillNodeId ? (nodes.find((n) => n.id === subgraphDrillNodeId) ?? null) : null}
        hydrateNonce={subgraphDrillHydrateNonce}
        handleOrientation={handleOrientation}
        workflowScopeDoc={workflowScopeDoc}
        onPatchWorkflowScope={onPatchWorkflowScope}
        onClose={() => setSubgraphDrillNodeId(null)}
        onSaveInnerCanvas={patchSubgraphInnerCanvas}
        onEnsureSubgraphBoundary={patchSubgraphBoundaryHubIds}
        onApplyPortsForOuterSubgraph={(ports) => {
          if (!subgraphDrillNodeId) return;
          applySubflowPorts(subgraphDrillNodeId, ports);
          setSubgraphDrillHydrateNonce((n) => n + 1);
        }}
        onPromoteInnerSubtreeToOwningGraph={promoteInnerSubtreeToOwningGraph}
        schemaSpace={schemaSpace}
        onActivityHint={onActivityHint}
      />
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

