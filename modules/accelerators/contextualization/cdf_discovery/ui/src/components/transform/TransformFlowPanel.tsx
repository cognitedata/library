import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent, type RefObject } from "react";
import { flushSync } from "react-dom";
import {
  applyNodeChanges,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  useOnSelectionChange,
  type Connection,
  type Edge,
  type FinalConnectionState,
  type Node,
  type NodeChange,
  type Viewport,
} from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import type { TransformCanvasDocument, TransformCanvasEdgeKind } from "../../types/transformCanvas";
import {
  normalizeTransformCanvasEdgePathStyle,
  normalizeTransformCanvasHandleOrientation,
  normalizeTransformCanvasLayoutMethod,
  type TransformCanvasEdgePathStyle,
  rfTypeToKind,
  type TransformCanvasHandleOrientation,
  type TransformCanvasLayoutMethod,
  type TransformCanvasNodeKind,
  type TransformPipelineParameters,
} from "../../types/transformCanvas";
import type { TreeNode } from "../../types/discoveryNodes";
import { entityDropMenuOptions, type EntityDropMenuOption } from "../../utils/dataTreeEntityDrop";
import {
  isOrchestrationNodeKind,
  shouldOpenNodeEditorOnDoubleClick,
  shouldOpenPreviewQueryOnActivate,
} from "../../utils/transformNodeEditorKinds";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  readCanvasDocumentViewport,
  applyTransformFlowRunDisplayClasses,
} from "./flowDocumentBridge";
import { canvasViewportToFlowViewport, viewportToCanvasViewport } from "./transformFlowHistory";
import { useTransformFlowHistory } from "./useTransformFlowHistory";
import { useTransformFlowKeyboard } from "./useTransformFlowKeyboard";
import { TransformFlowNodeToolbar } from "./TransformFlowNodeToolbar";
import { duplicateTransformFlowSelection } from "./duplicateTransformFlowSelection";
import { reconnectTransformFlowEdge } from "./reconnectTransformFlowEdge";
import type { TransformCanvasViewport } from "../../types/transformCanvasViewport";
import { TransformLocalRunDryRunField } from "./TransformLocalRunDryRunField";
import { FlowNodeInspector } from "./FlowNodeInspector";
import { FlowNodeEditorModal } from "./FlowNodeEditorModal";
import { FlowPalette } from "./FlowPalette";
import { getTransformFlowDropPayload } from "./transformFlowDrag";
import {
  TransformCanvasNodeList,
  TransformCanvasSearchField,
  TransformCanvasSearchResults,
} from "./TransformCanvasSearch";
import {
  filterTransformCanvasNodesBySearch,
  transformCanvasNodeMatchesSearch,
} from "../../utils/transformCanvasFlowSearch";
import {
  applyEntityCanvasDrop,
  applyEntityCanvasDropPair,
  applyTransformCanvasDrop,
  applyTransformCanvasDropAtPosition,
  materializeEtlStageAtPosition,
} from "./paletteDropOnEdge";
import { applyWiredCreationLabel } from "../../utils/etlNodeCreationLabel";
import { palettePayloadTooltip } from "../../utils/paletteStageTooltip";
import {
  handlerDropMenuGroupedOptionsForStage,
  handlerDropMenuOptionTooltip,
  palettePayloadNeedsHandlerPick,
  transformHandlerDropMenuGroups,
  type HandlerDropMenuOption,
} from "./handlerDropMenuOptions";
import { TransformHandlerCategoryPicker } from "./TransformHandlerCategoryPicker";
import { applyEtlNodeRemovals } from "./applyEtlNodeRemovals";
import {
  ETL_FLOW_NODE_TYPES,
} from "./flowNodeRegistry";
import {
  connectEndMenuGroupedOptionsForPane,
  connectEndMenuGroupedOptionsForSourceType,
  connectEndMenuOptionsForSourceType,
  type ConnectEndMenuGroup,
  type ConnectEndMenuOption,
} from "./connectEndMenuOptions";
import type { PaletteDragPayload } from "./transformFlowDrag";
import {
  appendEtlConnectionEdge,
  dedupeEdgesByHandles,
  persistenceOutboundEdgesToEnd,
} from "./transformFlowEdgeHelpers";
import { isValidEtlFlowConnection, wouldCreateCycle } from "./etlFlowConnections";
import { TreeContextMenuPortal, useTreeContextMenuState, type TreeCtxMenuItem } from "../governance/TreeContextMenu";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import {
  connectionLineTypeForEdgePathStyle,
  defaultTransformFlowEdgeOptions,
  FlowHandleOrientationEdgeSync,
} from "./FlowHandleOrientationEdgeSync";
import { TransformFlowLayoutControls } from "./TransformFlowLayoutControls";
import { highlightEdgesConnectedToNode } from "../flow/highlightEdgesForSelectedNode";
import { runProgressAnimatedEdgeIds } from "./flowRunProgressEdges";
import type { TransformFlowRunProgress } from "./transformPipelineRunStream";
import { applyFlowHandleOrientationToNode } from "./flowHandleOrientation";
import { layoutTransformFlowNodesByMethod } from "./transformAutoLayoutFlow";
import {
  buildTransformFlowPaneContextMenuItems,
  TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS,
} from "./transformFlowCanvasContextMenu";
import { alignSelectedTransformFlowNodes, type AlignFlowSelectionMode } from "./alignSelectedNodes";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { useTransformFlowClipboard } from "./useTransformFlowClipboard";
import {
  canMergeTransformSelection,
  isMergeableTransformFlowNode,
  mergeSelectedTransformFlowNodes,
} from "./mergeSelectedTransformNodes";
import {
  applyFlowOptimizeCandidates,
  discoverFlowOptimizeCandidates,
  type FlowOptimizeCandidate,
} from "./discoverFlowOptimizeCandidates";
import {
  canMergeScoreSelection,
  isMergeableScoreFlowNode,
  mergeSelectedScoreFlowNodes,
} from "./mergeSelectedScoreNodes";
import {
  explodeScoreFlowNode,
  isExplodableScoreFlowNode,
} from "./explodeScoreFlowNode";
import { OptimizeTransformsDialog } from "./OptimizeTransformsDialog";
import {
  explodeMultiStepTransformFlowNode,
  isExplodableMultiStepTransformFlowNode,
} from "./explodeMultiStepTransformNode";
import { autoResizeFlowNodesToContent } from "./autoResizeFlowNodesToContent";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function isBoundaryFlowNode(node: Node): boolean {
  const kind = rfTypeToKind(node.type);
  return kind === "start" || kind === "end";
}

function isFlowNodeEnabled(data: Record<string, unknown>): boolean {
  return data.canvas_node_enabled !== false;
}

type FlowStagePickerMenuProps = {
  screen: { x: number; y: number };
  groups: ConnectEndMenuGroup[];
  groupId: string | null;
  onGroupIdChange: (id: string | null) => void;
  onPick: (payload: PaletteDragPayload) => void;
  ariaLabel: string;
  t: TFn;
};

function FlowStagePickerMenu({
  screen,
  groups,
  groupId,
  onGroupIdChange,
  onPick,
  ariaLabel,
  t,
}: FlowStagePickerMenuProps) {
  const selectedGroup = groups.find((g) => g.id === groupId) ?? null;
  return (
    <div
      className="transform-flow-connect-end-menu"
      role="menu"
      style={{
        position: "fixed",
        left: Math.max(8, screen.x),
        top: Math.max(8, screen.y),
        zIndex: 1280,
      }}
      aria-label={ariaLabel}
    >
      {!selectedGroup ? (
        groups.map((g) => (
          <button
            key={g.id}
            type="button"
            className="disc-btn"
            role="menuitem"
            onClick={() => onGroupIdChange(g.id)}
          >
            {t(g.labelKey)}
          </button>
        ))
      ) : selectedGroup.id === "transform" ? (
        <>
          <button type="button" className="disc-btn" role="menuitem" onClick={() => onGroupIdChange(null)}>
            {t("transform.connectEnd.back")}
          </button>
          <TransformHandlerCategoryPicker
            variant="menu"
            groups={transformHandlerDropMenuGroups()}
            t={t}
            onPick={(opt) => onPick(opt.payload)}
          />
          {[...selectedGroup.options]
            .filter(
              (o) =>
                o.payload.stage !== "transform" ||
                !String(o.payload.handlerId ?? "").trim()
            )
            .sort((a, b) =>
              t(a.labelKey).localeCompare(t(b.labelKey), undefined, { sensitivity: "base" })
            )
            .map((opt: ConnectEndMenuOption) => {
              const tip = palettePayloadTooltip(opt.payload, t);
              return (
              <button
                key={opt.id}
                type="button"
                className="disc-btn"
                role="menuitem"
                onClick={() => onPick(opt.payload)}
                title={tip || undefined}
              >
                {t(opt.labelKey)}
              </button>
            );
            })}
        </>
      ) : (
        <>
          <button type="button" className="disc-btn" role="menuitem" onClick={() => onGroupIdChange(null)}>
            {t("transform.connectEnd.back")}
          </button>
          {[...selectedGroup.options]
            .sort((a, b) =>
              t(a.labelKey).localeCompare(t(b.labelKey), undefined, { sensitivity: "base" })
            )
            .map((opt: ConnectEndMenuOption) => {
              const tip = palettePayloadTooltip(opt.payload, t);
              return (
              <button
                key={opt.id}
                type="button"
                className="disc-btn"
                role="menuitem"
                onClick={() => onPick(opt.payload)}
                title={tip || undefined}
              >
                {t(opt.labelKey)}
              </button>
            );
            })}
        </>
      )}
    </div>
  );
}

type FlowEntityDropMenuProps = {
  screen: { x: number; y: number };
  entityLabel: string;
  options: EntityDropMenuOption[];
  onPick: (option: EntityDropMenuOption) => void;
  t: TFn;
};

function FlowEntityDropMenu({ screen, entityLabel, options, onPick, t }: FlowEntityDropMenuProps) {
  return (
    <div
      className="transform-flow-connect-end-menu"
      role="menu"
      style={{
        position: "fixed",
        left: Math.max(8, screen.x),
        top: Math.max(8, screen.y),
        zIndex: 1280,
      }}
      aria-label={t("transform.entityDrop.title", { label: entityLabel })}
    >
      {options.map((opt) => (
        <button
          key={opt.id}
          type="button"
          className="disc-btn"
          role="menuitem"
          onClick={() => onPick(opt)}
        >
          {t(opt.labelKey)}
        </button>
      ))}
    </div>
  );
}

type FlowHandlerDropMenuProps = {
  screen: { x: number; y: number };
  stage: TransformCanvasNodeKind;
  groupId: string | null;
  onGroupIdChange: (id: string | null) => void;
  onPick: (option: HandlerDropMenuOption) => void;
  t: TFn;
};

function FlowHandlerDropMenu({
  screen,
  stage,
  groupId,
  onGroupIdChange,
  onPick,
  t,
}: FlowHandlerDropMenuProps) {
  const groups = handlerDropMenuGroupedOptionsForStage(stage);
  if (!groups?.length) return null;
  const selectedGroup = groups.find((g) => g.id === groupId) ?? null;
  const showTransformCategories = stage === "transform";
  return (
    <div
      className="transform-flow-connect-end-menu"
      role="menu"
      style={{
        position: "fixed",
        left: Math.max(8, screen.x),
        top: Math.max(8, screen.y),
        zIndex: 1280,
      }}
      aria-label={t("transform.handlerDrop.title")}
    >
      {showTransformCategories ? (
        <TransformHandlerCategoryPicker
          variant="menu"
          groups={groups}
          t={t}
          onPick={onPick}
        />
      ) : !selectedGroup ? (
        groups.map((g) => (
          <button
            key={g.id}
            type="button"
            className="disc-btn"
            role="menuitem"
            onClick={() => onGroupIdChange(g.id)}
          >
            {t(g.labelKey)}
          </button>
        ))
      ) : (
        <>
          <button type="button" className="disc-btn" role="menuitem" onClick={() => onGroupIdChange(null)}>
            {t("transform.connectEnd.back")}
          </button>
          {selectedGroup.options.map((opt) => (
            <button
              key={opt.id}
              type="button"
              className="disc-btn"
              role="menuitem"
              onClick={() => onPick(opt)}
              title={
                palettePayloadTooltip(opt.payload, t) ||
                handlerDropMenuOptionTooltip(opt) ||
                undefined
              }
            >
              {t(opt.labelKey)}
            </button>
          ))}
        </>
      )}
    </div>
  );
}

type ConnectEndMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
  sourceNodeId: string;
  sourceHandleId: string | null;
};

type PaneAddMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
};

type EntityDropMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
  node: TreeNode;
};

type HandlerDropMenuState = {
  screen: { x: number; y: number };
  flow: { x: number; y: number };
  stage: TransformCanvasNodeKind;
  mode:
    | { type: "canvas" }
    | { type: "connect_end"; sourceNodeId: string; sourceHandleId: string | null }
    | { type: "pane_add" };
};

function FocusTransformFlowNode({ nodeId }: { nodeId: string | null }) {
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
  pipelineId?: string;
  initialDocument: TransformCanvasDocument;
  reloadNonce: number;
  readOnly?: boolean;
  onChange: (doc: TransformCanvasDocument) => void;
  onSave?: () => void;
  onSaveAs?: () => void;
  onReload?: () => void;
  onValidate?: () => void;
  onBuild?: () => void;
  onRun?: (options: { incrementalChangeProcessing: boolean; dryRun: boolean }) => void;
  onDelete?: () => void;
  onRename?: () => void;
  runScope?: "incremental" | "all";
  onRunScopeChange?: (scope: "incremental" | "all") => void;
  runScopeEnabled?: boolean;
  dryRun?: boolean;
  onDryRunChange?: (dryRun: boolean) => void;
  saving?: boolean;
  reloading?: boolean;
  runBusy?: boolean;
  onCancelLocalRun?: () => void;
  statusMessage?: string | null;
  runProgress?: TransformFlowRunProgress;
  validationFailedNodeIds?: readonly string[];
  pipelineParameters?: TransformPipelineParameters | null;
  lastRunId?: string | null;
  onOpenNodePreviewQuery?: (node: Node) => void;
};

function FlowCanvasBody({
  t,
  pipelineId,
  initialDocument,
  reloadNonce,
  readOnly = false,
  onChange,
  onSave,
  onSaveAs,
  onReload,
  onValidate,
  onBuild,
  onRun,
  onDelete,
  onRename,
  runScope = "all",
  onRunScopeChange,
  runScopeEnabled = false,
  dryRun = false,
  onDryRunChange,
  saving = false,
  reloading = false,
  runBusy = false,
  onCancelLocalRun,
  statusMessage,
  runProgress,
  validationFailedNodeIds,
  onOpenNodePreviewQuery,
}: Props) {
  const { theme } = useAppSettings();
  const { screenToFlowPosition, getNode, getEdges, getNodes, getZoom, fitView, setViewport } =
    useReactFlow();
  const flowRootRef = useRef<HTMLDivElement>(null);
  const viewportRef = useRef<TransformCanvasViewport | null>(
    readCanvasDocumentViewport(initialDocument) ?? null
  );
  const viewportPersistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const rfSelectionRef = useRef<Node[]>([]);
  const [alignableSelectionCount, setAlignableSelectionCount] = useState(0);
  useOnSelectionChange({
    onChange: useCallback(({ nodes: sel }) => {
      rfSelectionRef.current = sel;
      setAlignableSelectionCount(sel.filter((n) => n.type !== "etlStart" && n.type !== "etlEnd").length);
    }, []),
  });
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [handleOrientation, setHandleOrientation] = useState<TransformCanvasHandleOrientation>(() =>
    normalizeTransformCanvasHandleOrientation(initialDocument.handle_orientation)
  );
  const [edgePathStyle, setEdgePathStyle] = useState<TransformCanvasEdgePathStyle>(() =>
    normalizeTransformCanvasEdgePathStyle(initialDocument.edge_path_style)
  );
  const [layoutMethod, setLayoutMethod] = useState<TransformCanvasLayoutMethod>(() =>
    normalizeTransformCanvasLayoutMethod(initialDocument.layout_method)
  );
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusNodeId, setFocusNodeId] = useState<string | null>(null);
  const [editorModalNode, setEditorModalNode] = useState<Node | null>(null);
  const closeEditorModal = useCallback(() => setEditorModalNode(null), []);
  const [connectEndMenu, setConnectEndMenu] = useState<ConnectEndMenuState | null>(null);
  const [connectEndMenuGroupId, setConnectEndMenuGroupId] = useState<string | null>(null);
  const [paneAddMenu, setPaneAddMenu] = useState<PaneAddMenuState | null>(null);
  const [paneAddMenuGroupId, setPaneAddMenuGroupId] = useState<string | null>(null);
  const [entityDropMenu, setEntityDropMenu] = useState<EntityDropMenuState | null>(null);
  const [handlerDropMenu, setHandlerDropMenu] = useState<HandlerDropMenuState | null>(null);
  const [handlerDropMenuGroupId, setHandlerDropMenuGroupId] = useState<string | null>(null);
  const [optimizeOpen, setOptimizeOpen] = useState(false);
  const [optimizeCandidates, setOptimizeCandidates] = useState<FlowOptimizeCandidate[]>([]);
  const [optimizeNotice, setOptimizeNotice] = useState<string | null>(null);
  const flowCtxMenu = useTreeContextMenuState();
  const latestInitialRef = useRef(initialDocument);
  latestInitialRef.current = initialDocument;

  const initialDocumentRevision = useMemo(
    () =>
      `${initialDocument.nodes.length}:${initialDocument.edges.length}:${initialDocument.nodes[0]?.id ?? ""}`,
    [initialDocument]
  );

  const flowHistory = useTransformFlowHistory({
    nodes,
    edges,
    handleOrientation,
    layoutMethod,
    edgePathStyle,
    viewportRef,
  });

  useEffect(() => {
    const doc = latestInitialRef.current;
    const ho = normalizeTransformCanvasHandleOrientation(doc.handle_orientation);
    const lm = normalizeTransformCanvasLayoutMethod(doc.layout_method);
    const eps = normalizeTransformCanvasEdgePathStyle(doc.edge_path_style);
    const savedViewport = readCanvasDocumentViewport(doc);
    viewportRef.current = savedViewport ?? null;
    setHandleOrientation(ho);
    setLayoutMethod(lm);
    setEdgePathStyle(eps);
    let flowNodes = canvasToFlowNodes(doc.nodes);
    const flowEdges = canvasToFlowEdges(doc.edges, eps);
    if (
      flowNodes.length > 0 &&
      flowNodes.some(
        (n) => !Number.isFinite(n.position?.x) || !Number.isFinite(n.position?.y)
      )
    ) {
      flowNodes = layoutTransformFlowNodesByMethod(flowNodes, flowEdges, ho, lm);
    }
    setNodes(flowNodes);
    setEdges(flowEdges);
    setSelectedNode(null);
    setSelectedEdge(null);
    setEditorModalNode(null);
    setSearchQuery("");
    setFocusNodeId(null);
    flowHistory.reset();
    if (savedViewport) {
      window.requestAnimationFrame(() => {
        setViewport(canvasViewportToFlowViewport(savedViewport));
      });
    } else {
      window.setTimeout(() => fitView({ padding: 0.15, duration: 0 }), 0);
    }
  }, [reloadNonce, initialDocumentRevision, setNodes, setEdges, fitView, setViewport, flowHistory.reset]);

  useEffect(() => {
    return () => {
      if (viewportPersistTimerRef.current) {
        window.clearTimeout(viewportPersistTimerRef.current);
      }
    };
  }, []);

  const emitChange = useCallback(
    (
      nextNodes: Node[],
      nextEdges: Edge[],
      opts?: {
        orientation?: TransformCanvasHandleOrientation;
        layoutMethod?: TransformCanvasLayoutMethod;
        edgePathStyle?: TransformCanvasEdgePathStyle;
      },
      meta?: { viewport?: TransformCanvasViewport | null; recordHistory?: boolean }
    ) => {
      if (meta?.recordHistory !== false && !flowHistory.isApplying()) {
        flowHistory.recordBeforeChange();
      }
      if (meta?.viewport !== undefined) {
        viewportRef.current = meta.viewport;
      }
      const vp = viewportRef.current ?? undefined;
      onChange(
        flowToCanvasDocument(nextNodes, nextEdges, {
          handleOrientation: opts?.orientation ?? handleOrientation,
          layoutMethod: opts?.layoutMethod ?? layoutMethod,
          edgePathStyle: opts?.edgePathStyle ?? edgePathStyle,
          viewport: vp,
        })
      );
    },
    [onChange, handleOrientation, layoutMethod, edgePathStyle, flowHistory]
  );

  const applyHistorySnapshot = useCallback(
    (snap: ReturnType<typeof flowHistory.undo>) => {
      if (!snap) return;
      viewportRef.current = snap.viewport;
      setHandleOrientation(snap.handleOrientation);
      setLayoutMethod(snap.layoutMethod);
      setEdgePathStyle(snap.edgePathStyle);
      setNodes(snap.nodes);
      setEdges(snap.edges);
      if (snap.viewport) {
        setViewport(canvasViewportToFlowViewport(snap.viewport));
      }
      emitChange(snap.nodes, snap.edges, {
        orientation: snap.handleOrientation,
        layoutMethod: snap.layoutMethod,
        edgePathStyle: snap.edgePathStyle,
      }, { viewport: snap.viewport, recordHistory: false });
      setSelectedNode(null);
      setSelectedEdge(null);
      setEditorModalNode(null);
    },
    [setNodes, setEdges, setViewport, emitChange]
  );

  const handleUndo = useCallback(() => {
    applyHistorySnapshot(flowHistory.undo());
  }, [applyHistorySnapshot, flowHistory]);

  const handleRedo = useCallback(() => {
    applyHistorySnapshot(flowHistory.redo());
  }, [applyHistorySnapshot, flowHistory]);

  const scheduleViewportPersist = useCallback(
    (vp: Viewport) => {
      viewportRef.current = viewportToCanvasViewport(vp);
      if (viewportPersistTimerRef.current) {
        window.clearTimeout(viewportPersistTimerRef.current);
      }
      viewportPersistTimerRef.current = window.setTimeout(() => {
        emitChange(nodes, edges, undefined, {
          viewport: viewportRef.current,
          recordHistory: false,
        });
      }, 400);
    },
    [nodes, edges, emitChange]
  );

  const onMoveEnd = useCallback(
    (_event: MouseEvent | TouchEvent | null, vp: Viewport) => {
      if (readOnly) return;
      scheduleViewportPersist(vp);
    },
    [readOnly, scheduleViewportPersist]
  );

  const flowClipboard = useTransformFlowClipboard({
    nodes,
    edges,
    setNodes,
    setEdges,
    rfSelectionRef,
    readOnly,
    onPasted: (nextNodes, nextEdges) => emitChange(nextNodes, nextEdges),
  });

  const onSelectAllNodes = useCallback(() => {
    setNodes((nds) => nds.map((n) => ({ ...n, selected: true })));
  }, [setNodes]);

  const onDuplicateSelection = useCallback((): boolean => {
    if (readOnly) return false;
    const selected = rfSelectionRef.current.filter((n) => !isBoundaryFlowNode(n));
    if (selected.length === 0) return false;
    const result = duplicateTransformFlowSelection(nodes, edges, selected);
    if (!result) return false;
    setNodes(result.nodes);
    setEdges(result.edges);
    emitChange(result.nodes, result.edges);
    const first = result.nodes.find((n) => result.newNodeIds.includes(n.id)) ?? null;
    if (first) setSelectedNode(first);
    return true;
  }, [readOnly, nodes, edges, setNodes, setEdges, emitChange]);

  const transformCanvas = useMemo(
    () => flowToCanvasDocument(nodes, edges, { handleOrientation, layoutMethod, edgePathStyle }),
    [nodes, edges, handleOrientation, layoutMethod, edgePathStyle]
  );

  const searchActive = searchQuery.trim().length > 0;

  const searchMatches = useMemo(
    () => filterTransformCanvasNodesBySearch(transformCanvas.nodes, searchQuery, t),
    [transformCanvas.nodes, searchQuery, t]
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

  const applyAutoLayout = useCallback(
    (
      orientation: TransformCanvasHandleOrientation,
      method: TransformCanvasLayoutMethod = layoutMethod
    ) => {
      const autoResized = autoResizeFlowNodesToContent(getNodes());
      setNodes(autoResized);
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          const latestNodes = getNodes();
          const latestEdges = getEdges();
          const laidOut = layoutTransformFlowNodesByMethod(latestNodes, latestEdges, orientation, method);
          setNodes(laidOut);
          emitChange(laidOut, latestEdges, { orientation, layoutMethod: method });
          window.setTimeout(() => fitView({ padding: 0.15, duration: 200 }), 0);
        });
      });
    },
    [getNodes, getEdges, setNodes, emitChange, fitView, layoutMethod]
  );

  const handleAutoLayout = useCallback(() => {
    applyAutoLayout(handleOrientation, layoutMethod);
  }, [applyAutoLayout, handleOrientation, layoutMethod]);

  const onLayoutMethodChange = useCallback(
    (next: string) => {
      const method = normalizeTransformCanvasLayoutMethod(next);
      setLayoutMethod(method);
      applyAutoLayout(handleOrientation, method);
    },
    [applyAutoLayout, handleOrientation]
  );

  const applySelectionAlign = useCallback(
    (mode: AlignFlowSelectionMode) => {
      setNodes((nds) => {
        const next = alignSelectedTransformFlowNodes(nds, rfSelectionRef.current, mode);
        if (!next) return nds;
        emitChange(next, getEdges());
        return next;
      });
    },
    [setNodes, emitChange, getEdges]
  );

  const onHandleOrientationChange = useCallback(
    (next: TransformCanvasHandleOrientation) => {
      setHandleOrientation(next);
      applyAutoLayout(next);
    },
    [applyAutoLayout]
  );

  const onEdgePathStyleChange = useCallback(
    (next: TransformCanvasEdgePathStyle) => {
      setEdgePathStyle(next);
      setEdges((eds) => {
        const rfType = normalizeTransformCanvasEdgePathStyle(next);
        const nextEds = eds.map((e) => {
          if ((e.type ?? rfType) === rfType) return e;
          const { pathOptions: _pathOptions, ...rest } = e;
          return { ...rest, type: rfType };
        });
        emitChange(nodes, nextEds, { edgePathStyle: next });
        return nextEds;
      });
    },
    [nodes, setEdges, emitChange]
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      if (readOnly) return;
      if (!isValidEtlFlowConnection(connection, getNode)) return;
      if (wouldCreateCycle(getEdges(), connection.source, connection.target)) return;
      setEdges((eds) => {
        const merged = appendEtlConnectionEdge(getNode, eds, connection);
        emitChange(nodes, merged);
        return merged;
      });
    },
    [readOnly, setEdges, emitChange, nodes, getNode, getEdges]
  );

  const onReconnect = useCallback(
    (oldEdge: Edge, newConnection: Connection) => {
      if (readOnly) return;
      setEdges((eds) => {
        const next = reconnectTransformFlowEdge(oldEdge, newConnection, eds, getNode);
        if (next === eds) return eds;
        emitChange(nodes, next);
        return next;
      });
    },
    [readOnly, setEdges, emitChange, nodes, getNode]
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
      const st = srcNode?.type ?? cs.fromNode.type;
      const handleId = cs.fromHandle.id ?? null;
      const opts = connectEndMenuOptionsForSourceType(st);
      if (opts.length === 0) return;
      const flow = screenToFlowPosition(coords);
      setConnectEndMenu({
        screen: coords,
        flow,
        sourceNodeId: cs.fromNode.id,
        sourceHandleId: handleId,
      });
      setConnectEndMenuGroupId(null);
      setEntityDropMenu(null);
    },
    [getNode, screenToFlowPosition, readOnly]
  );

  const commitConnectEndMenu = useCallback(
    (payload: PaletteDragPayload) => {
      if (!connectEndMenu) return;
      if (palettePayloadNeedsHandlerPick(payload)) {
        setHandlerDropMenu({
          screen: connectEndMenu.screen,
          flow: connectEndMenu.flow,
          stage: payload.stage,
          mode: {
            type: "connect_end",
            sourceNodeId: connectEndMenu.sourceNodeId,
            sourceHandleId: connectEndMenu.sourceHandleId,
          },
        });
        setHandlerDropMenuGroupId(null);
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        return;
      }
      const nds = getNodes();
      const eds = getEdges();
      const existingIds = new Set(nds.map((n) => n.id));
      const materialized = materializeEtlStageAtPosition(payload, connectEndMenu.flow, existingIds);
      if (!materialized) return;
      const predecessor = getNode(connectEndMenu.sourceNodeId);
      const node = applyWiredCreationLabel(materialized.node, predecessor, t);
      const conn: Connection = {
        source: connectEndMenu.sourceNodeId,
        sourceHandle: connectEndMenu.sourceHandleId ?? "out",
        target: node.id,
        targetHandle: "in",
      };
      const resolveNode = (id: string) => (id === node.id ? node : getNode(id));
      if (!isValidEtlFlowConnection(conn, resolveNode)) return;
      if (wouldCreateCycle(eds, conn.source, conn.target)) return;
      const nextNodes = [...nds, node];
      const toEnd = persistenceOutboundEdgesToEnd(materialized.rfType, node.id, nextNodes);
      const nextEdges = dedupeEdgesByHandles([
        ...appendEtlConnectionEdge(getNode, eds, conn),
        ...toEnd,
      ]);
      flushSync(() => {
        setNodes(nextNodes);
        setEdges(nextEdges);
      });
      emitChange(nextNodes, nextEdges);
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setSelectedNode(node);
      setSelectedEdge(null);
    },
    [connectEndMenu, getNodes, getEdges, getNode, setNodes, setEdges, emitChange, t]
  );

  const commitPaneAddNode = useCallback(
    (payload: PaletteDragPayload) => {
      if (!paneAddMenu) return;
      if (palettePayloadNeedsHandlerPick(payload)) {
        setHandlerDropMenu({
          screen: paneAddMenu.screen,
          flow: paneAddMenu.flow,
          stage: payload.stage,
          mode: { type: "pane_add" },
        });
        setHandlerDropMenuGroupId(null);
        setPaneAddMenu(null);
        setPaneAddMenuGroupId(null);
        return;
      }
      const nds = getNodes();
      const existingIds = new Set(nds.map((n) => n.id));
      const materialized = materializeEtlStageAtPosition(payload, paneAddMenu.flow, existingIds);
      if (!materialized) return;
      const { node } = materialized;
      const nextNodes = [...nds, node];
      const toEnd = persistenceOutboundEdgesToEnd(materialized.rfType, node.id, nextNodes);
      const nextEdges = dedupeEdgesByHandles([...getEdges(), ...toEnd]);
      setNodes(nextNodes);
      setEdges(nextEdges);
      emitChange(nextNodes, nextEdges);
      setSelectedNode(node);
      setSelectedEdge(null);
      setPaneAddMenu(null);
      setPaneAddMenuGroupId(null);
    },
    [paneAddMenu, getNodes, getEdges, setNodes, emitChange]
  );

  const commitHandlerDropMenu = useCallback(
    (option: HandlerDropMenuOption) => {
      if (!handlerDropMenu) return;
      const payload = option.payload;
      const { flow, mode } = handlerDropMenu;

      if (mode.type === "canvas") {
        const result = applyTransformCanvasDropAtPosition(flow, payload, {
          getNode,
          getEdges,
          zoom: getZoom(),
          nodes,
          localize: t,
        });
        if (!result) return;
        setNodes(result.nodes);
        setEdges(result.edges);
        emitChange(result.nodes, result.edges);
        const selected = result.nodes.find((n) => n.id === result.selectNodeId) ?? null;
        setSelectedNode(selected);
        setSelectedEdge(null);
      } else if (mode.type === "connect_end") {
        const nds = getNodes();
        const eds = getEdges();
        const existingIds = new Set(nds.map((n) => n.id));
        const materialized = materializeEtlStageAtPosition(payload, flow, existingIds);
        if (!materialized) return;
        const predecessor = getNode(mode.sourceNodeId);
        const node = applyWiredCreationLabel(materialized.node, predecessor, t);
        const conn: Connection = {
          source: mode.sourceNodeId,
          sourceHandle: mode.sourceHandleId ?? "out",
          target: node.id,
          targetHandle: "in",
        };
        const resolveNode = (id: string) => (id === node.id ? node : getNode(id));
        if (!isValidEtlFlowConnection(conn, resolveNode)) return;
        if (wouldCreateCycle(eds, conn.source, conn.target)) return;
        const nextNodes = [...nds, node];
        const toEnd = persistenceOutboundEdgesToEnd(materialized.rfType, node.id, nextNodes);
        const nextEdges = dedupeEdgesByHandles([
          ...appendEtlConnectionEdge(getNode, eds, conn),
          ...toEnd,
        ]);
        flushSync(() => {
          setNodes(nextNodes);
          setEdges(nextEdges);
        });
        emitChange(nextNodes, nextEdges);
        setSelectedNode(node);
        setSelectedEdge(null);
      } else if (mode.type === "pane_add") {
        const nds = getNodes();
        const existingIds = new Set(nds.map((n) => n.id));
        const materialized = materializeEtlStageAtPosition(payload, flow, existingIds);
        if (!materialized) return;
        const { node } = materialized;
        const nextNodes = [...nds, node];
        const toEnd = persistenceOutboundEdgesToEnd(materialized.rfType, node.id, nextNodes);
        const nextEdges = dedupeEdgesByHandles([...getEdges(), ...toEnd]);
        setNodes(nextNodes);
        setEdges(nextEdges);
        emitChange(nextNodes, nextEdges);
        setSelectedNode(node);
        setSelectedEdge(null);
      }

      setHandlerDropMenu(null);
      setHandlerDropMenuGroupId(null);
    },
    [handlerDropMenu, nodes, getNode, getEdges, getZoom, getNodes, setNodes, setEdges, emitChange, t]
  );

  useEffect(() => {
    if (!connectEndMenu && !paneAddMenu && !entityDropMenu && !handlerDropMenu) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        setPaneAddMenu(null);
        setPaneAddMenuGroupId(null);
        setEntityDropMenu(null);
        setHandlerDropMenu(null);
        setHandlerDropMenuGroupId(null);
      }
    };
    const onDocPointerDown = (e: PointerEvent) => {
      const tgt = e.target;
      if (tgt instanceof Element && tgt.closest(".transform-flow-connect-end-menu")) return;
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setPaneAddMenu(null);
      setPaneAddMenuGroupId(null);
      setEntityDropMenu(null);
      setHandlerDropMenu(null);
      setHandlerDropMenuGroupId(null);
    };
    document.addEventListener("keydown", onKey);
    /** Defer so the pointerup that opened the menu does not immediately dismiss it. */
    const timer = window.setTimeout(() => {
      document.addEventListener("pointerdown", onDocPointerDown, true);
    }, 0);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener("keydown", onKey);
      document.removeEventListener("pointerdown", onDocPointerDown, true);
    };
  }, [connectEndMenu, paneAddMenu, entityDropMenu, handlerDropMenu]);

  const removeNodesByIds = useCallback(
    (nodeIds: string[]) => {
      const result = applyEtlNodeRemovals(getNodes(), getEdges(), nodeIds);
      if (!result) return;
      setNodes(result.nodes);
      setEdges(result.edges);
      emitChange(result.nodes, result.edges);
      setSelectedNode((prev) => (prev && result.clearedSelectionNodeIds.has(prev.id) ? null : prev));
      setSelectedEdge(null);
      setEditorModalNode((prev) => (prev && result.clearedSelectionNodeIds.has(prev.id) ? null : prev));
    },
    [getNodes, getEdges, setNodes, setEdges, emitChange]
  );

  const onCutSelection = useCallback((): boolean => {
    if (readOnly) return false;
    if (!flowClipboard.copySelection()) return false;
    const ids = rfSelectionRef.current.filter((n) => !isBoundaryFlowNode(n)).map((n) => n.id);
    if (ids.length === 0) return false;
    removeNodesByIds(ids);
    return true;
  }, [readOnly, flowClipboard, removeNodesByIds]);

  useTransformFlowKeyboard({
    flowRootRef: flowRootRef as RefObject<HTMLElement | null>,
    readOnly,
    rfSelectionRef,
    onCopy: flowClipboard.copySelection,
    onPaste: flowClipboard.pasteClipboard,
    onCut: onCutSelection,
    onDuplicate: onDuplicateSelection,
    onSelectAll: onSelectAllNodes,
    onUndo: handleUndo,
    onRedo: handleRedo,
    canUndo: flowHistory.canUndo,
    canRedo: flowHistory.canRedo,
  });

  const onNodesChangeWrapped = useCallback(
    (changes: NodeChange[]) => {
      const removals = changes.filter((c): c is Extract<NodeChange, { type: "remove" }> => c.type === "remove");
      const rest = changes.filter((c) => c.type !== "remove");
      const resizeEnded =
        !readOnly &&
        rest.some(
          (c) =>
            c.type === "dimensions" &&
            "resizing" in c &&
            (c as { resizing?: boolean }).resizing === false
        );
      if (rest.length) {
        setNodes((nds) => {
          const nextNodes = applyNodeChanges(rest, nds);
          if (resizeEnded) {
            setEdges((eds) => {
              emitChange(nextNodes, eds);
              return eds;
            });
          }
          return nextNodes;
        });
      }
      if (readOnly || removals.length === 0) return;
      const ids = [...new Set(removals.map((r) => r.id))];
      flushSync(() => {
        removeNodesByIds(ids);
      });
    },
    [readOnly, removeNodesByIds, setNodes, setEdges, emitChange]
  );

  const onDeleteNode = useCallback(
    (nodeId: string) => {
      if (readOnly) return;
      removeNodesByIds([nodeId]);
    },
    [readOnly, removeNodesByIds]
  );

  const onMergeSelectedPipelineNodes = useCallback(
    (anchorNodeId: string) => {
      if (readOnly) return;
      const selected = rfSelectionRef.current;
      const transformIds = selected.filter(isMergeableTransformFlowNode).map((n) => n.id);
      if (transformIds.length >= 2 && transformIds.includes(anchorNodeId)) {
        const result = mergeSelectedTransformFlowNodes(getNodes(), getEdges(), transformIds, anchorNodeId);
        if (result) {
          setNodes(result.nodes);
          setEdges(result.edges);
          emitChange(result.nodes, result.edges);
          const anchor = result.nodes.find((n) => n.id === result.anchorNodeId) ?? null;
          setSelectedNode(anchor);
          setSelectedEdge(null);
          setEditorModalNode(anchor);
          return;
        }
      }
      const scoreIds = selected.filter(isMergeableScoreFlowNode).map((n) => n.id);
      if (scoreIds.length >= 2 && scoreIds.includes(anchorNodeId)) {
        const result = mergeSelectedScoreFlowNodes(getNodes(), getEdges(), scoreIds, anchorNodeId);
        if (!result) return;
        setNodes(result.nodes);
        setEdges(result.edges);
        emitChange(result.nodes, result.edges);
        const anchor = result.nodes.find((n) => n.id === result.anchorNodeId) ?? null;
        setSelectedNode(anchor);
        setSelectedEdge(null);
        setEditorModalNode(anchor);
      }
    },
    [readOnly, getNodes, getEdges, setNodes, setEdges, emitChange]
  );

  const onOpenOptimizeTransforms = useCallback(() => {
    if (readOnly) return;
    setOptimizeNotice(null);
    setOptimizeCandidates(discoverFlowOptimizeCandidates(getNodes(), getEdges()));
    setOptimizeOpen(true);
  }, [readOnly, getNodes, getEdges]);

  const onApplyOptimizeTransforms = useCallback(
    (approved: FlowOptimizeCandidate[]) => {
      if (readOnly) return;
      const result = applyFlowOptimizeCandidates(getNodes(), getEdges(), approved);
      if (!result) return;
      setNodes(result.nodes);
      setEdges(result.edges);
      emitChange(result.nodes, result.edges);
      setSelectedNode(null);
      setSelectedEdge(null);
      setEditorModalNode(null);
      setOptimizeNotice(t("transform.optimize.applied", { count: result.appliedCount }));
    },
    [readOnly, getNodes, getEdges, setNodes, setEdges, emitChange, t]
  );

  const onExplodePipelineNode = useCallback(
    (sourceNodeId: string) => {
      if (readOnly) return;
      const nds = getNodes();
      const eds = getEdges();
      const source = nds.find((n) => n.id === sourceNodeId);
      if (!source) return;
      const result = isExplodableScoreFlowNode(source)
        ? explodeScoreFlowNode(nds, eds, sourceNodeId, { handleOrientation })
        : explodeMultiStepTransformFlowNode(nds, eds, sourceNodeId, { handleOrientation });
      if (!result) return;
      setNodes(result.nodes);
      setEdges(result.edges);
      emitChange(result.nodes, result.edges);
      const first = result.nodes.find((n) => n.id === result.newNodeIds[0]) ?? null;
      setSelectedNode(first);
      setSelectedEdge(null);
      setEditorModalNode(null);
    },
    [readOnly, getNodes, getEdges, setNodes, setEdges, emitChange, handleOrientation]
  );

  const onNodeDragStop = useCallback(() => {
    if (readOnly) return;
    setNodes((nds) => {
      setEdges((eds) => {
        emitChange(nds, eds);
        return eds;
      });
      return nds;
    });
  }, [readOnly, setNodes, setEdges, emitChange]);

  const onEdgesChangeWrapped = useCallback(
    (changes: Parameters<typeof onEdgesChange>[0]) => {
      const removals = changes.filter((c): c is Extract<(typeof changes)[number], { type: "remove" }> => c.type === "remove");
      const shouldPersist = changes.some((c) => c.type !== "select");
      onEdgesChange(changes);
      if (removals.some((r) => selectedEdge?.id === r.id)) {
        setSelectedEdge(null);
      }
      if (readOnly || !shouldPersist) return;
      setEdges((eds) => {
        emitChange(nodes, eds);
        return eds;
      });
    },
    [onEdgesChange, readOnly, setEdges, emitChange, nodes, selectedEdge]
  );

  const onPatchEdge = useCallback(
    (edgeId: string, kind: TransformCanvasEdgeKind) => {
      setEdges((eds) => {
        const next = eds.map((edge) =>
          edge.id === edgeId
            ? { ...edge, data: { ...((edge.data ?? {}) as FlowEdgeData), kind } }
            : edge
        );
        emitChange(nodes, next);
        return next;
      });
      setSelectedEdge((prev) =>
        prev?.id === edgeId ? { ...prev, data: { ...((prev.data ?? {}) as FlowEdgeData), kind } } : prev
      );
    },
    [setEdges, emitChange, nodes]
  );

  const removeEdgeById = useCallback(
    (edgeId: string) => {
      if (readOnly) return;
      setEdges((eds) => {
        const next = eds.filter((e) => e.id !== edgeId);
        emitChange(nodes, next);
        return next;
      });
      setSelectedEdge((se) => (se?.id === edgeId ? null : se));
    },
    [readOnly, setEdges, emitChange, nodes]
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

  const onFlowEdgeContextMenu = useCallback(
    (e: React.MouseEvent, edge: Edge) => {
      if (readOnly) return;
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setPaneAddMenu(null);
      setPaneAddMenuGroupId(null);
      setEntityDropMenu(null);
      setSelectedEdge(edge);
      setSelectedNode(null);
      const fd = (edge.data ?? {}) as FlowEdgeData;
      const cur: TransformCanvasEdgeKind =
        fd.kind === "sequence" || fd.kind === "parallel_group" ? fd.kind : "data";
      const kinds = ["data", "sequence", "parallel_group"] as const;
      const labelFor = (k: (typeof kinds)[number]) =>
        k === "data"
          ? t("transform.inspector.edgeKindData")
          : k === "sequence"
            ? t("transform.inspector.edgeKindSequence")
            : t("transform.inspector.edgeKindParallel");
      const items: TreeCtxMenuItem[] = [
        ...kinds.map((k) => ({
          id: `kind-${k}`,
          label: labelFor(k),
          disabled: cur === k,
          onSelect: () => onPatchEdge(edge.id, k),
        })),
        {
          id: "remove-edge",
          label: t("transform.inspector.deleteEdge"),
          danger: true,
          onSelect: () => removeEdgeById(edge.id),
        },
      ];
      flowCtxMenu.open(e, items);
    },
    [flowCtxMenu, t, onPatchEdge, removeEdgeById, readOnly]
  );

  const onPatchNode = useCallback(
    (nodeId: string, data: Record<string, unknown>) => {
      setNodes((nds) => {
        const next = nds.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n));
        emitChange(next, edges);
        setEditorModalNode((prev) => {
          if (!prev || prev.id !== nodeId) return prev;
          const updated = next.find((n) => n.id === nodeId);
          return updated ?? prev;
        });
        setSelectedNode((prev) => {
          if (!prev || prev.id !== nodeId) return prev;
          const updated = next.find((n) => n.id === nodeId);
          return updated ?? prev;
        });
        return next;
      });
    },
    [setNodes, emitChange, edges]
  );

  const onToggleNodeEnabled = useCallback(
    (node: Node) => {
      if (readOnly) return;
      const data = (node.data ?? {}) as Record<string, unknown>;
      onPatchNode(node.id, { ...data, canvas_node_enabled: !isFlowNodeEnabled(data) });
    },
    [readOnly, onPatchNode]
  );

  const toolbarNodes = useMemo(
    () => nodes.filter((n) => n.selected && !isBoundaryFlowNode(n)),
    [nodes]
  );

  const flowDefaultViewport = useMemo(
    () =>
      viewportRef.current ? canvasViewportToFlowViewport(viewportRef.current) : undefined,
    [reloadNonce, initialDocumentRevision]
  );

  const onFlowNodeContextMenu = useCallback(
    (e: ReactMouseEvent | globalThis.MouseEvent, node: Node) => {
      if (readOnly) return;
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setPaneAddMenu(null);
      setPaneAddMenuGroupId(null);
      setSelectedNode(node);
      setSelectedEdge(null);
      if (isBoundaryFlowNode(node)) return;
      const data = (node.data ?? {}) as Record<string, unknown>;
      const mergeableTransforms = rfSelectionRef.current.filter(isMergeableTransformFlowNode);
      const mergeableScores = rfSelectionRef.current.filter(isMergeableScoreFlowNode);
      const canMergeTransforms =
        isMergeableTransformFlowNode(node) &&
        mergeableTransforms.length >= 2 &&
        mergeableTransforms.some((n) => n.id === node.id) &&
        canMergeTransformSelection(mergeableTransforms, getEdges());
      const canMergeScores =
        isMergeableScoreFlowNode(node) &&
        mergeableScores.length >= 2 &&
        mergeableScores.some((n) => n.id === node.id) &&
        canMergeScoreSelection(mergeableScores, getEdges());
      const canExplodeNode =
        isExplodableMultiStepTransformFlowNode(node) || isExplodableScoreFlowNode(node);
      const items: TreeCtxMenuItem[] = [
        {
          id: "copy",
          label: t("transform.flow.ctxMenuCopy"),
          onSelect: () => flowClipboard.copySelection(),
        },
        {
          id: "paste",
          label: t("transform.flow.ctxMenuPaste"),
          onSelect: () => void flowClipboard.pasteClipboard(),
        },
        ...(canMergeTransforms
          ? [
              {
                id: "merge-transforms",
                label: t("transform.flow.ctxMenuMergeTransforms"),
                onSelect: () => onMergeSelectedPipelineNodes(node.id),
              },
            ]
          : []),
        ...(canMergeScores
          ? [
              {
                id: "merge-scores",
                label: t("transform.flow.ctxMenuMergeScores"),
                onSelect: () => onMergeSelectedPipelineNodes(node.id),
              },
            ]
          : []),
        ...(canExplodeNode
          ? [
              {
                id: "explode-node",
                label: isExplodableScoreFlowNode(node)
                  ? t("transform.flow.ctxMenuExplodeScore")
                  : t("transform.flow.ctxMenuExplodeTransforms"),
                onSelect: () => onExplodePipelineNode(node.id),
              },
            ]
          : []),
        {
          id: "open-editor",
          label: t("transform.inspector.openEditor"),
          onSelect: () => setEditorModalNode(node),
        },
        {
          id: "toggle-enabled",
          label: isFlowNodeEnabled(data)
            ? t("transform.contextMenu.disableNode")
            : t("transform.contextMenu.enableNode"),
          onSelect: () =>
            onPatchNode(node.id, { ...data, canvas_node_enabled: !isFlowNodeEnabled(data) }),
        },
        {
          id: "delete-node",
          label: t("transform.inspector.deleteNode"),
          danger: true,
          onSelect: () => onDeleteNode(node.id),
        },
      ];
      flowCtxMenu.open(e, items);
    },
    [
      readOnly,
      flowCtxMenu,
      t,
      onPatchNode,
      onDeleteNode,
      onMergeSelectedPipelineNodes,
      onExplodePipelineNode,
      flowClipboard,
      getEdges,
    ]
  );

  const onFlowPaneContextMenu = useCallback(
    (e: ReactMouseEvent | globalThis.MouseEvent) => {
      if (readOnly) return;
      e.preventDefault();
      flowCtxMenu.close();
      setConnectEndMenu(null);
      setConnectEndMenuGroupId(null);
      setEntityDropMenu(null);
      const coords = { x: e.clientX, y: e.clientY };
      const flowPos = screenToFlowPosition(coords);
      flowCtxMenu.open(
        e,
        buildTransformFlowPaneContextMenuItems({
          t,
          onCopy: () => flowClipboard.copySelection(),
          onPaste: () => void flowClipboard.pasteClipboard(),
          onAddNode: () => {
            setPaneAddMenu({ screen: coords, flow: flowPos });
            setPaneAddMenuGroupId(null);
          },
          showAlign: !readOnly,
          alignDisabled: alignableSelectionCount < 2,
          onAlignSelection: applySelectionAlign,
          handleOrientation,
          onHandleOrientationChange,
          layoutMethod,
          layoutMethodOptions: TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS,
          onLayoutMethodChange,
          edgePathStyle,
          onEdgePathStyleChange,
          onFitView: () => fitView({ padding: 0.15, duration: 200 }),
          onAutoLayout: handleAutoLayout,
        })
      );
      setSelectedNode(null);
      setSelectedEdge(null);
    },
    [
      readOnly,
      flowCtxMenu,
      screenToFlowPosition,
      t,
      flowClipboard,
      fitView,
      handleAutoLayout,
      layoutMethod,
      onLayoutMethodChange,
      handleOrientation,
      onHandleOrientationChange,
      edgePathStyle,
      onEdgePathStyleChange,
      alignableSelectionCount,
      applySelectionAlign,
    ]
  );

  const onNodeDoubleClick = useCallback(
    (e: React.MouseEvent, node: Node) => {
      e.preventDefault();
      const kind = rfTypeToKind(node.type);
      if (shouldOpenPreviewQueryOnActivate(kind)) {
        onOpenNodePreviewQuery?.(node);
        setSelectedNode(node);
        setSelectedEdge(null);
        return;
      }
      if (!shouldOpenNodeEditorOnDoubleClick(kind, readOnly)) return;
      setEditorModalNode(node);
      setSelectedNode(node);
      setSelectedEdge(null);
    },
    [readOnly, onOpenNodePreviewQuery]
  );

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const commitEntityDropMenu = useCallback(
    (option: EntityDropMenuOption) => {
      if (!entityDropMenu) return;
      const dropInput = {
        node: entityDropMenu.node,
        flowPosition: entityDropMenu.flow,
        getNode,
        getEdges,
        zoom: getZoom(),
        nodes,
      };
      const result =
        option.kind === "query_save_pair"
          ? applyEntityCanvasDropPair(dropInput)
          : applyEntityCanvasDrop({ ...dropInput, stage: option.stage });
      if (!result) return;

      setNodes(result.nodes);
      setEdges(result.edges);
      emitChange(result.nodes, result.edges);
      const selected = result.nodes.find((n) => n.id === result.selectNodeId) ?? null;
      setSelectedNode(selected);
      setSelectedEdge(null);
      setEntityDropMenu(null);
    },
    [entityDropMenu, nodes, getNode, getEdges, getZoom, setNodes, setEdges, emitChange]
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      if (readOnly) return;
      e.preventDefault();
      const payload = getTransformFlowDropPayload(e);
      if (!payload) return;

      if (payload.kind === "data_tree_entity") {
        const options = entityDropMenuOptions(payload.node);
        if (!options?.length) return;
        const coords = { x: e.clientX, y: e.clientY };
        setEntityDropMenu({
          screen: coords,
          flow: screenToFlowPosition(coords),
          node: payload.node,
        });
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        setPaneAddMenu(null);
        setPaneAddMenuGroupId(null);
        setHandlerDropMenu(null);
        setHandlerDropMenuGroupId(null);
        return;
      }

      if (payload.kind === "etl_stage" && palettePayloadNeedsHandlerPick(payload)) {
        const coords = { x: e.clientX, y: e.clientY };
        setHandlerDropMenu({
          screen: coords,
          flow: screenToFlowPosition(coords),
          stage: payload.stage,
          mode: { type: "canvas" },
        });
        setHandlerDropMenuGroupId(null);
        setConnectEndMenu(null);
        setConnectEndMenuGroupId(null);
        setPaneAddMenu(null);
        setPaneAddMenuGroupId(null);
        setEntityDropMenu(null);
        return;
      }

      const result = applyTransformCanvasDrop({
        event: e,
        screenToFlowPosition,
        getNode,
        getEdges,
        zoom: getZoom(),
        nodes,
        localize: t,
      });
      if (!result) return;

      setNodes(result.nodes);
      setEdges(result.edges);
      emitChange(result.nodes, result.edges);
      const selected = result.nodes.find((n) => n.id === result.selectNodeId) ?? null;
      setSelectedNode(selected);
      setSelectedEdge(null);
    },
    [readOnly, nodes, screenToFlowPosition, getNode, getEdges, getZoom, setNodes, setEdges, emitChange, t]
  );

  const nodeTypes = useMemo(() => ETL_FLOW_NODE_TYPES, []);

  const defaultEdgeOptions = useMemo(
    () => defaultTransformFlowEdgeOptions(edgePathStyle),
    [edgePathStyle]
  );

  const connectionLineType = useMemo(
    () => connectionLineTypeForEdgePathStyle(edgePathStyle),
    [edgePathStyle]
  );

  const runProgressAnimatedIds = useMemo(
    () =>
      runProgress
        ? runProgressAnimatedEdgeIds(
            initialDocument.edges,
            runProgress.runActiveCanvasNodeIds,
            runProgress.runCompletedCanvasNodeIds
          )
        : new Set<string>(),
    [initialDocument.edges, runProgress]
  );
  const edgesForRf = useMemo(() => {
    const withRun = edges.map((e) => ({
      ...e,
      animated: e.animated || runProgressAnimatedIds.has(e.id),
    }));
    return highlightEdgesConnectedToNode(withRun, selectedNode?.id ?? null);
  }, [edges, runProgressAnimatedIds, selectedNode?.id]);
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
  const runWarningSet = useMemo(
    () => new Set(runProgress?.warningCanvasNodeIds ?? []),
    [runProgress?.warningCanvasNodeIds]
  );
  const validationFailedSet = useMemo(
    () => new Set(validationFailedNodeIds ?? []),
    [validationFailedNodeIds]
  );
  const nodesForRf = useMemo(
    () => {
      const canvasNodesById = new Map(transformCanvas.nodes.map((node) => [node.id, node]));
      return nodes.map((n) => {
        const cn = canvasNodesById.get(n.id);
        const matches = cn ? transformCanvasNodeMatchesSearch(cn, searchQuery, t) : true;
        const withRun = applyTransformFlowRunDisplayClasses(n, {
          runFailed: runFailedSet.has(n.id),
          runWarning: runWarningSet.has(n.id),
          executing: runExecutingSet.has(n.id),
          completed: runCompletedSet.has(n.id),
          dimmed: searchActive && !matches,
          validationFailed: validationFailedSet.has(n.id),
        });
        const oriented = applyFlowHandleOrientationToNode(withRun, handleOrientation);
        return {
          ...oriented,
          data: {
            ...(oriented.data as Record<string, unknown>),
            canvas_resize_enabled: !readOnly,
            nodeRunProgress: runProgress?.nodeProgressById[n.id],
            nodeRunExecuting: runExecutingSet.has(n.id),
          },
        };
      });
    },
    [
      nodes,
      transformCanvas.nodes,
      searchQuery,
      searchActive,
      runFailedSet,
      runWarningSet,
      runExecutingSet,
      runCompletedSet,
      validationFailedSet,
      handleOrientation,
      readOnly,
      runProgress?.nodeProgressById,
    ]
  );

  return (
    <div className="transform-flow-panel">
      {!readOnly ? (
      <div className="transform-flow-toolbar" role="toolbar" aria-label={t("transform.toolbar.aria")}>
        <button
          type="button"
          className="disc-btn disc-btn--primary"
          disabled={readOnly || saving || reloading || runBusy || !onSave}
          onClick={onSave}
        >
          {saving ? t("transform.toolbar.saving") : t("transform.toolbar.save")}
        </button>
        {onSaveAs ? (
          <button
            type="button"
            className="disc-btn"
            disabled={saving || reloading || runBusy}
            onClick={onSaveAs}
            title={t("transform.saveAs.hint")}
          >
            {t("transform.toolbar.saveAs")}
          </button>
        ) : null}
        {onReload ? (
          <button
            type="button"
            className="disc-btn"
            disabled={saving || reloading || runBusy}
            onClick={onReload}
            title={t("transform.toolbar.reloadHint")}
          >
            {reloading ? t("transform.toolbar.reloading") : t("btn.reload")}
          </button>
        ) : null}
        <button
          type="button"
          className="disc-btn"
          disabled={readOnly || saving || reloading || runBusy}
          onClick={onOpenOptimizeTransforms}
          title={t("transform.optimize.buttonHint")}
        >
          {t("transform.optimize.button")}
        </button>
        <button
          type="button"
          className="disc-btn"
          disabled={saving || reloading || runBusy || !onValidate}
          onClick={onValidate}
        >
          {t("transform.toolbar.validate")}
        </button>
        <button
          type="button"
          className="disc-btn"
          disabled={saving || reloading || runBusy || !onBuild}
          onClick={onBuild}
        >
          {t("transform.toolbar.build")}
        </button>
        {runScopeEnabled && onRunScopeChange ? (
          <label className="transform-flow-toolbar__run-scope">
            <span className="transform-flow-toolbar__run-scope-label">{t("transform.toolbar.runScope")}</span>
            <select
              className="gov-input"
              value={runScope}
              onChange={(e) => onRunScopeChange(e.target.value as "incremental" | "all")}
              title={t("transform.toolbar.runScopeHint")}
              disabled={saving || reloading || runBusy}
            >
              <option value="incremental">{t("transform.toolbar.runScopeIncremental")}</option>
              <option value="all">{t("transform.toolbar.runScopeAll")}</option>
            </select>
          </label>
        ) : null}
        {runBusy && onCancelLocalRun ? (
          <button type="button" className="disc-btn" onClick={onCancelLocalRun}>
            {t("transform.toolbar.cancelLocalRun")}
          </button>
        ) : (
          <button
            type="button"
            className="disc-btn"
            disabled={!onRun || saving || reloading}
            onClick={() =>
              onRun?.({
                incrementalChangeProcessing: runScope === "incremental",
                dryRun,
              })
            }
          >
            {t("transform.toolbar.runLocal")}
          </button>
        )}
        {onDryRunChange ? (
          <TransformLocalRunDryRunField
            t={t}
            dryRun={dryRun}
            onDryRunChange={onDryRunChange}
            disabled={saving || reloading || runBusy}
          />
        ) : null}
        {onRename ? (
          <button
            type="button"
            className="disc-btn"
            disabled={readOnly || saving || reloading || runBusy}
            onClick={onRename}
            title={t("transform.pipelines.rename")}
          >
            {t("transform.pipelines.rename")}
          </button>
        ) : null}
        {onDelete ? (
          <button
            type="button"
            className="disc-btn disc-btn--danger"
            disabled={readOnly || saving || reloading || runBusy}
            onClick={onDelete}
            title={t("transform.pipelines.delete")}
          >
            {t("transform.pipelines.delete")}
          </button>
        ) : null}
        {optimizeNotice ? (
          <span className="transform-flow-toolbar__status" role="status">
            {optimizeNotice}
          </span>
        ) : null}
        {statusMessage ? (
          <span className="transform-flow-toolbar__status" role="status">
            {statusMessage}
          </span>
        ) : null}
      </div>
      ) : null}
      {transformCanvas.nodes.length > 0 ? (
        <div className="transform-flow-search-row">
          <TransformCanvasSearchField
            t={t}
            searchQuery={searchQuery}
            onSearchQueryChange={setSearchQuery}
          />
          <TransformFlowLayoutControls
            t={t}
            readOnly={readOnly}
            handleOrientation={handleOrientation}
            onHandleOrientationChange={onHandleOrientationChange}
            layoutMethod={layoutMethod}
            layoutMethodOptions={TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS}
            onLayoutMethodChange={onLayoutMethodChange}
            edgePathStyle={edgePathStyle}
            onEdgePathStyleChange={onEdgePathStyleChange}
            onAutoLayout={handleAutoLayout}
            onFitView={() => fitView({ padding: 0.15, duration: 200 })}
            alignDisabled={readOnly || alignableSelectionCount < 2}
            onAlignSelection={applySelectionAlign}
            showAlign={!readOnly}
            canUndo={flowHistory.canUndo}
            canRedo={flowHistory.canRedo}
            onUndo={readOnly ? undefined : handleUndo}
            onRedo={readOnly ? undefined : handleRedo}
          />
        </div>
      ) : null}
      {transformCanvas.nodes.length > 0 ? (
        <>
          <TransformCanvasSearchResults
            t={t}
            searchQuery={searchQuery}
            searchMatches={searchMatches}
            selectedNodeId={selectedNode?.id ?? null}
            onSelectNode={selectCanvasNodeFromSearch}
          />
          {searchQuery.trim().length === 0 ? (
            <TransformCanvasNodeList
              t={t}
              nodes={transformCanvas.nodes}
              selectedNodeId={selectedNode?.id ?? null}
              onSelectNode={selectCanvasNodeFromSearch}
            />
          ) : null}
        </>
      ) : null}
      <div className="transform-flow-body">
        <FlowPalette t={t} readOnly={readOnly} />
        <div
          ref={flowRootRef}
          className="transform-flow-canvas-wrap"
          onDragOver={onDragOver}
          onDrop={onDrop}
          data-local-run-active={runProgress?.busy ? "true" : undefined}
        >
          <FlowHandleOrientationProvider value={handleOrientation}>
            <ReactFlow
            colorMode={theme}
            nodes={nodesForRf}
            edges={edgesForRf}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChangeWrapped}
            onEdgesChange={onEdgesChangeWrapped}
            onNodeDragStop={onNodeDragStop}
            onConnect={onConnect}
            onReconnect={readOnly ? undefined : onReconnect}
            edgesReconnectable={!readOnly}
            onConnectEnd={readOnly ? undefined : onConnectEnd}
            onMoveEnd={onMoveEnd}
            defaultViewport={flowDefaultViewport}
            snapToGrid={!readOnly}
            snapGrid={[16, 16]}
            onlyRenderVisibleElements={nodesForRf.length > 48}
            isValidConnection={(conn) => {
              const c: Connection = {
                source: conn.source,
                target: conn.target,
                sourceHandle: conn.sourceHandle ?? null,
                targetHandle: conn.targetHandle ?? null,
              };
              return isValidEtlFlowConnection(c, getNode) && !wouldCreateCycle(getEdges(), c.source, c.target);
            }}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onNodeDoubleClick={onNodeDoubleClick}
            onPaneClick={onPaneClick}
            onNodeContextMenu={readOnly ? undefined : onFlowNodeContextMenu}
            onPaneContextMenu={readOnly ? undefined : onFlowPaneContextMenu}
            onEdgeContextMenu={readOnly ? undefined : onFlowEdgeContextMenu}
            deleteKeyCode={readOnly ? null : ["Backspace", "Delete"]}
            nodesDraggable={!readOnly}
            nodesConnectable={!readOnly}
            elementsSelectable
            connectionLineType={connectionLineType}
            defaultEdgeOptions={defaultEdgeOptions}
            proOptions={{ hideAttribution: true }}
          >
            <FlowHandleOrientationEdgeSync orientation={handleOrientation} edgePathStyle={edgePathStyle} />
            <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
            <Controls showInteractive={!readOnly} />
            <MiniMap pannable zoomable />
            <FocusTransformFlowNode nodeId={focusNodeId} />
            {toolbarNodes.map((n) => (
              <TransformFlowNodeToolbar
                key={n.id}
                t={t}
                node={n}
                readOnly={readOnly}
                onOpenEditor={setEditorModalNode}
                onToggleEnabled={onToggleNodeEnabled}
                onCopy={() => flowClipboard.copySelection()}
              />
            ))}
          </ReactFlow>
          </FlowHandleOrientationProvider>
          {connectEndMenu ? (
            <FlowStagePickerMenu
              screen={connectEndMenu.screen}
              groups={connectEndMenuGroupedOptionsForSourceType(getNode(connectEndMenu.sourceNodeId)?.type)}
              groupId={connectEndMenuGroupId}
              onGroupIdChange={setConnectEndMenuGroupId}
              onPick={commitConnectEndMenu}
              ariaLabel={t("transform.connectEnd.title")}
              t={t}
            />
          ) : null}
          {paneAddMenu ? (
            <FlowStagePickerMenu
              screen={paneAddMenu.screen}
              groups={connectEndMenuGroupedOptionsForPane()}
              groupId={paneAddMenuGroupId}
              onGroupIdChange={setPaneAddMenuGroupId}
              onPick={commitPaneAddNode}
              ariaLabel={t("transform.contextMenu.addNode")}
              t={t}
            />
          ) : null}
          {entityDropMenu ? (
            <FlowEntityDropMenu
              screen={entityDropMenu.screen}
              entityLabel={entityDropMenu.node.label.trim() || entityDropMenu.node.id}
              options={entityDropMenuOptions(entityDropMenu.node) ?? []}
              onPick={commitEntityDropMenu}
              t={t}
            />
          ) : null}
          {handlerDropMenu ? (
            <FlowHandlerDropMenu
              screen={handlerDropMenu.screen}
              stage={handlerDropMenu.stage}
              groupId={handlerDropMenuGroupId}
              onGroupIdChange={setHandlerDropMenuGroupId}
              onPick={commitHandlerDropMenu}
              t={t}
            />
          ) : null}
          <TreeContextMenuPortal menu={flowCtxMenu.menu} onClose={flowCtxMenu.close} classPrefix="disc" />
        </div>
        <FlowNodeInspector
          t={t}
          pipelineId={pipelineId}
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          flowNodes={nodes}
          readOnly={readOnly}
          onPatchNode={onPatchNode}
          onPatchEdge={readOnly ? undefined : onPatchEdge}
          onOpenEditor={
            readOnly
              ? (n) => {
                  const k = rfTypeToKind(n.type);
                  if (isOrchestrationNodeKind(k)) setEditorModalNode(n);
                }
              : setEditorModalNode
          }
          onOpenPreviewQuery={onOpenNodePreviewQuery}
          onDeleteNode={readOnly ? undefined : onDeleteNode}
          onDeleteEdge={readOnly ? undefined : removeEdgeById}
        />
      </div>
      {editorModalNode ? (
        <FlowNodeEditorModal
          node={nodes.find((n) => n.id === editorModalNode.id) ?? editorModalNode}
          onClose={closeEditorModal}
          onPatchNode={onPatchNode}
          t={t}
          pipelineId={pipelineId}
          flowNodes={nodes}
          flowEdges={edges}
          readOnly={readOnly}
        />
      ) : null}
      <OptimizeTransformsDialog
        open={optimizeOpen}
        onClose={() => setOptimizeOpen(false)}
        candidates={optimizeCandidates}
        getNode={getNode}
        onApply={onApplyOptimizeTransforms}
      />
    </div>
  );
}

export function TransformFlowPanel(props: Props) {
  return (
    <ReactFlowProvider>
      <FlowCanvasBody {...props} />
    </ReactFlowProvider>
  );
}
