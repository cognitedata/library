import { useCallback, useEffect, useRef, useState } from "react";
import type { DraggablePanel } from "../components/PanelDragHandle";

const LS_KEY = "cdf-discovery.panelLayout.v1";

const TREE_MIN = 180;
const TREE_MAX = 480;
const DEFAULT_TREE_WIDTH = 280;

const PROPS_MIN = 80;
const DEFAULT_PROPS_SIZE = 200;

export type TreePanelSide = "left" | "right";
export type PropertiesPanelDock = "left-bottom" | "bottom" | "right";

type StoredLayout = {
  treeSide?: TreePanelSide;
  treeWidth?: number;
  treeHidden?: boolean;
  propertiesDock?: PropertiesPanelDock;
  propertiesSize?: number;
  propertiesCollapsed?: boolean;
};

function clamp(n: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, n));
}

function readStoredLayout(): StoredLayout {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return {};
    return JSON.parse(raw) as StoredLayout;
  } catch {
    return {};
  }
}

function isTreeSide(v: unknown): v is TreePanelSide {
  return v === "left" || v === "right";
}

function isPropertiesDock(v: unknown): v is PropertiesPanelDock {
  return v === "left-bottom" || v === "bottom" || v === "right";
}

export function useDiscoveryPanelLayout() {
  const stored = readStoredLayout();

  const [treeSide, setTreeSide] = useState<TreePanelSide>(
    isTreeSide(stored.treeSide) ? stored.treeSide : "left"
  );
  const [treeWidth, setTreeWidth] = useState(
    typeof stored.treeWidth === "number" && Number.isFinite(stored.treeWidth)
      ? clamp(stored.treeWidth, TREE_MIN, TREE_MAX)
      : DEFAULT_TREE_WIDTH
  );
  const [treeHidden, setTreeHidden] = useState(stored.treeHidden === true);

  const [propertiesDock, setPropertiesDock] = useState<PropertiesPanelDock>(
    isPropertiesDock(stored.propertiesDock) ? stored.propertiesDock : "left-bottom"
  );
  const [propertiesSize, setPropertiesSize] = useState(
    typeof stored.propertiesSize === "number" && Number.isFinite(stored.propertiesSize)
      ? clamp(stored.propertiesSize, PROPS_MIN, Math.round(window.innerHeight * 0.5))
      : DEFAULT_PROPS_SIZE
  );
  const [propertiesCollapsed, setPropertiesCollapsed] = useState(stored.propertiesCollapsed === true);
  const [draggingPanel, setDraggingPanel] = useState<DraggablePanel | null>(null);
  const dragFrameRef = useRef<number | null>(null);

  const hydrated = useRef(false);

  useEffect(() => {
    hydrated.current = true;
  }, []);

  useEffect(() => {
    if (!hydrated.current) return;
    try {
      localStorage.setItem(
        LS_KEY,
        JSON.stringify({
          treeSide,
          treeWidth,
          treeHidden,
          propertiesDock,
          propertiesSize,
          propertiesCollapsed,
        } satisfies StoredLayout)
      );
    } catch {
      /* ignore */
    }
  }, [treeSide, treeWidth, treeHidden, propertiesDock, propertiesSize, propertiesCollapsed]);

  useEffect(() => {
    const mq = window.matchMedia("(max-width: 900px)");
    const apply = () => setTreeHidden(mq.matches);
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, []);

  const propsMaxHeight = useCallback(() => Math.round(window.innerHeight * 0.5), []);
  const propsMaxWidth = useCallback(
    () => Math.min(560, Math.round(window.innerWidth * 0.45)),
    []
  );
  const treeMaxWidth = useCallback(
    () => Math.min(TREE_MAX, Math.round(window.innerWidth * 0.55)),
    []
  );

  const onResizeTreeStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (treeHidden) return;
      const startX = e.clientX;
      const startW = treeWidth;
      const onMove = (ev: MouseEvent) => {
        const delta = treeSide === "left" ? ev.clientX - startX : startX - ev.clientX;
        setTreeWidth(clamp(startW + delta, TREE_MIN, treeMaxWidth()));
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [treeHidden, treeSide, treeWidth, treeMaxWidth]
  );

  const onResizePropertiesBottomStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (propertiesCollapsed) return;
      const startY = e.clientY;
      const startH = propertiesSize;
      const onMove = (ev: MouseEvent) => {
        setPropertiesSize(
          clamp(startH - (ev.clientY - startY), PROPS_MIN, propsMaxHeight())
        );
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [propertiesCollapsed, propertiesSize, propsMaxHeight]
  );

  const onResizePropertiesStackedStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (propertiesCollapsed) return;
      const startY = e.clientY;
      const startH = propertiesSize;
      const onMove = (ev: MouseEvent) => {
        setPropertiesSize(
          clamp(startH + (ev.clientY - startY), PROPS_MIN, propsMaxHeight())
        );
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [propertiesCollapsed, propertiesSize, propsMaxHeight]
  );

  const onResizePropertiesSideStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (propertiesCollapsed) return;
      const startX = e.clientX;
      const startW = propertiesSize;
      const onMove = (ev: MouseEvent) => {
        const delta = propertiesDock === "right" ? startX - ev.clientX : ev.clientX - startX;
        setPropertiesSize(clamp(startW + delta, PROPS_MIN, propsMaxWidth()));
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [propertiesCollapsed, propertiesDock, propertiesSize, propsMaxWidth]
  );

  const togglePropertiesCollapsed = useCallback(() => {
    setPropertiesCollapsed((c) => !c);
  }, []);

  const toggleTreeHidden = useCallback(() => {
    setTreeHidden((h) => !h);
  }, []);

  const beginPanelDrag = useCallback((panel: DraggablePanel) => {
    // Defer overlay updates until after dragstart completes.
    // Synchronous setState during dragstart can cancel HTML5 drag in some browsers.
    if (dragFrameRef.current != null) {
      cancelAnimationFrame(dragFrameRef.current);
    }
    dragFrameRef.current = requestAnimationFrame(() => {
      dragFrameRef.current = null;
      setDraggingPanel(panel);
    });
  }, []);

  const endPanelDrag = useCallback(() => {
    if (dragFrameRef.current != null) {
      cancelAnimationFrame(dragFrameRef.current);
      dragFrameRef.current = null;
    }
    setDraggingPanel(null);
  }, []);

  const dropTreeSide = useCallback((side: TreePanelSide) => {
    setTreeSide(side);
    setDraggingPanel(null);
  }, []);

  const dropPropertiesDock = useCallback((dock: PropertiesPanelDock) => {
    setPropertiesDock(dock);
    setDraggingPanel(null);
  }, []);

  const sideColumnWidth =
    propertiesDock === "left-bottom" && !treeHidden
      ? treeWidth
      : treeHidden
        ? propertiesDock === "left-bottom"
          ? Math.min(treeWidth, 320)
          : 0
        : treeWidth;

  const showSideColumn =
    (!treeHidden && (propertiesDock === "left-bottom" || treeSide === "left" || treeSide === "right")) ||
    (treeHidden && propertiesDock === "left-bottom");

  return {
    treeSide,
    setTreeSide,
    treeWidth,
    treeHidden,
    toggleTreeHidden,
    onResizeTreeStart,
    propertiesDock,
    setPropertiesDock,
    propertiesSize,
    propertiesCollapsed,
    togglePropertiesCollapsed,
    onResizePropertiesBottomStart,
    onResizePropertiesStackedStart,
    onResizePropertiesSideStart,
    sideColumnWidth,
    showSideColumn,
    treeMin: TREE_MIN,
    treeMax: TREE_MAX,
    propsMin: PROPS_MIN,
    draggingPanel,
    beginPanelDrag,
    endPanelDrag,
    dropTreeSide,
    dropPropertiesDock,
  };
}
