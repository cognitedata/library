import { useCallback, useEffect, useRef, useState } from "react";

const LS_KEY = "keaFlow.panelLayout.v1";

const LEFT_MIN = 160;
const LEFT_MAX = 440;
const RIGHT_MIN = 200;
const RIGHT_MAX = 560;
const DEFAULT_LEFT = 224;
const DEFAULT_RIGHT = 288;
const COLLAPSED_STRIP_PX = 36;

export type FlowPanelLayout = {
  leftWidth: number;
  rightWidth: number;
  leftCollapsed: boolean;
  rightCollapsed: boolean;
  collapsedStripPx: number;
  leftMin: number;
  leftMax: number;
  rightMin: number;
  rightMax: number;
  collapseLeft: () => void;
  expandLeft: () => void;
  collapseRight: () => void;
  expandRight: () => void;
  onResizeLeftStart: (e: React.MouseEvent) => void;
  onResizeRightStart: (e: React.MouseEvent) => void;
};

function clamp(n: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, n));
}

export function useFlowPanelLayout(): FlowPanelLayout {
  const [leftWidth, setLeftWidth] = useState(DEFAULT_LEFT);
  const [rightWidth, setRightWidth] = useState(DEFAULT_RIGHT);
  const [leftCollapsed, setLeftCollapsed] = useState(false);
  const [rightCollapsed, setRightCollapsed] = useState(false);

  const leftWidthBeforeCollapse = useRef(DEFAULT_LEFT);
  const rightWidthBeforeCollapse = useRef(DEFAULT_RIGHT);
  const hydrated = useRef(false);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(LS_KEY);
      if (!raw) {
        hydrated.current = true;
        return;
      }
      const j = JSON.parse(raw) as Record<string, unknown>;
      if (typeof j.leftWidth === "number" && Number.isFinite(j.leftWidth)) {
        setLeftWidth(clamp(j.leftWidth, LEFT_MIN, LEFT_MAX));
      }
      if (typeof j.rightWidth === "number" && Number.isFinite(j.rightWidth)) {
        setRightWidth(clamp(j.rightWidth, RIGHT_MIN, RIGHT_MAX));
      }
      if (j.leftCollapsed === true) setLeftCollapsed(true);
      if (j.rightCollapsed === true) setRightCollapsed(true);
    } catch {
      /* ignore */
    }
    hydrated.current = true;
  }, []);

  useEffect(() => {
    if (!hydrated.current) return;
    try {
      localStorage.setItem(
        LS_KEY,
        JSON.stringify({
          leftWidth,
          rightWidth,
          leftCollapsed,
          rightCollapsed,
        })
      );
    } catch {
      /* ignore */
    }
  }, [leftWidth, rightWidth, leftCollapsed, rightCollapsed]);

  const dragRef = useRef<null | { which: "left" | "right"; startX: number; startLeft: number; startRight: number }>(
    null
  );

  const onResizeLeftStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (leftCollapsed) return;
      dragRef.current = {
        which: "left",
        startX: e.clientX,
        startLeft: leftWidth,
        startRight: rightWidth,
      };
      const onMove = (ev: MouseEvent) => {
        const d = dragRef.current;
        if (!d || d.which !== "left") return;
        const next = Math.round(d.startLeft + (ev.clientX - d.startX));
        setLeftWidth(clamp(next, LEFT_MIN, LEFT_MAX));
      };
      const onUp = () => {
        dragRef.current = null;
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
    [leftCollapsed, leftWidth, rightWidth]
  );

  const onResizeRightStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (rightCollapsed) return;
      dragRef.current = {
        which: "right",
        startX: e.clientX,
        startLeft: leftWidth,
        startRight: rightWidth,
      };
      const onMove = (ev: MouseEvent) => {
        const d = dragRef.current;
        if (!d || d.which !== "right") return;
        const next = Math.round(d.startRight - (ev.clientX - d.startX));
        setRightWidth(clamp(next, RIGHT_MIN, RIGHT_MAX));
      };
      const onUp = () => {
        dragRef.current = null;
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
    [rightCollapsed, leftWidth, rightWidth]
  );

  const collapseLeft = useCallback(() => {
    leftWidthBeforeCollapse.current = leftWidth;
    setLeftCollapsed(true);
  }, [leftWidth]);

  const expandLeft = useCallback(() => {
    setLeftWidth(clamp(leftWidthBeforeCollapse.current, LEFT_MIN, LEFT_MAX));
    setLeftCollapsed(false);
  }, []);

  const collapseRight = useCallback(() => {
    rightWidthBeforeCollapse.current = rightWidth;
    setRightCollapsed(true);
  }, [rightWidth]);

  const expandRight = useCallback(() => {
    setRightWidth(clamp(rightWidthBeforeCollapse.current, RIGHT_MIN, RIGHT_MAX));
    setRightCollapsed(false);
  }, []);

  return {
    leftWidth,
    rightWidth,
    leftCollapsed,
    rightCollapsed,
    collapsedStripPx: COLLAPSED_STRIP_PX,
    leftMin: LEFT_MIN,
    leftMax: LEFT_MAX,
    rightMin: RIGHT_MIN,
    rightMax: RIGHT_MAX,
    collapseLeft,
    expandLeft,
    collapseRight,
    expandRight,
    onResizeLeftStart,
    onResizeRightStart,
  };
}
