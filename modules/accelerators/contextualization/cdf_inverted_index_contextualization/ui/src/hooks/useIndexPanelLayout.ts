import { useCallback, useEffect, useRef, useState } from "react";

const LS_KEY = "cdf-inverted-index.panelLayout.v1";

const TREE_MIN = 180;
const TREE_MAX = 480;
const DEFAULT_TREE_WIDTH = 280;

const PROPS_MIN = 120;
const DEFAULT_PROPS_SIZE = 220;

type StoredLayout = {
  treeWidth?: number;
  treeCollapsed?: boolean;
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

export function useIndexPanelLayout() {
  const stored = readStoredLayout();
  const [treeWidth, setTreeWidth] = useState(
    typeof stored.treeWidth === "number" && Number.isFinite(stored.treeWidth)
      ? clamp(stored.treeWidth, TREE_MIN, TREE_MAX)
      : DEFAULT_TREE_WIDTH
  );
  const [treeCollapsed, setTreeCollapsed] = useState(stored.treeCollapsed === true);
  const [propertiesSize, setPropertiesSize] = useState(
    typeof stored.propertiesSize === "number" && Number.isFinite(stored.propertiesSize)
      ? clamp(stored.propertiesSize, PROPS_MIN, Math.round(window.innerHeight * 0.45))
      : DEFAULT_PROPS_SIZE
  );
  const [propertiesCollapsed, setPropertiesCollapsed] = useState(stored.propertiesCollapsed === true);
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
          treeWidth,
          treeCollapsed,
          propertiesSize,
          propertiesCollapsed,
        } satisfies StoredLayout)
      );
    } catch {
      /* ignore */
    }
  }, [treeWidth, treeCollapsed, propertiesSize, propertiesCollapsed]);

  useEffect(() => {
    const mq = window.matchMedia("(max-width: 900px)");
    const apply = () => setTreeCollapsed(mq.matches);
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, []);

  const treeMaxWidth = useCallback(
    () => Math.min(TREE_MAX, Math.round(window.innerWidth * 0.45)),
    []
  );

  const propsMaxHeight = useCallback(() => Math.round(window.innerHeight * 0.45), []);

  const onResizeTreeStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (treeCollapsed) return;
      const startX = e.clientX;
      const startW = treeWidth;
      const onMove = (ev: MouseEvent) => {
        setTreeWidth(clamp(startW + (ev.clientX - startX), TREE_MIN, treeMaxWidth()));
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
    [treeCollapsed, treeWidth, treeMaxWidth]
  );

  const onResizePropertiesStart = useCallback(
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

  const setTreeWidthClamped = useCallback(
    (next: number) => {
      setTreeWidth(clamp(next, TREE_MIN, treeMaxWidth()));
    },
    [treeMaxWidth]
  );

  const setPropertiesSizeClamped = useCallback(
    (next: number) => {
      setPropertiesSize(clamp(next, PROPS_MIN, propsMaxHeight()));
    },
    [propsMaxHeight]
  );

  return {
    treeWidth,
    treeCollapsed,
    toggleTreeCollapsed: () => setTreeCollapsed((c) => !c),
    onResizeTreeStart,
    propertiesSize,
    propertiesCollapsed,
    togglePropertiesCollapsed: () => setPropertiesCollapsed((c) => !c),
    onResizePropertiesStart,
    treeMin: TREE_MIN,
    treeMax: TREE_MAX,
    propsMin: PROPS_MIN,
    setTreeWidthClamped,
    setPropertiesSizeClamped,
    propsMaxHeight,
  };
}
