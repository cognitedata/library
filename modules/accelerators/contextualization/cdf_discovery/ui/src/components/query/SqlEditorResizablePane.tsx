import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { AccessibleResizeHandle } from "../AccessibleResizeHandle";
import { useVerticalPaneResize } from "../../hooks/useVerticalPaneResize";

type Props = {
  label: ReactNode;
  children: ReactNode | ((bodyHeight: number) => ReactNode);
  storageKey?: string;
};

/** SQL query area in a vertically resizable pane (drag handle below, not the textarea). */
export function SqlEditorResizablePane({
  label,
  children,
  storageKey = "transform.sqlEditorPaneHeight.v1",
}: Props) {
  const { height, onResizeStart, setHeight } = useVerticalPaneResize({
    storageKey,
    initialHeight: 240,
  });
  const bodyRef = useRef<HTMLDivElement>(null);
  const [bodyHeight, setBodyHeight] = useState(0);

  useLayoutEffect(() => {
    const el = bodyRef.current;
    if (!el) return;
    const update = () => setBodyHeight(el.clientHeight);
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, [height]);

  const content = typeof children === "function" ? children(bodyHeight) : children;

  return (
    <div className="transform-query-sql-stack">
      <label className="transform-query-label transform-query-label--block transform-query-fields__query-label">
        {label}
      </label>
      <div className="transform-query-sql-pane" style={{ height, maxHeight: height }}>
        <div ref={bodyRef} className="transform-query-sql-pane__body">
          {content}
        </div>
      </div>
      <AccessibleResizeHandle
        className="transform-query-resize-handle-v"
        orientation="horizontal"
        value={height}
        min={80}
        max={Math.round(window.innerHeight * 0.5)}
        labelKey="transform.query.resizeSqlPane"
        onMouseDown={onResizeStart}
        onValueChange={setHeight}
      />
    </div>
  );
}
