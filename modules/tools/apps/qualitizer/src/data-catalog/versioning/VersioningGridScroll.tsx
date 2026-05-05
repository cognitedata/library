import { useCallback, useRef, type RefObject } from "react";

/** Height of the version-header strip (matches version label layout). */
export const GRID_VERSION_HEADER_HEIGHT = 28;

const hideHxScrollClass =
  "overflow-x-auto overflow-y-hidden [scrollbar-width:none] [-ms-overflow-style:none] [&::-webkit-scrollbar]:hidden";

type Props = {
  /** Scrollable content width (full SVG width). */
  contentWidth: number;
  svgWidth: number;
  headerHeight?: number;
  bodySvgHeight: number;
  headerSvgRef: RefObject<SVGSVGElement | null>;
  bodySvgRef: RefObject<SVGSVGElement | null>;
  maxHeightClassName?: string;
};

export function VersioningGridScroll({
  contentWidth,
  svgWidth,
  headerHeight = GRID_VERSION_HEADER_HEIGHT,
  bodySvgHeight,
  headerSvgRef,
  bodySvgRef,
  maxHeightClassName = "max-h-[min(78vh,calc(100vh-11rem))]",
}: Props) {
  const headerHScrollRef = useRef<HTMLDivElement>(null);
  const bodyHScrollRef = useRef<HTMLDivElement>(null);
  const topRailRef = useRef<HTMLDivElement>(null);
  const syncing = useRef(false);

  const applyHorizontalFrom = useCallback(
    (source: "header" | "body" | "rail", rail?: HTMLDivElement | null) => {
      let sl: number;
      if (source === "header") {
        sl = headerHScrollRef.current?.scrollLeft ?? 0;
      } else if (source === "body") {
        sl = bodyHScrollRef.current?.scrollLeft ?? 0;
      } else {
        sl = rail?.scrollLeft ?? 0;
      }
      if (syncing.current) return;
      syncing.current = true;
      if (headerHScrollRef.current) headerHScrollRef.current.scrollLeft = sl;
      if (bodyHScrollRef.current) bodyHScrollRef.current.scrollLeft = sl;
      if (topRailRef.current) topRailRef.current.scrollLeft = sl;
      requestAnimationFrame(() => {
        syncing.current = false;
      });
    },
    []
  );

  const scrollMainBy = useCallback((deltaX: number) => {
    headerHScrollRef.current?.scrollBy({ left: deltaX, behavior: "smooth" });
    bodyHScrollRef.current?.scrollBy({ left: deltaX, behavior: "smooth" });
  }, []);

  const onHeaderHScroll = useCallback(() => applyHorizontalFrom("header"), [applyHorizontalFrom]);
  const onBodyHScroll = useCallback(() => applyHorizontalFrom("body"), [applyHorizontalFrom]);
  const onTopRailScroll = useCallback(() => {
    applyHorizontalFrom("rail", topRailRef.current);
  }, [applyHorizontalFrom]);

  const nudge = Math.min(240, contentWidth * 0.25);

  return (
    <div
      className={`flex min-h-0 flex-col overflow-hidden rounded-md border border-slate-200 bg-sky-100 ${maxHeightClassName}`}
    >
      <div className="shrink-0">
        <div className="flex items-center gap-2 border-b border-slate-200 bg-slate-100 px-2 py-1.5">
          <button
            type="button"
            className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
            aria-label="Scroll grid left"
            onClick={() => scrollMainBy(-nudge)}
          >
            ◀
          </button>
          <div
            ref={topRailRef}
            className="h-4 min-h-[1rem] min-w-[120px] flex-1 cursor-grab overflow-x-auto rounded border border-slate-200 bg-white active:cursor-grabbing"
            onScroll={onTopRailScroll}
          >
            <div style={{ width: contentWidth, height: 1 }} aria-hidden />
          </div>
          <button
            type="button"
            className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
            aria-label="Scroll grid right"
            onClick={() => scrollMainBy(nudge)}
          >
            ▶
          </button>
        </div>

        <div className="border-b border-slate-300/80 bg-sky-100">
          <div ref={headerHScrollRef} className={hideHxScrollClass} onScroll={onHeaderHScroll}>
            <svg ref={headerSvgRef} className="block" width={svgWidth} height={headerHeight} />
          </div>
        </div>
      </div>

      <div className="min-h-0 min-w-0 flex-1 overflow-x-hidden overflow-y-auto">
        <div ref={bodyHScrollRef} className={hideHxScrollClass} onScroll={onBodyHScroll}>
          <svg ref={bodySvgRef} className="block" width={svgWidth} height={bodySvgHeight} />
        </div>
      </div>
    </div>
  );
}
