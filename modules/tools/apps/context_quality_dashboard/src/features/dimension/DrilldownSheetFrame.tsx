import { Button } from '@cognite/aura/components/button';
import { useCallback, useEffect, useRef, useState, type PointerEvent as ReactPointerEvent, type ReactNode } from 'react';

import {
  clampDrilldownSheetWidth,
  DRILLDOWN_SHEET_DEFAULT_WIDTH_PX,
} from './drilldownSidesheetLayout';

type DrilldownSheetFrameProps = {
  t: (key: string) => string;
  title: string;
  subtitle?: string;
  onClose: () => void;
  children: ReactNode;
};

/** Resizable side sheet for drill-down: drag the left edge to resize; header has close only. */
export function DrilldownSheetFrame({ t, title, subtitle, onClose, children }: DrilldownSheetFrameProps) {
  const [panelWidthPx, setPanelWidthPx] = useState(DRILLDOWN_SHEET_DEFAULT_WIDTH_PX);
  const resizeDragRef = useRef<{ startX: number; startWidth: number } | null>(null);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose]);

  const onResizePointerDown = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    resizeDragRef.current = { startX: event.clientX, startWidth: panelWidthPx };
    event.currentTarget.setPointerCapture(event.pointerId);
  }, [panelWidthPx]);

  const onResizePointerMove = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    const drag = resizeDragRef.current;
    if (drag === null) {
      return;
    }
    const deltaX = drag.startX - event.clientX;
    setPanelWidthPx(clampDrilldownSheetWidth(drag.startWidth + deltaX, window.innerWidth));
  }, []);

  const onResizePointerUp = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    resizeDragRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  }, []);

  return (
    <aside
      role="dialog"
      aria-modal="true"
      aria-labelledby="drilldown-sidesheet-title"
      data-testid="drilldown-sidesheet"
      style={{ width: `${panelWidthPx}px`, maxWidth: '100vw' }}
      className="fixed inset-y-0 right-0 z-30 flex flex-col border-l border-border bg-card-background shadow-lg"
    >
      <div
        role="separator"
        aria-orientation="vertical"
        aria-label={t('dimension.drilldown.resizeHandle')}
        data-testid="drilldown-sidesheet-resize"
        className="absolute inset-y-0 left-0 z-10 w-2 -translate-x-1/2 cursor-col-resize touch-none bg-transparent hover:bg-border-emphasized/40 active:bg-border-emphasized/60"
        onPointerDown={onResizePointerDown}
        onPointerMove={onResizePointerMove}
        onPointerUp={onResizePointerUp}
        onPointerCancel={onResizePointerUp}
      />
      <div className="flex min-h-0 flex-1 flex-col p-6">
        <div className="flex shrink-0 items-start justify-between gap-3 border-b border-border pb-4">
          <div className="min-w-0">
            <h2 id="drilldown-sidesheet-title" className="truncate text-base font-semibold text-foreground">
              {title}
            </h2>
            {subtitle !== undefined ? <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p> : null}
          </div>
          <div className="flex shrink-0 items-center gap-1">
            <Button variant="ghost" size="icon-sm" onClick={onClose} aria-label={t('dimension.closeDrilldown')}>
              ×
            </Button>
          </div>
        </div>
        {children}
      </div>
    </aside>
  );
}
