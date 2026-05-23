import { useEffect, useState } from "react";
import type { MessageKey } from "../i18n";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  pageIndex: number;
  pageCount: number;
  disabled?: boolean;
  onPageIndexChange: (pageIndex: number) => void;
};

function commitPageDraft(draft: string, pageCount: number): number | null {
  const trimmed = draft.trim();
  if (!trimmed) return null;
  const n = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(pageCount - 1, n - 1));
}

/** Numeric page jump field (1-based display) for query/grid pagination. */
export function PaginationPageJump({ t, pageIndex, pageCount, disabled, onPageIndexChange }: Props) {
  const [draft, setDraft] = useState(String(pageIndex + 1));
  const [focused, setFocused] = useState(false);

  useEffect(() => {
    if (!focused) setDraft(String(pageIndex + 1));
  }, [pageIndex, focused]);

  const commit = () => {
    const next = commitPageDraft(draft, pageCount);
    if (next === null) {
      setDraft(String(pageIndex + 1));
      return;
    }
    onPageIndexChange(next);
    setDraft(String(next + 1));
  };

  const inputLabel = t("grid.pageJumpInput", { pages: String(pageCount) });

  return (
    <label className="disc-pagination__jump" title={inputLabel}>
      <input
        type="number"
        className="disc-input disc-pagination__jump-input"
        min={1}
        max={pageCount}
        disabled={disabled}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => {
          setFocused(false);
          commit();
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            (e.target as HTMLInputElement).blur();
          }
        }}
        aria-label={inputLabel}
      />
      <span className="disc-pagination__jump-of" aria-hidden>
        {t("grid.pageOf", { pages: String(pageCount) })}
      </span>
    </label>
  );
}
