import { createPortal } from "react-dom";
import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { findGlobMatches, scrollTextareaToRange } from "../utils/yamlGlobSearch";

type Props = {
  initialContent: string;
  onSaveRaw: (content: string) => Promise<void>;
  onAfterSave?: () => Promise<void>;
};

export function AdvancedYamlPanel({ initialContent, onSaveRaw, onAfterSave }: Props) {
  const { t } = useAppSettings();
  const searchInputId = useId();
  const [open, setOpen] = useState(false);
  const [text, setText] = useState(initialContent);
  const [searchQuery, setSearchQuery] = useState("");
  const [matchIndex, setMatchIndex] = useState(0);
  /** After Find / Next / Prev — enables match position display and Next/Prev. Reset when the search pattern changes. */
  const [navigated, setNavigated] = useState(false);
  /** Incremented only when we should scroll/select a match (explicit Find / Next / Prev). */
  const [scrollTick, setScrollTick] = useState(0);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const matchesRef = useRef<ReturnType<typeof findGlobMatches>>([]);
  const matchIndexRef = useRef(0);

  useEffect(() => {
    if (!open) {
      setText(initialContent);
    }
  }, [initialContent, open]);

  const matches = useMemo(() => findGlobMatches(text, searchQuery), [text, searchQuery]);
  matchesRef.current = matches;
  matchIndexRef.current = matchIndex;

  const safeIndex = useMemo(() => {
    if (matches.length === 0) return 0;
    return Math.min(matchIndex, matches.length - 1);
  }, [matches, matchIndex]);

  useEffect(() => {
    if (safeIndex !== matchIndex) {
      setMatchIndex(safeIndex);
    }
  }, [safeIndex, matchIndex]);

  useLayoutEffect(() => {
    if (!open || scrollTick === 0) return;
    const ta = textareaRef.current;
    const list = matchesRef.current;
    if (!ta || list.length === 0) return;
    const idx = Math.min(matchIndexRef.current, list.length - 1);
    const m = list[idx];
    if (!m) return;
    scrollTextareaToRange(ta, m.start, m.end);
  }, [open, scrollTick]);

  const bumpScroll = useCallback(() => {
    setScrollTick((n) => n + 1);
  }, []);

  const handleFind = useCallback(() => {
    const list = findGlobMatches(text, searchQuery);
    if (list.length === 0) return;
    setMatchIndex(0);
    setNavigated(true);
    bumpScroll();
  }, [text, searchQuery, bumpScroll]);

  const goNext = useCallback(() => {
    const list = matchesRef.current;
    if (list.length === 0) return;
    setMatchIndex((i) => (i + 1) % list.length);
    setNavigated(true);
    bumpScroll();
  }, [bumpScroll]);

  const goPrev = useCallback(() => {
    const list = matchesRef.current;
    if (list.length === 0) return;
    setMatchIndex((i) => (i - 1 + list.length) % list.length);
    setNavigated(true);
    bumpScroll();
  }, [bumpScroll]);

  const requestClose = useCallback(() => {
    if (text !== initialContent) {
      if (!window.confirm(t("advanced.confirmDiscard"))) return;
    }
    setOpen(false);
    setSearchQuery("");
    setMatchIndex(0);
    setNavigated(false);
    setScrollTick(0);
    setText(initialContent);
  }, [text, initialContent, t]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        requestClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, requestClose]);

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const id = window.setTimeout(() => textareaRef.current?.focus(), 0);
    return () => clearTimeout(id);
  }, [open]);

  const openModal = () => {
    setText(initialContent);
    setSearchQuery("");
    setMatchIndex(0);
    setNavigated(false);
    setScrollTick(0);
    setOpen(true);
  };

  const handleSave = async () => {
    try {
      await onSaveRaw(text);
      await onAfterSave?.();
      setOpen(false);
      setSearchQuery("");
      setMatchIndex(0);
      setNavigated(false);
      setScrollTick(0);
    } catch {
      /* Stay open; caller / fetch layer may show an error. */
    }
  };

  const canStep = navigated && matches.length > 0;

  const statusText = (() => {
    if (searchQuery.trim() === "") return "";
    if (matches.length === 0) return t("advanced.noMatches");
    if (!navigated) return t("advanced.matchTotal", { total: String(matches.length) });
    return t("advanced.matchStatus", {
      current: String(safeIndex + 1),
      total: String(matches.length),
    });
  })();

  const modal = open ? (
    <div
      className="kea-advanced-fs-backdrop"
      role="dialog"
      aria-modal="true"
      aria-labelledby="advanced-yaml-dialog-title"
    >
      <div className="kea-advanced-fs">
        <header className="kea-advanced-fs__header">
          <h2 id="advanced-yaml-dialog-title" className="kea-advanced-fs__title">
            {t("advanced.modalTitle")}
          </h2>
          <div className="kea-advanced-fs__header-actions">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={requestClose}>
              {t("btn.cancel")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void handleSave()}>
              {t("advanced.modalSave")}
            </button>
          </div>
        </header>
        <div className="kea-advanced-fs__main">
          <p className="kea-hint kea-hint--warn" style={{ marginTop: 0 }}>
            {t("advanced.warning1")} <code>#</code> {t("advanced.warning2")}
          </p>
          <div className="kea-advanced-search">
            <label htmlFor={searchInputId} className="kea-advanced-search__label-text">
              {t("advanced.searchLabel")}
            </label>
            <div className="kea-advanced-search__control-row">
              <input
                id={searchInputId}
                type="search"
                className="kea-input kea-advanced-search__input"
                value={searchQuery}
                placeholder={t("advanced.searchPlaceholder")}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setMatchIndex(0);
                  setNavigated(false);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    if (e.shiftKey) goPrev();
                    else handleFind();
                  }
                }}
                autoComplete="off"
                spellCheck={false}
                aria-describedby="advanced-search-wildcard-hint"
              />
              <div className="kea-advanced-search__actions">
                <button
                  type="button"
                  className="kea-btn kea-btn--sm kea-btn--primary"
                  disabled={matches.length === 0}
                  onClick={handleFind}
                  title={t("advanced.find")}
                >
                  {t("advanced.find")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  disabled={!canStep}
                  onClick={goPrev}
                  title={t("advanced.prevMatch")}
                >
                  {t("advanced.prevMatch")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  disabled={!canStep}
                  onClick={goNext}
                  title={t("advanced.nextMatch")}
                >
                  {t("advanced.nextMatch")}
                </button>
              </div>
              <span className="kea-advanced-search__status" role="status" aria-live="polite">
                {statusText}
              </span>
            </div>
          </div>
          <p id="advanced-search-wildcard-hint" className="kea-hint" style={{ marginTop: 0, marginBottom: 0 }}>
            {t("advanced.wildcardHint")}
          </p>
          <textarea
            ref={textareaRef}
            className="kea-textarea kea-advanced-fs__editor"
            value={text}
            onChange={(e) => setText(e.target.value)}
            spellCheck={false}
          />
        </div>
      </div>
    </div>
  ) : null;

  return (
    <div className="kea-advanced">
      <button
        type="button"
        className="kea-advanced-toggle"
        title={t("advanced.toggle.tooltip")}
        aria-expanded={open}
        aria-haspopup="dialog"
        onClick={openModal}
      >
        {t("advanced.toggle")}
      </button>
      {modal ? createPortal(modal, document.body) : null}
    </div>
  );
}
