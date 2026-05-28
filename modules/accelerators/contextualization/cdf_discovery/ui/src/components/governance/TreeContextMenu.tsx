import { useCallback, useEffect, useLayoutEffect, useRef, useState, type MouseEvent } from "react";
import { createPortal } from "react-dom";

export type TreeCtxMenuItem = {
  id: string;
  label: string;
  disabled?: boolean;
  danger?: boolean;
  /** Renders a non-interactive divider (``label`` / ``onSelect`` ignored). */
  separator?: boolean;
  onSelect: () => void;
};

export function treeCtxMenuSeparator(id: string): TreeCtxMenuItem {
  return { id, label: "", separator: true, onSelect: () => {} };
}

export function useTreeContextMenuState() {
  const [menu, setMenu] = useState<null | { x: number; y: number; items: TreeCtxMenuItem[] }>(null);
  const close = useCallback(() => setMenu(null), []);
  const open = useCallback((e: MouseEvent | globalThis.MouseEvent, items: TreeCtxMenuItem[]) => {
    e.preventDefault();
    e.stopPropagation();
    setMenu({ x: e.clientX, y: e.clientY, items });
  }, []);
  return { menu, open, close };
}

type MenuProps = {
  menu: { x: number; y: number; items: TreeCtxMenuItem[] } | null;
  onClose: () => void;
  classPrefix: "gov" | "disc";
};

export function TreeContextMenuPortal({ menu, onClose, classPrefix }: MenuProps) {
  const rootRef = useRef<HTMLUListElement>(null);
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);

  useLayoutEffect(() => {
    if (!menu) {
      setPos(null);
      return;
    }
    setPos({ x: menu.x, y: menu.y });
  }, [menu]);

  useLayoutEffect(() => {
    if (!menu || !pos || !rootRef.current) return;
    const el = rootRef.current;
    const pad = 8;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const r = el.getBoundingClientRect();
    let x = pos.x;
    let y = pos.y;
    if (x + r.width > vw - pad) x = Math.max(pad, vw - r.width - pad);
    if (y + r.height > vh - pad) y = Math.max(pad, vh - r.height - pad);
    if (x !== pos.x || y !== pos.y) setPos({ x, y });
  }, [menu, pos]);

  useEffect(() => {
    if (!menu) return;
    const onDoc = (ev: Event) => {
      if (rootRef.current?.contains(ev.target as Node)) return;
      onClose();
    };
    /** Defer so the same gesture that opened the menu does not immediately close it. */
    const t = window.setTimeout(() => {
      document.addEventListener("pointerdown", onDoc, true);
    }, 0);
    return () => {
      window.clearTimeout(t);
      document.removeEventListener("pointerdown", onDoc, true);
    };
  }, [menu, onClose]);

  useEffect(() => {
    if (!menu) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [menu, onClose]);

  useEffect(() => {
    if (!menu || !rootRef.current) return;
    const first = rootRef.current.querySelector<HTMLButtonElement>('[role="menuitem"]');
    first?.focus();
  }, [menu, pos]);

  if (!menu || !pos) return null;

  const base = classPrefix === "disc" ? "disc-ctx-menu" : `${classPrefix}-tree-ctx-menu`;

  return createPortal(
    <ul
      ref={rootRef}
      className={base}
      role="menu"
      style={{ position: "fixed", left: pos.x, top: pos.y, zIndex: 10000 }}
    >
      {menu.items.map((item) =>
        item.separator ? (
          <li key={item.id} className={classPrefix === "disc" ? "disc-ctx-menu__sep" : `${base}__sep`} role="separator" />
        ) : (
          <li key={item.id} role="none">
            <button
              type="button"
              role="menuitem"
              className={
                classPrefix === "disc"
                  ? undefined
                  : item.danger
                    ? `${base}__btn ${base}__btn--danger`
                    : `${base}__btn`
              }
              disabled={item.disabled}
              onClick={() => {
                if (!item.disabled) {
                  item.onSelect();
                  onClose();
                }
              }}
            >
              {item.label}
            </button>
          </li>
        )
      )}
    </ul>,
    document.body
  );
}
