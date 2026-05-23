/**
 * Wildcard search for raw text: `*` = any substring, `?` = single character.
 * Other characters are matched literally (regex metacharacters escaped).
 */

function escapeRegexLiteral(c: string): string {
  return /[\\^$+.*?()[\]{}|]/.test(c) ? `\\${c}` : c;
}

/** Build a case-insensitive RegExp from a glob-style pattern. Returns null if pattern is empty. */
export function globPatternToRegExp(pattern: string): RegExp | null {
  const trimmed = pattern.trim();
  if (!trimmed) return null;
  let source = "";
  for (let i = 0; i < trimmed.length; i++) {
    const c = trimmed[i];
    if (c === "*") {
      source += ".*";
    } else if (c === "?") {
      source += ".";
    } else {
      source += escapeRegexLiteral(c);
    }
  }
  try {
    return new RegExp(source, "gi");
  } catch {
    return null;
  }
}

export type TextMatch = { start: number; end: number };

/** All non-overlapping matches of the glob pattern in `text`. */
export function findGlobMatches(text: string, pattern: string): TextMatch[] {
  const re = globPatternToRegExp(pattern);
  if (!re) return [];
  const global = new RegExp(re.source, re.flags.includes("g") ? re.flags : `${re.flags}g`);
  const out: TextMatch[] = [];
  let m: RegExpExecArray | null;
  while ((m = global.exec(text)) !== null) {
    const start = m.index;
    const end = start + m[0].length;
    out.push({ start, end });
    if (end === start) {
      global.lastIndex = start + 1;
      if (global.lastIndex > text.length) break;
    }
  }
  return out;
}

/** Scroll a textarea so the line containing `start` is near the middle (best-effort with wrapping). */
export function scrollTextareaToRange(el: HTMLTextAreaElement, start: number, end: number): void {
  const text = el.value;
  const safeStart = Math.max(0, Math.min(start, text.length));
  const safeEnd = Math.max(safeStart, Math.min(end, text.length));
  const before = text.slice(0, safeStart);
  const lineIndex = before.split("\n").length - 1;
  const style = window.getComputedStyle(el);
  const lh = parseFloat(style.lineHeight);
  const lineHeight = Number.isFinite(lh) && lh > 0 ? lh : 20;
  const padTop = parseFloat(style.paddingTop) || 0;
  const lineTop = lineIndex * lineHeight + padTop;
  const target = lineTop - el.clientHeight / 2 + lineHeight / 2;
  el.scrollTop = Math.max(0, Math.min(target, Math.max(0, el.scrollHeight - el.clientHeight)));
  el.setSelectionRange(safeStart, safeEnd);
  el.focus({ preventScroll: true });
}
