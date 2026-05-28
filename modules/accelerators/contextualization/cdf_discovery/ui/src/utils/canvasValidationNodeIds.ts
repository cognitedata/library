/** Canvas node ids referenced in compile/validation error messages from the transform API. */

const CANVAS_NODE_ID_RE = /node\s+'([^']+)'/gi;

/** Extract React Flow node ids from ``errors`` strings (e.g. ``transform node 't1': …``). */
export function canvasValidationNodeIds(errors: readonly string[] | undefined | null): string[] {
  if (!errors?.length) return [];
  const ids = new Set<string>();
  for (const line of errors) {
    const text = String(line ?? "").trim();
    if (!text) continue;
    CANVAS_NODE_ID_RE.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = CANVAS_NODE_ID_RE.exec(text)) !== null) {
      const id = match[1]?.trim();
      if (id) ids.add(id);
    }
  }
  return [...ids];
}
