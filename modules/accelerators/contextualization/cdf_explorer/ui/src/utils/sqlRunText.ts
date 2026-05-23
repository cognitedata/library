/** Text to execute: non-empty editor selection, otherwise full query. */
export function queryTextForRun(
  fullQuery: string,
  selectionStart: number,
  selectionEnd: number
): string {
  if (selectionStart !== selectionEnd) {
    const selected = fullQuery.slice(selectionStart, selectionEnd).trim();
    if (selected) return selected;
  }
  return fullQuery.trim();
}
