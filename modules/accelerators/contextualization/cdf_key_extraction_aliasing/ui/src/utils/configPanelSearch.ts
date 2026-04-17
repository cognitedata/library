import { globPatternToRegExp } from "./yamlGlobSearch";

/** True if `pattern` is empty, or any non-empty `haystack` matches the glob pattern (case-insensitive). */
export function matchConfigSearch(pattern: string, ...haystacks: string[]): boolean {
  const re = globPatternToRegExp(pattern);
  if (!re) return true;
  const r = new RegExp(re.source, re.flags);
  return haystacks.some((s) => {
    if (!s) return false;
    r.lastIndex = 0;
    return r.test(s);
  });
}
