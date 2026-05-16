/**
 * HTML `id` segment for focusing elements by a rule/display name (unicode-safe).
 */
export function focusTargetDomId(prefix: string, name: string): string {
  try {
    const b = btoa(unescape(encodeURIComponent(name)));
    return `${prefix}-${b.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")}`;
  } catch {
    return `${prefix}-${name.replace(/\W+/g, "_")}`;
  }
}
