export function formatResourceDisplayLabel(
  name: string | undefined | null,
  externalId: string | undefined | null,
  fallback = ""
): string {
  const n = name?.trim() || "";
  const id = externalId?.trim() || "";
  if (n && id) {
    if (n === id) return id;
    return `${n} · ${id}`;
  }
  if (id) return id;
  if (n) return n;
  return fallback;
}
