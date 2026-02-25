/**
 * Shared text color class for property value types (search results, view card, view modal).
 * Use so scalar types (float64, int32, boolean, etc.) and relations get consistent coloring.
 */
export function getPropertyTypeTextClass(
  type: string,
  isRelation: boolean,
  isDark: boolean
): string {
  if (isRelation) return "text-amber-400";
  const t = (type ?? "").toLowerCase();
  if (t === "timestamp") return "text-orange-500";
  if (t === "date") return "text-amber-600";
  if (t === "text") return "text-emerald-500";
  if (t === "int32" || t === "int64" || t === "int" || t === "integer" || t === "long") return "text-blue-500";
  if (t === "float32" || t === "float64" || t === "float" || t === "double" || t === "number") return "text-cyan-500";
  if (t === "boolean" || t === "bool") return "text-violet-500";
  if (t === "json") return "text-pink-500";
  return isDark ? "text-slate-300" : "text-slate-600";
}
