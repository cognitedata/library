export function extractFieldValue(row: Record<string, unknown>, field: string) {
  if (row[field] !== undefined) {
    return row[field];
  }
  const properties = row.properties;
  if (properties && typeof properties === "object") {
    if ((properties as Record<string, unknown>)[field] !== undefined) {
      return (properties as Record<string, unknown>)[field];
    }
    const found = findNestedField(properties as Record<string, unknown>, field);
    if (found !== undefined) {
      return found;
    }
  }
  return "—";
}

export function findNestedField(value: Record<string, unknown>, field: string): unknown {
  if (field in value) {
    return value[field];
  }
  for (const entry of Object.values(value)) {
    if (entry && typeof entry === "object") {
      const result = findNestedField(entry as Record<string, unknown>, field);
      if (result !== undefined) {
        return result;
      }
    }
  }
  return undefined;
}
